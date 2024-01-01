package main

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	soda "github.com/SebastiaanKlippert/go-soda"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

func syncOne(configFile string, quiet bool, token string, pageSize uint64) {
	cf, err := LoadConfigFile(configFile)
	if err != nil {
		log.Fatal(err)
	}

	// todo Validate
	// bg-project-id, etc

	sodareq := soda.NewGetRequest(cf.Dataset, token)
	sodareq.Query.Select = []string{":*", "*"}
	md, err := sodareq.Metadata.Get()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Syncronizing Socrata: %s (%s) (last modified %v)\n", md.ID, md.Name, time.Time(md.RowsUpdatedAt).Format(time.RFC3339))

	// socrata count
	sodareq.Query.Where = cf.BigQuery.WhereFilter
	sodataCount, err := sodareq.Count()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Socrata Records: %d\n", sodataCount)

	ctx := context.Background()
	bqclient, err := bigquery.NewClient(ctx, cf.BigQuery.ProjectID)
	if err != nil {
		log.Fatal(err)
	}
	dataset := bqclient.Dataset(cf.BigQuery.DatasetName)
	dmd, err := dataset.Metadata(ctx)
	if err != nil {
		// TODO: dataset doesn't exist? require that first?
		log.Fatalf("Error fetching BigQuery dataset %s.%s err %s", cf.BigQuery.ProjectID, cf.BigQuery.DatasetName, err)
	}
	fmt.Printf("BQ Dataset %s OK\n", dmd.FullID)

	bqTable := dataset.Table(cf.BigQuery.TableName)
	tmd, err := bqTable.Metadata(ctx)
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok {
			if e.Code == 404 {
				fmt.Printf("Auto-creating table %s\n", e.Message)
				if err := bqTable.Create(ctx, &bigquery.TableMetadata{
					Name:        cf.BigQuery.TableName,
					Description: cf.BigQuery.Description,
					Schema:      cf.Schema.BigQuerySchema(),
				}); err != nil {
					log.Fatal(err)
				}
				tmd, err = bqTable.Metadata(ctx)
				if err != nil {
					log.Fatal(err)
				}
			}
		}
	}
	if tmd == nil {
		log.Fatalf("Error fetching BigQuery Table %s.%s %s", dmd.FullID, md.ID, err)
	}
	fmt.Printf("BQ Table %s OK (last modified %s)\n", tmd.FullID, tmd.LastModifiedTime)

	missing := uint64(sodataCount) - tmd.NumRows
	fmt.Printf("BQ Records: %d\n", tmd.NumRows)
	if sodataCount == 0 {
		return
	}
	// Note: missing records could be from on_error=SKIP_ROW
	// fmt.Printf("Missing records from BQ: %d\n", missing)
	if missing == 0 {
		fmt.Printf("0 out-of-sync records found\n")
		fmt.Printf("Sync Complete\n")
		return
	}

	where := cf.BigQuery.WhereFilter
	if tmd.NumRows > 0 {
		// automatically generate a where clause picking up after the last _created_at
		q := bqclient.Query(`SELECT max(_created_at) as created FROM ` + cf.BigQuery.SQLTableName())
		it, err := q.Read(ctx)
		if err != nil {
			log.Fatal(err)
		}
		type Result struct {
			Created time.Time
		}
		var r Result
		for {
			err := it.Next(&r)
			if err == iterator.Done {
				break
			}
			if err != nil && err.Error() == "bigquery: NULL values cannot be read into structs" {
				break
			}
			if err != nil {
				log.Fatal(err)
			}
		}
		if !r.Created.IsZero() {
			fmt.Printf("BigQuery most recent record created_at: %s\n", r.Created)
			createdFilter := fmt.Sprintf(":created_at >= '%s'", r.Created.Add(time.Second).Format(time.RFC3339))
			fmt.Printf("> filtering to %s\n", createdFilter)
			if where == "" {
				where = createdFilter
			} else {
				where = where + " and " + createdFilter
			}
		}
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	bkt := client.Bucket(cf.GoogleStorageBucketName)
	throttle := NewConcurrentLimit(2)
	var wg sync.WaitGroup
	for n := uint64(0); n < missing; n += pageSize {
		wg.Add(1)
		n := n
		go throttle.Run(func() {
			remain := pageSize
			if n+remain > missing {
				remain = missing - n
			}
			for i := 0; i < 3; i++ {
				err := CopyChunk(ctx, cf, token, where, n, remain, bkt, bqTable, quiet)
				if errors.Is(err, io.EOF) {
					log.Printf("%d-%d err %s on try %d.", n, n+remain, err, i)
					continue
				}
				if err != nil {
					log.Fatal(err)
				}
				break
			}
			wg.Done()
		})
	}
	wg.Wait()
	fmt.Printf("Sync Complete\n")

}

func CopyChunk(ctx context.Context, cf ConfigFile, token, where string, offset, limit uint64, bkt *storage.BucketHandle, bqTable *bigquery.Table, quiet bool) error {
	// start socrata data stream
	sodareq := soda.NewGetRequest(cf.Dataset, token)
	sodareq.Query.Offset = uint(offset)
	sodareq.Query.Limit = uint(limit)
	sodareq.Query.Select = []string{":*", "*"}
	sodareq.Query.Where = where
	sodareq.Query.AddOrder(":id", false) // make paging stable. see https://dev.socrata.com/docs/paging.html
	sodareq.Format = "json"
	req, err := http.NewRequest("GET", sodareq.GetEndpoint(), nil)
	if err != nil {
		return err
	}
	req.URL.RawQuery = sodareq.URLValues().Encode()
	req.Header.Set("X-App-Token", token)
	fmt.Printf("> connecting to %s\n", req.URL)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("got unexpected response %d", resp.StatusCode)
	}

	// stream to a google storage file
	obj := bkt.Object(filepath.Join("socrata_to_bigquery", time.Now().Format("20060102-150405"), cf.DatasetID()+"-"+fmt.Sprintf("%d", offset)+".json.gz"))
	fmt.Printf("> writing to %s/%s\n", cf.GSBucket(), obj.ObjectName())
	w := obj.NewWriter(ctx)
	w.ObjectAttrs.ContentType = "application/json"
	w.ObjectAttrs.ContentEncoding = "gzip"
	gw := gzip.NewWriter(w)

	rows, transformErr := Transform(gw, resp.Body, cf.Schema, quiet, limit)
	if transformErr != nil {
		log.Printf("transformErr: %s", transformErr)
	}
	err = gw.Close()
	if err != nil {
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}
	resp.Body.Close()
	if transformErr != nil {
		return transformErr
	}

	if rows != 0 {
		fmt.Printf("Queued %d rows for BigQuery load\n", rows)
		// load into bigquery
		gcsRef := bigquery.NewGCSReference(fmt.Sprintf("%s/%s", cf.GSBucket(), obj.ObjectName()))
		gcsRef.SourceFormat = bigquery.JSON

		loader := bqTable.LoaderFrom(gcsRef)
		loader.WriteDisposition = bigquery.WriteAppend

		loadJob, err := loader.Run(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("BigQuery import running job %s\n", loadJob.ID())
		status, err := loadJob.Wait(ctx)
		fmt.Printf("BigQuery import job %s done\n", loadJob.ID())
		if err != nil {
			return err
		}
		if err = status.Err(); err != nil {
			return err
		}
	} else {
		fmt.Printf("0 out-of-sync records found\n")
	}

	// cleanup google storage
	// log.Printf("removing temp file %s/%s", cf.GSBucket(), obj.ObjectName())
	if err = obj.Delete(ctx); err != nil {
		return err
	}
	return nil
}
