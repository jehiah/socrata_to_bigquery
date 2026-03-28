package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

func syncOne(configFile string, quiet bool, token string) {
	cf, err := LoadConfigFile(configFile)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	apiBase := cf.APIBase()
	datasetID := cf.DatasetID()

	md, err := FetchMetadata(ctx, apiBase, datasetID, token)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Synchronizing Socrata: %s (%s) (last modified %v)\n", md.ID, md.Name, md.RowsUpdatedAtTime().Format(time.RFC3339))

	socrataCount, err := CountV3(ctx, apiBase, datasetID, cf.BigQuery.WhereFilter, token)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Socrata Records: %d\n", socrataCount)

	bqclient, err := bigquery.NewClient(ctx, cf.BigQuery.ProjectID)
	if err != nil {
		log.Fatal(err)
	}
	dataset := bqclient.Dataset(cf.BigQuery.DatasetName)
	dmd, err := dataset.Metadata(ctx)
	if err != nil {
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
		log.Fatalf("Error fetching BigQuery Table %s.%s %s", dmd.FullID, datasetID, err)
	}
	fmt.Printf("BQ Table %s OK (last modified %s)\n", tmd.FullID, tmd.LastModifiedTime)

	fmt.Printf("BQ Records: %d\n", tmd.NumRows)
	if socrataCount == 0 {
		return
	}
	missing := socrataCount - int64(tmd.NumRows)
	if missing <= 0 {
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
				where = where + " AND " + createdFilter
			}
		}
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	bkt := client.Bucket(cf.GoogleStorageBucketName)

	if err := streamAndLoad(ctx, cf, datasetID, where, token, bkt, bqTable, quiet, missing); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Sync Complete\n")
}

// estimate calculates the remaining rows and estimated time remaining based on the
// count of rows processed, total missing rows, and elapsed time
func estimate(count, missing int64, elapsed time.Duration) (int64, time.Duration) {
	remainingRows := missing - count
	if count > 0 && remainingRows > 0 {
		avgTimePerRow := elapsed / time.Duration(count)
		estimatedRemaining := time.Duration(remainingRows) * avgTimePerRow
		return remainingRows, estimatedRemaining
	}
	return remainingRows, 0
}

func streamAndLoad(ctx context.Context, cf ConfigFile, datasetID, where, token string, bkt *storage.BucketHandle, bqTable *bigquery.Table, quiet bool, missing int64) error {
	sql := "SELECT :*, *"
	if where != "" {
		sql += " WHERE " + where
	}

	obj := bkt.Object(filepath.Join("socrata_to_bigquery", time.Now().Format("20060102-150405"), datasetID+".json.gz"))
	fmt.Printf("> writing to %s/%s\n", cf.GSBucket(), obj.ObjectName())
	w := obj.NewWriter(ctx)
	w.ContentType = "application/json"
	w.ContentEncoding = "gzip"
	bw := bufio.NewWriterSize(w, 5*1024*1024) // 5MB buffer
	gw := gzip.NewWriter(bw)
	enc := json.NewEncoder(gw)
	enc.SetEscapeHTML(false)

	var rows int64
	start := time.Now()
	out := make(chan Record, 100000)
	wg, ctxg := errgroup.WithContext(ctx)
	// gorotine for encoding and writing to GCS
	wg.Go(func() error {
		for row := range out {
			if err := enc.Encode(row); err != nil {
				return err
			}
		}
		return nil
	})
	streamErr := StreamV3(ctxg, cf.APIBase(), datasetID, sql, token, func(row Record) error {
		mm, err := TransformOne(row, cf.Schema)
		if err != nil {
			return fmt.Errorf("row %d: %w", rows+1, err)
		}
		if mm != nil {
			rows++
			if !quiet && rows%100000 == 0 {
				elapsed := time.Since(start)
				remainingRows, estimatedRemaining := estimate(rows, missing, elapsed)
				if remainingRows > 0 && estimatedRemaining > 0 {
					log.Printf("processed %d rows (%s) - estimated remaining %d : %s", rows, elapsed.Truncate(time.Second), remainingRows, estimatedRemaining.Truncate(time.Second))
				} else {
					log.Printf("processed %d rows (%s)", rows, elapsed.Truncate(time.Second))
				}
			}
			out <- mm
		}
		return nil
	})
	if streamErr != nil {
		log.Printf("streamErr: %s", streamErr)
	}

	close(out)
	if err := wg.Wait(); err != nil {
		return err
	}
	if err := gw.Close(); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil {
		return err
	}
	if err := w.Close(); err != nil {
		return err
	}
	if streamErr != nil {
		return streamErr
	}

	if rows == 0 {
		fmt.Printf("0 out-of-sync records found\n")
		if err := obj.Delete(ctx); err != nil {
			return err
		}
		return nil
	}

	fmt.Printf("Queued %d rows for BigQuery load\n", rows)
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

	if err = obj.Delete(ctx); err != nil {
		return err
	}
	return nil
}
