package main

import (
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"
	"sync"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	soda "github.com/SebastiaanKlippert/go-soda"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	if len(os.Args) < 2 {
		fmt.Println("subcommand required")
		fmt.Println(" * init")
		fmt.Println(" * sync")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "init":
		initDataset(os.Args[2:])
	case "sync":
		syncCmd(os.Args[2:])
	default:
		flag.PrintDefaults()
		os.Exit(1)
	}
}

func syncCmd(args []string) {
	flagSet := flag.NewFlagSet("sync", flag.ExitOnError)
	quiet := flagSet.Bool("quiet", false, "disable progress output")
	token := flagSet.String("socrata-app-token", "", "Socrata App Token (also src SOCRATA_APP_TOKEN env)")
	// limit := flag.Int("limit", 100000000, "limit")
	// where := flag.String("where", "", "$where clause")
	flagSet.Parse(args)
	if *token == "" {
		*token = os.Getenv("SOCRATA_APP_TOKEN")
	}
	if *token == "" {
		log.Fatal("missing --socrata-app-token or environment variable SOCRATA_APP_TOKEN")
	}

	if flagSet.NArg() == 0 {
		log.Fatal("missing filename")
	}
	for _, configFile := range flagSet.Args() {
		syncOne(configFile, *quiet, *token)
	}
}

func syncOne(configFile string, quiet bool, token string) {
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
	log.Printf("Socrata: %s (%s) last modified %v", md.ID, md.Name, time.Time(md.RowsUpdatedAt).Format(time.RFC3339))

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
	log.Printf("BQ Dataset %s OK (last modified %s)", dmd.FullID, dmd.LastModifiedTime)

	bqTable := dataset.Table(cf.BigQuery.TableName)
	tmd, err := bqTable.Metadata(ctx)
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok {
			if e.Code == 404 {
				log.Printf("Auto-creating table %s", e.Message)
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
	log.Printf("BQ Table %s OK (last modified %s)", tmd.FullID, tmd.LastModifiedTime)

	// socrata count
	sodataCount, err := sodareq.Count()
	if err != nil {
		log.Fatal(err)
	}

	missing := uint64(sodataCount) - tmd.NumRows
	log.Printf("BQ Records: %d (missing:%d) Socrata Records: %d", tmd.NumRows, missing, sodataCount)
	if sodataCount == 0 {
		return
	}

	where := cf.BigQuery.WhereFilter
	if where == "" {
		// automatically generate a where clause
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
			where = fmt.Sprintf(":created_at >= '%s'", r.Created.Add(time.Second).Format(time.RFC3339))
			log.Printf("using automatic where clause %s", where)
		}
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	bkt := client.Bucket(cf.GoogleStorageBucketName)
	pageSize := uint64(1000000)
	throttle := NewConcurrentLimit(3)
	var wg sync.WaitGroup
	for n := uint64(0); n < missing; n += pageSize {
		wg.Add(1)
		n := n
		throttle.Run(func(){
			remain := pageSize
			if n + remain > missing {
				remain = missing - n
			}
			err := CopyChunk(ctx, cf, token, where, n, remain, bkt, bqTable, quiet)
			if err != nil {
				log.Fatal(err)
			}
			wg.Done()
		})
	}
	wg.Wait()
	log.Printf("done")

}

func CopyChunk(ctx context.Context, cf ConfigFile, token, where string, offset, limit uint64, bkt *storage.BucketHandle, bqTable *bigquery.Table, quiet bool) error {
	// start socrata data stream
	sodareq := soda.NewGetRequest(cf.Dataset, token)
	sodareq.Query.Offset = uint(offset)
	sodareq.Query.Limit = uint(limit)
	sodareq.Query.Select = []string{":*", "*"}
	sodareq.Query.Where = where
	sodareq.Format = "json"
	req, err := http.NewRequest("GET", sodareq.GetEndpoint(), nil)
	if err != nil {
		return err
	}
	req.URL.RawQuery = sodareq.URLValues().Encode()
	req.Header.Set("X-App-Token", token)
	log.Printf("streaming from %s", req.URL)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("got unexpected response %d", resp.StatusCode)
	}

	// stream to a google storage file
	obj := bkt.Object(filepath.Join("socrata_to_bigquery", time.Now().Format("20060102-150405"), cf.DatasetID()+"-"+fmt.Sprintf("%d", offset)+".json.gz"))
	log.Printf("streaming to %s/%s", cf.GSBucket(), obj.ObjectName())
	w := obj.NewWriter(ctx)
	w.ObjectAttrs.ContentType = "application/json"
	w.ObjectAttrs.ContentEncoding = "gzip"
	gw := gzip.NewWriter(w)

	rows, transformErr := Transform(gw, resp.Body, cf.Schema, quiet, limit)
	log.Printf("wrote %d rows to Google Storage", rows)
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
		// load into bigquery
		gcsRef := bigquery.NewGCSReference(fmt.Sprintf("%s/%s", cf.GSBucket(), obj.ObjectName()))
		gcsRef.SourceFormat = bigquery.JSON

		loader := bqTable.LoaderFrom(gcsRef)
		loader.WriteDisposition = bigquery.WriteAppend

		loadJob, err := loader.Run(ctx)
		if err != nil {
			return err
		}
		log.Printf("BigQuery import running job %s", loadJob.ID())
		status, err := loadJob.Wait(ctx)
		log.Printf("BigQuery import job %s done", loadJob.ID())
		if err != nil {
			return err
		}
		if err = status.Err(); err != nil {
			return err
		}
	}

	// cleanup google storage
	log.Printf("removing temp file %s/%s", cf.GSBucket(), obj.ObjectName())
	if err = obj.Delete(ctx); err != nil {
		return err
	}
	return nil
}
