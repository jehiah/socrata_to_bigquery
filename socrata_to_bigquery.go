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
	"runtime/pprof"
	"time"

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
	cpuprofile := flagSet.String("cpuprofile", "", "write cpu profile to `file`")
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
		syncOne(configFile, *quiet, *token, *cpuprofile)
	}
}

func syncOne(configFile string, quiet bool, token, cpuprofile string) {
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

	// bigquery so we where we left off
	// create if needed
	sodareq.Query.Limit = uint(100000000)
	sodareq.Query.Select = []string{":*", "*"}
	sodareq.Query.Where = where
	sodareq.Format = "json"
	req, err := http.NewRequest("GET", sodareq.GetEndpoint(), nil)
	if err != nil {
		log.Fatal(err)
	}
	req.URL.RawQuery = sodareq.URLValues().Encode()
	req.Header.Set("X-App-Token", token)
	log.Printf("streaming from %s", req.URL)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	if resp.StatusCode >= 400 {
		log.Fatalf("got unexpected response %d", resp.StatusCode)
	}
	// query socrata
	// stream to a google storage file
	obj := bkt.Object(filepath.Join("socrata_to_bigquery", time.Now().Format("20060102-150405"), md.ID+".json.gz"))
	log.Printf("streaming to %s/%s", cf.GSBucket(), obj.ObjectName())
	w := obj.NewWriter(ctx)
	w.ObjectAttrs.ContentType = "application/json"
	w.ObjectAttrs.ContentEncoding = "gzip"
	gw := gzip.NewWriter(w)

	var f *os.File
	if cpuprofile != "" {
		log.Printf("creating %s", cpuprofile)
		f, err = os.Create(cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
	}

	rows, transformErr := Transform(gw, resp.Body, cf.Schema, quiet, missing)
	log.Printf("wrote %d rows to Google Storage", rows)
	err = gw.Close()
	if err != nil {
		log.Fatal(err)
	}
	err = w.Close()
	if err != nil {
		log.Fatal(err)
	}
	resp.Body.Close()
	if transformErr != nil {
		log.Fatal(transformErr)
	}

	if cpuprofile != "" {
		log.Printf("wrote cpu profile to %q", cpuprofile)
		pprof.StopCPUProfile()
		f.Close()
	}

	if rows != 0 {
		// load into bigquery
		gcsRef := bigquery.NewGCSReference(fmt.Sprintf("%s/%s", cf.GSBucket(), obj.ObjectName()))
		gcsRef.SourceFormat = bigquery.JSON

		loader := bqTable.LoaderFrom(gcsRef)
		loader.WriteDisposition = bigquery.WriteAppend

		loadJob, err := loader.Run(ctx)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("BigQuery import running job %s", loadJob.ID())
		status, err := loadJob.Wait(ctx)
		log.Printf("BigQuery import job %s done", loadJob.ID())
		if err != nil {
			log.Fatal(err)
		}
		if err = status.Err(); err != nil {
			log.Fatal(err)
		}
	}

	// cleanup google storage
	log.Printf("removing temp file %s/%s", cf.GSBucket(), obj.ObjectName())
	if err = obj.Delete(ctx); err != nil {
		log.Fatal(err)
	}
}
