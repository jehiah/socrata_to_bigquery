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
	"encoding/json"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	soda "github.com/SebastiaanKlippert/go-soda"
	toml "github.com/pelletier/go-toml"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

// m := &bigquery.TableMetadata{
// 	Name:        ToTableName(s.ID, s.Name),
// 	Description: s.Name,
// 	Schema:      schema,
// }

func initDataset(args []string) {
	initFlagSet := flag.NewFlagSet("init", flag.ExitOnError)
	dataset := initFlagSet.String("dataset", "", "The URL to the socrata dataset")
	token := initFlagSet.String("socrata-app-token", "", "Socrata App Token (also src SOCRATA_APP_TOKEN env)")
	debug := initFlagSet.Bool("debug", false, "show debug output")
	initFlagSet.Parse(args)

	if *dataset == "" {
		log.Fatal("missing --dataset")
	}
	if *token == "" {
		*token = os.Getenv("SOCRATA_APP_TOKEN")
	}
	if *token == "" {
		log.Fatal("missing --socrata-app-token or environment variable SOCRATA_APP_TOKEN")
	}

	sodareq := soda.NewGetRequest(*dataset, *token)
	// ":*" includes metadata records :id, :created_at, :updated_at, :version
	sodareq.Query.Select = []string{":*", "*"}
	md, err := sodareq.Metadata.Get()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Found Socrata Dataset: %s (%s) last modified %v\n", md.ID, md.Name, time.Time(md.RowsUpdatedAt).Format(time.RFC3339))
	if *debug {
		LogSocrataSchema(md.Columns)
	}

	filename := fmt.Sprintf("%s.toml", md.ID)
	fmt.Printf("creating %s\n", filename)
	f, err := os.Create(filename)
	defer f.Close()
	encoder := toml.NewEncoder(f)
	encoder.Encode(NewConfig(*dataset, *md))
	encoder.Encode(map[string]TableSchema{"schema": NewSchema(*md, MustExampleRecords(*sodareq))})
}


func MustExampleRecords(r soda.GetRequest) map[string]string {
	d, err := ExampleRecords(r)
	if err != nil {
		log.Fatal(err)
	}
	return d
}

func ExampleRecords(sr soda.GetRequest) (map[string]string, error) {
	r := &sr
	r.Query.Limit = 10
	fmt.Println("Fetching example records.")
	resp, err := r.Get()
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var data []map[string]interface{}
	if err = json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	buffer := make(map[string][]string)
	for _, row := range data {
		for k, v := range row {
			switch v := v.(type) {
			case string:
				buffer[k] = append(buffer[k], fmt.Sprintf("%q", v))
			case int, int64:
				buffer[k] = append(buffer[k], fmt.Sprintf("%d", v))
			case bool:
				buffer[k] = append(buffer[k], fmt.Sprintf("%v", v))
			default:
				log.Printf("unhandled type %T %#v", v, v)
			}
		}
	}
	
	out := make(map[string]string)
	for k, v := range buffer {
		out[k] = strings.Join(uniqExamples(v, 3), ", ")
	}
	return out, nil
}

func uniqExamples(a[]string, max int) []string {
	var out []string
	data := make(map[string]bool, len(a))
	for _, aa := range a {
		if data[aa] {
			continue
		}
		data[aa] = true
		out = append(out, aa)
		if len(out) == max {
			break
		}
	}
	return out
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	if len(os.Args) < 2 {
		fmt.Println("subcommand required")
		fmt.Println(" * init")
		fmt.Println(" * test")
		fmt.Println(" * sync")
		os.Exit(1)
	}

	switch os.Args[1] {
	case "init":
		initDataset(os.Args[2:])
	default:
		flag.PrintDefaults()
		os.Exit(1)
	}
	os.Exit(0)

	socrataDataset := flag.String("socrata-dataset", "", "the URL to the socrata dataset")
	bucketName := flag.String("gs-bucket", "", "google storage bucket name")
	projectID := flag.String("bq-project-id", "", "")
	datasetName := flag.String("bq-dataset", "", "bigquery dataset name")
	quiet := flag.Bool("quiet", false, "disable progress output")
	limit := flag.Int("limit", 100000000, "limit")
	where := flag.String("where", "", "$where clause")
	flag.Parse()

	if *socrataDataset == "" {
		log.Fatal("missing --socrata-dataset")
	}

	token := os.Getenv("SOCRATA_APP_TOKEN")
	sodareq := soda.NewGetRequest(*socrataDataset, token)
	sodareq.Query.Select = []string{":*", "*"}
	md, err := sodareq.Metadata.Get()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Socrata: %s (%s) last modified %v", md.ID, md.Name, time.Time(md.RowsUpdatedAt).Format(time.RFC3339))
	// LogSocrataSchema(md.Columns)

	if *projectID == "" {
		log.Fatal("missing --bq-project-id")
	}

	ctx := context.Background()
	// bigquery - see where we left off
	bqclient, err := bigquery.NewClient(ctx, *projectID)
	if err != nil {
		log.Fatal(err)
	}
	dataset := bqclient.Dataset(*datasetName)
	dmd, err := dataset.Metadata(ctx)
	if err != nil {
		// TODO: dataset doesn't exist? require that first?
		log.Fatalf("Error fetching BigQuery dataset %s.%s err %s", *projectID, *datasetName, err)
	}
	log.Printf("BQ Dataset %s OK (last modified %s)", dmd.FullID, dmd.LastModifiedTime)

	bqTable := dataset.Table(ToTableName(md.ID, md.Name))
	tmd, err := bqTable.Metadata(ctx)
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok {
			if e.Code == 404 {
				log.Printf("Auto-creating table %s", e.Message)
				// if err := bqTable.Create(ctx, BQTableMetadata(*md, *partitionColumn, ParseSchemaOverride(schemaOverride))); err != nil {
				// 	log.Fatal(err)
				// }
				// tmd, err = bqTable.Metadata(ctx)
				// if err != nil {
				// 	log.Fatal(err)
				// }
			}
		}
	}
	if tmd == nil {
		log.Fatalf("Error fetching BigQuery Table %s.%s %s", dmd.FullID, md.ID, err)
	}
	log.Printf("BQ Table %s OK (last modified %s)", tmd.FullID, tmd.LastModifiedTime)

	var minCreatedDate time.Time
	if *where == "" {

		// automatically generate a where clause
		tn := fmt.Sprintf("`%s.%s.%s`", *projectID, *datasetName, tmd.Name)
		q := bqclient.Query(`SELECT max(_created_at) as created FROM ` + tn)
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
			minCreatedDate = r.Created
			*where = fmt.Sprintf(":created_at >= '%s'", r.Created.Add(time.Second).Format(time.RFC3339))
			log.Printf("using automatic where clause %s", *where)
		}
	}

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	bkt := client.Bucket(*bucketName)

	// bigquery so we where we left off
	// create if needed
	sodareq.Query.Limit = uint(*limit)
	sodareq.Query.Select = []string{":*", "*"}
	sodareq.Query.Where = *where
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
	log.Printf("streaming to gs://%s/%s", *bucketName, obj.ObjectName())
	w := obj.NewWriter(ctx)
	w.ObjectAttrs.ContentType = "application/json"
	w.ObjectAttrs.ContentEncoding = "gzip"
	gw := gzip.NewWriter(w)
	rows, transformErr := Transform(gw, resp.Body, md.Columns, tmd.Schema, minCreatedDate, *quiet)
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
	if rows != 0 {
		// load into bigquery
		gcsRef := bigquery.NewGCSReference(fmt.Sprintf("gs://%s/%s", *bucketName, obj.ObjectName()))
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
	log.Printf("removing temp file gs://%s/%s", *bucketName, obj.ObjectName())
	if err = obj.Delete(ctx); err != nil {
		log.Fatal(err)
	}
}
