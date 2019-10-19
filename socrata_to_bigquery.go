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
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/SebastiaanKlippert/go-soda"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

type StringArray []string

func (a *StringArray) Set(s string) error {
	*a = append(*a, s)
	return nil
}

func (a *StringArray) String() string {
	return strings.Join(*a, ",")
}

func LogSocrataSchema(c []soda.Column) {
	for i, cc := range c {
		fmt.Printf("[%d] %q (%s) %s %#v\n", i, cc.FieldName, cc.DataTypeName, cc.Name, cc)
	}
}

func DataTypeNameToFieldType(t string) bigquery.FieldType {
	switch t {
	case "text", "url":
		return bigquery.StringFieldType
	case "number":
		return bigquery.NumericFieldType
	case "calendar_date":
		return bigquery.DateFieldType
	case "point":
		return bigquery.GeographyFieldType
	}
	panic(fmt.Sprintf("unknown type %q", t))
}

func ToTableName(id, name string) string {
	r := strings.NewReplacer("-", "_", " ", "_")
	return r.Replace(id + "-" + strings.ToLower(name))
}

// parses values of the format "field_name:BIGQUERY_FIELD_TYPE" i.e. "my_date_col:DATE"
func ParseSchemaOverride(s []string) map[string]bigquery.FieldType {
	d := make(map[string]bigquery.FieldType)
	for _, ss := range s {
		c := strings.SplitN(ss, ":", 2)
		d[c[0]] = bigquery.FieldType(c[1])
	}
	return d
}

func BQTableMetadata(s soda.Metadata, partitionField string, schemaOverride map[string]bigquery.FieldType) *bigquery.TableMetadata {
	schema := bigquery.Schema{
		&bigquery.FieldSchema{
			Name:     "_id",
			Type:     bigquery.StringFieldType,
			Required: true,
		},
		&bigquery.FieldSchema{
			Name:     "_created_at",
			Type:     bigquery.TimestampFieldType,
			Required: true,
		},
		&bigquery.FieldSchema{
			Name:     "_updated_at",
			Type:     bigquery.TimestampFieldType,
			Required: true,
		},
		&bigquery.FieldSchema{
			Name:     "_version",
			Type:     bigquery.StringFieldType,
			Required: false,
		},
	}
	var foundRequired bool
	for _, c := range s.Columns {
		var required bool
		if c.FieldName == partitionField {
			required = true
			foundRequired = true
		}
		t := DataTypeNameToFieldType(c.DataTypeName)
		if tt, ok := schemaOverride[c.FieldName]; ok {
			t = tt
		}
		schema = append(schema, &bigquery.FieldSchema{
			Name:        c.FieldName,
			Description: c.Name,
			Type:        t,
			Required:    required,
		})
	}
	if !foundRequired && partitionField != "" {
		log.Panicf("partition column %q not detected", partitionField)
	}

	m := &bigquery.TableMetadata{
		Name:        ToTableName(s.ID, s.Name),
		Description: s.Name,
		Schema:      schema,
	}
	if partitionField != "" {
		m.RequirePartitionFilter = true
		m.TimePartitioning = &bigquery.TimePartitioning{
			Field: partitionField,
		}
	}
	return m
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	socrataDataset := flag.String("socrata-dataset", "", "the URL to the socrata dataset")
	bucketName := flag.String("gs-bucket", "", "google storage bucket name")
	projectID := flag.String("bq-project-id", "", "")
	datasetName := flag.String("bq-dataset", "", "bigquery dataset name")
	partitionColumn := flag.String("partition-column", "", "Date column to partition BQ table on")
	quiet := flag.Bool("quiet", false, "disable progress output")
	limit := flag.Int("limit", 100000000, "limit")
	where := flag.String("where", "", "$where clause")
	var schemaOverride StringArray
	flag.Var(&schemaOverride, "schema-override", "override a field schema field:type (may be given multiple times)")
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
				if err := bqTable.Create(ctx, BQTableMetadata(*md, *partitionColumn, ParseSchemaOverride(schemaOverride))); err != nil {
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
