package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
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
	}
	panic(fmt.Sprintf("unknown type %q", t))
}

func ToTableName(s string) string {
	return strings.ReplaceAll(s, "-", "_")
}

func ParseSchemaOverride(s []string) map[string]bigquery.FieldType {
	d := make(map[string]bigquery.FieldType)
	for _, ss := range s {
		c := strings.SplitN(ss, ":", 2)
		d[c[0]] = bigquery.FieldType(c[1])
	}
	return d
}

func Transform(w io.Writer, r io.Reader, sourceSchema []soda.Column, targetSchema bigquery.Schema) (int64, error) {
	dec := json.NewDecoder(r)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)

	// read open bracket
	var rows int64
	_, err := dec.Token()
	if err != nil {
		return rows, fmt.Errorf("initial token; rows %d %s", rows, err)
	}
	for dec.More() {
		rows += 1
		var m map[string]interface{}
		err := dec.Decode(&m)
		if err != nil {
			return rows, fmt.Errorf("row %d %s", rows, err)
		}
		// transform
		for i, f := range targetSchema {
			switch f.Type {
			case bigquery.DateFieldType:
				switch sourceSchema[i].DataTypeName {
				case "text":
					var s string
					switch ss := m[f.Name].(type) {
					case string:
						s = ss
					case nil:
						m[f.Name] = nil
						continue
					default:
						return rows, fmt.Errorf("error row %d casting %T as string %#v", rows, m[f.Name], m[f.Name])
					}
					if s == "" {
						m[f.Name] = nil
						continue
					}
					var d time.Time
					for _, timeFormat := range []string{"01/02/2006", "2006/01/02"} {
						d, err = time.Parse(timeFormat, s)
						if err == nil {
							break
						}
					}
					if err != nil {
						return rows, fmt.Errorf("error %s parsing %q", err, m[f.Name])
					}
					m[f.Name] = d.Format("2006-01-02")
				}
			case bigquery.StringFieldType:
				switch sourceSchema[i].DataTypeName {
				case "url":
					m[f.Name] = m[f.Name].(map[string]interface{})["url"]
				}
			}
		}
		log.Printf("[%d] %#v", rows, m)
		enc.Encode(m)
	}
	_, err = dec.Token()
	if err != nil {
		return rows, err
	}
	return rows, nil
}

func BQTableMetadata(s soda.Metadata, partitionField string, schemaOverride map[string]bigquery.FieldType) *bigquery.TableMetadata {
	var schema bigquery.Schema
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
	if !foundRequired {
		log.Panicf("partition column %q not detected", partitionField)
	}
	return &bigquery.TableMetadata{
		Name:                   ToTableName(s.ID),
		Description:            s.Name,
		Schema:                 schema,
		RequirePartitionFilter: true,
		TimePartitioning: &bigquery.TimePartitioning{
			Field: partitionField,
		},
	}
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	socrataDataset := flag.String("socrata-dataset", "", "the URL to the socrata dataset")
	bucketName := flag.String("gs-bucket", "", "google storage bucket name")
	projectID := flag.String("bq-project-id", "", "")
	datasetName := flag.String("bq-dataset", "", "bigquery dataset name")
	partitionColumn := flag.String("partition-column", "", "Date column to partition BQ table on")
	limit := flag.Int("limit", 100, "limit")
	var schemaOverride StringArray
	flag.Var(&schemaOverride, "schema-override", "override a field schema field:type (may be given multiple times)")
	flag.Parse()

	if *socrataDataset == "" {
		log.Fatal("missing --socrata-dataset")
	}

	token := os.Getenv("SOCRATA_APP_TOKEN")
	sodareq := soda.NewGetRequest(*socrataDataset, token)
	md, err := sodareq.Metadata.Get()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Socrata: %s (%s) last modified %v", md.ID, md.Name, time.Time(md.RowsUpdatedAt).Format(time.RFC3339))
	LogSocrataSchema(md.Columns)

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

	bqTable := dataset.Table(ToTableName(md.ID))
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

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	bkt := client.Bucket(*bucketName)

	// bigquery so we where we left off
	// create if needed
	sodareq.Query.Limit = uint(*limit)
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
	obj := bkt.Object(filepath.Join("socrata_to_bigquery", time.Now().Format("20060102-150405"), md.ID+".json"))
	log.Printf("streaming GS gs://%s/%s", *bucketName, obj.ObjectName())
	w := obj.NewWriter(ctx)
	w.ObjectAttrs.ContentType = "application/json"
	rows, transformErr := Transform(w, resp.Body, md.Columns, tmd.Schema)
	log.Printf("wrote %d rows to Google Storage", rows)
	err = w.Close()
	if err != nil {
		log.Fatal(err)
	}
	resp.Body.Close()
	if transformErr != nil {
		log.Fatal(transformErr)
	}

	// load into bigquery

	gcsRef := bigquery.NewGCSReference(fmt.Sprintf("gs://%s/%s", *bucketName, obj.ObjectName()))
	// gcsRef.MaxBadRecords
	gcsRef.SourceFormat = bigquery.JSON
	// gcsRef.IgnoreUnknownValues = true
	// gcsRef.SkipLeadingRows = 1
	// gcsRef.AllowQuotedNewlines = AllowNewLines

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
