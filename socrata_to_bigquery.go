package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/SebastiaanKlippert/go-soda"
	"google.golang.org/api/googleapi"
)

func LogSocrataSchema(c []soda.Column) {
	for i, cc := range c {
		fmt.Printf("[%d] %q (%s) %s %#v\n", i, cc.FieldName, cc.DataTypeName, cc.Name, cc)
	}
}
func DataTypeNameToFieldType(t string) bigquery.FieldType {
	switch t {
	case "text":
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

func BQTableMetadata(s soda.Metadata, partitionField string) *bigquery.TableMetadata {
	var schema bigquery.Schema
	var foundRequired bool
	for _, c := range s.Columns {
		var required bool
		if c.FieldName == partitionField {
			required = true
			foundRequired = true
		}
		schema = append(schema, &bigquery.FieldSchema{
			Name:        c.FieldName,
			Description: c.Name,
			Type:        DataTypeNameToFieldType(c.DataTypeName),
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
	socrataDataset := flag.String("socrata-dataset", "", "the URL to the socrata dataset")
	bucketName := flag.String("gs-bucket", "", "google storage bucket name")
	projectID := flag.String("bq-project-id", "", "")
	datasetName := flag.String("bq-dataset", "", "bigquery dataset name")
	partitionColumn := flag.String("partition-column", "", "Date column to partition BQ table on")
	flag.Parse()

	if *socrataDataset == "" {
		log.Fatal("missing --socrata-dataset")
	}

	var token string
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
				if err := bqTable.Create(ctx, BQTableMetadata(*md, *partitionColumn)); err != nil {
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

	// check table?

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	bkt := client.Bucket(*bucketName)
	log.Printf("%#v", client)

	// bigquery so we where we left off
	// create if needed

	// query socrata
	// stream to a google storage file
	obj := bkt.Object("tempfilename")
	w := obj.NewWriter(ctx)
	bytes, err := io.Copy(w, strings.NewReader(``))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("read %d bytes", bytes)
	err = w.Close()
	if err != nil {
		log.Fatal(err)
	}

	// load into bigquery
}
