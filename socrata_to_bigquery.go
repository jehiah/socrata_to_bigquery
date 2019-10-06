package main

import (
	"context"
	"flag"
	"io"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/SebastiaanKlippert/go-soda"
)

func main() {
	socrataDataset := flag.String("socrata-dataset", "", "the URL to the socrata dataset")
	bucketName := flag.String("gs-bucket", "", "google storage bucket name")
	projectID := flag.String("bq-project-id", "", "")
	datasetName := flag.String("bq-dataset", "", "bigquery dataset name")
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
	// log.Printf("%s dataset %#v", *socrataDataset, md)
	// modified, err := sodareq.Modified()
	// if err != nil {
	// 	log.Fatal(err)
	// }
	log.Printf("%s last modified %v", *socrataDataset, time.Time(md.RowsUpdatedAt).Format(time.RFC3339))

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
	bqmd, err := dataset.Metadata(ctx)
	if err != nil {
		// TODO: dataset doesn't exist? require that first?
		log.Fatal(err)
	}
	log.Printf("BQ metadata %#v", bqmd)
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
