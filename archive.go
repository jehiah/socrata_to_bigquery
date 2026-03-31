package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"cloud.google.com/go/storage"
)

func archiveCmd(args []string) {
	flagSet := flag.NewFlagSet(fmt.Sprintf("%s archive", os.Args[0]), flag.ExitOnError)
	quiet := flagSet.Bool("quiet", false, "disable progress output")
	token := flagSet.String("socrata-app-token", "", "Socrata App Token (also src SOCRATA_APP_TOKEN env)")
	if err := flagSet.Parse(args); err != nil {
		log.Fatal(err)
	}
	if *token == "" {
		*token = os.Getenv("SOCRATA_APP_TOKEN")
	}
	if *token == "" {
		fmt.Fprintln(os.Stderr, "missing --socrata-app-token or environment variable SOCRATA_APP_TOKEN")
		os.Exit(1)
	}

	if flagSet.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "missing filename")
		os.Exit(1)
	}
	for _, configFile := range flagSet.Args() {
		archiveOne(configFile, *quiet, *token)
	}
}

func archiveOne(configFile string, quiet bool, token string) {
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
	fmt.Printf("Archiving Socrata: %s (%s) (last modified %v)\n", md.ID, md.Name, md.RowsUpdatedAtTime().Format(time.RFC3339))

	socrataCount, err := CountV3(ctx, apiBase, datasetID, cf.BigQuery.WhereFilter, token)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Socrata Records: %d\n", socrataCount)

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	bkt := client.Bucket(cf.GoogleStorageBucketName)
	tableName := ToTableName(datasetID, md.Name)
	objPath := filepath.Join("socrata_archive", tableName, time.Now().Format("20060102-150405")+".json.gz")
	obj := bkt.Object(objPath)

	fmt.Printf("> writing to %s/%s\n", cf.GSBucket(), obj.ObjectName())

	w := obj.NewWriter(ctx)
	w.ContentType = "application/json"
	w.ContentEncoding = "gzip"
	w.PredefinedACL = "publicRead"

	var pw *ProgressWriter
	var innerWriter io.Writer = w
	if !quiet {
		pw = NewProgressWriter(w, time.Minute)
		innerWriter = pw
	}
	bw := bufio.NewWriterSize(innerWriter, 1*1024*1024) // 1MB buffer
	gw := gzip.NewWriter(bw)

	sql := "SELECT :*, *"
	if cf.BigQuery.WhereFilter != "" {
		sql += " WHERE " + cf.BigQuery.WhereFilter
	}

	start := time.Now()
	body, err := StreamRawV3(ctx, apiBase, datasetID, sql, token)
	if err != nil {
		log.Fatal(err)
	}

	written, err := io.Copy(gw, body)
	if err != nil {
		log.Fatal(err)
	}
	err = body.Close()
	if err != nil {
		log.Fatal(err)
	}

	if err := gw.Close(); err != nil {
		log.Fatal(err)
	}
	if err := bw.Flush(); err != nil {
		log.Fatal(err)
	}
	if err := w.Close(); err != nil {
		log.Fatal(err)
	}

	elapsed := time.Since(start).Truncate(time.Second)
	compressedSize := int64(0)
	if pw != nil {
		pw.Stop()
		compressedSize = pw.Total()
	}
	fmt.Printf("Archive complete: %s raw -> %s compressed in %s\n",
		humanBytes(written), humanBytes(compressedSize), elapsed)
	fmt.Printf("GCS: %s/%s\n", cf.GSBucket(), obj.ObjectName())
	fmt.Printf("URL: https://storage.googleapis.com/%s/%s\n", cf.GoogleStorageBucketName, obj.ObjectName())
}
