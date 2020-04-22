package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"path/filepath"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	soda "github.com/SebastiaanKlippert/go-soda"
	"google.golang.org/api/googleapi"
)

func Download(ctx context.Context, cf ConfigFile, r io.ReadCloser, token string, bkt *storage.BucketHandle, bqTable *bigquery.Table, quiet bool) error {

	if r == nil {
		// i.e 'https://data.cityofnewyork.us/api/views/${ID}/rows.json?accessType=DOWNLOAD'
		api := cf.APIBase()
		api.Path = fmt.Sprintf("/api/views/%s/rows.json", url.PathEscape(cf.DatasetID()))
		api.RawQuery = "accessType=DOWNLOAD"
		req, err := http.NewRequest("GET", api.String(), nil)
		if err != nil {
			return err
		}
		req.Header.Set("X-App-Token", token)
		fmt.Printf("Streaming from %s\n", req.URL)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode >= 400 {
			return fmt.Errorf("got unexpected response %d", resp.StatusCode)
		}
		r = resp.Body
	}

	// stream to a google storage file
	obj := bkt.Object(filepath.Join("socrata_to_bigquery", time.Now().Format("20060102-150405"), cf.DatasetID()+".json.gz"))
	fmt.Printf("streaming to %s/%s\n", cf.GSBucket(), obj.ObjectName())
	w := obj.NewWriter(ctx)
	w.ObjectAttrs.ContentType = "application/json"
	w.ObjectAttrs.ContentEncoding = "gzip"
	gw := gzip.NewWriter(w)

	rows, transformErr := TransformDownload(gw, r, cf.Schema, quiet, 0)
	fmt.Printf("wrote %d rows to Google Storage\n", rows)
	if transformErr != nil {
		log.Printf("transformErr: %s", transformErr)
	}
	err := gw.Close()
	if err != nil {
		return err
	}
	err = w.Close()
	if err != nil {
		return err
	}
	r.Close()
	if transformErr != nil {
		return transformErr
	}
	// os.Exit(1)

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
		fmt.Printf("BigQuery import running job %s\n", loadJob.ID())
		status, err := loadJob.Wait(ctx)
		fmt.Printf("BigQuery import job %s done\n", loadJob.ID())
		if err != nil {
			return err
		}
		if err = status.Err(); err != nil {
			return err
		}
	}

	// cleanup google storage
	// fmt.Printf("removing temp file %s/%s", cf.GSBucket(), obj.ObjectName())
	if err = obj.Delete(ctx); err != nil {
		return err
	}
	return nil
}

func downloadOne(configFile string, quiet bool, r io.ReadCloser, token string) {
	cf, err := LoadConfigFile(configFile)
	if err != nil {
		log.Fatal(err)
	}
	if cf.GoogleStorageBucketName == "" {
		log.Fatalf("missing GoogleStorageBucketName in %q", configFile)
	}

	// todo Validate
	// bg-project-id, etc

	sodareq := soda.NewGetRequest(cf.Dataset, token)
	sodareq.Query.Select = []string{":*", "*"}
	md, err := sodareq.Metadata.Get()
	// https://data.cityofnewyork.us/api/views/${ID}/rows.json?accessType=DOWNLOAD

	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Socrata: %s (%s) last modified %v\n", md.ID, md.Name, time.Time(md.RowsUpdatedAt).Format(time.RFC3339))

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
	fmt.Printf("BQ Dataset %s OK (last modified %s)\n", dmd.FullID, dmd.LastModifiedTime)

	bqTable := dataset.Table(cf.BigQuery.TableName)
	tmd, err := bqTable.Metadata(ctx)
	if err != nil {
		if e, ok := err.(*googleapi.Error); ok {
			if e.Code == 404 {
				log.Printf("Auto-creating table %s\n", e.Message)
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

	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	bkt := client.Bucket(cf.GoogleStorageBucketName)

	Download(ctx, cf, r, token, bkt, bqTable, quiet)
	fmt.Printf("done\n")
}
