package main

import (
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	soda "github.com/SebastiaanKlippert/go-soda"
	toml "github.com/pelletier/go-toml"
)

func initDataset(args []string) {
	initFlagSet := flag.NewFlagSet(fmt.Sprintf("%s init", os.Args[0]), flag.ExitOnError)
	apiEndpoint := initFlagSet.String("api-endpoint", "", "The URL to the socrata dataset")
	token := initFlagSet.String("socrata-app-token", "", "Socrata App Token (also src SOCRATA_APP_TOKEN env)")
	debug := initFlagSet.Bool("debug", false, "show debug output")
	dataDir := initFlagSet.String("data-dir", "", "directory to create config file in")
	fn := initFlagSet.String("filename", "", "defaults to ${NAME}-${ID}.toml")
	bqProject := initFlagSet.String("project-id", "", "Google Cloud Project ID")
	bqDataset := initFlagSet.String("bq-dataset", "", "BigQuery Dataset")
	downloadFile := initFlagSet.String("download-file", "", "re-process existing download file (gzip supported)")
	initFlagSet.Parse(args)

	if *apiEndpoint == "" {
		fmt.Fprintln(os.Stderr, "missing --api-endpoint")
		os.Exit(1)
	}
	if *token == "" {
		*token = os.Getenv("SOCRATA_APP_TOKEN")
	}
	if *token == "" {
		fmt.Fprintln(os.Stderr, "missing --socrata-app-token or environment variable SOCRATA_APP_TOKEN")
		os.Exit(1)
	}

	var md *soda.Metadata
	var examples []map[string]interface{}
	var err error

	if *downloadFile != "" {
		var r io.ReadCloser
		fmt.Printf("Opening %s\n", *downloadFile)
		r, err = os.Open(*downloadFile)
		if err != nil {
			log.Fatal(err)
		}
		if strings.HasSuffix(*downloadFile, ".gz") {
			r, err = gzip.NewReader(r)
			if err != nil {
				log.Fatal(err)
			}
		}
		var data DownloadFile
		dec := json.NewDecoder(r)
		if err = dec.Decode(&data); err != nil {
			log.Fatal(err)
		}
		md = &data.Meta.Metadata
		examples = data.ExampleRecords()

	} else {
		sodareq := soda.NewGetRequest(*apiEndpoint, *token)
		// ":*" includes metadata records :id, :created_at, :updated_at, :version
		sodareq.Query.Select = []string{":*", "*"}
		md, err = sodareq.Metadata.Get()
		if err != nil {
			log.Fatal(err)
		}
		examples, err = FetchExampleRecords(*sodareq)
		if err != nil {
			log.Fatal(err)
		}
	}

	fmt.Printf("Found Socrata Dataset: %s (%s) last modified %v\n", md.ID, md.Name, time.Time(md.RowsUpdatedAt).Format(time.RFC3339))
	if *debug {
		LogSocrataSchema(md.Columns)
	}

	filename := *fn
	if filename == "" {
		filename = fmt.Sprintf("%s.toml", strings.Replace(ToTableName(md.ID, md.Name), "_", "-", -1))
	}
	if *dataDir != "" {
		filename = filepath.Join(*dataDir, filename)
	}
	fmt.Printf("creating %s\n", filename)
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	encoder := toml.NewEncoder(f)
	c := NewConfig(*apiEndpoint, *md)
	c.BigQuery.ProjectID = *bqProject
	c.BigQuery.DatasetName = *bqDataset
	encoder.Encode(c)
	encoder.Encode(map[string]TableSchema{"schema": NewSchema(*md, ExampleRecords(examples))})
}

func FetchExampleRecords(sr soda.GetRequest) ([]map[string]interface{}, error) {
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
	return data, nil
}

func ExampleRecords(data []map[string]interface{}) map[string]string {
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
			case map[string]interface{}:
				if u, ok := v["url"]; ok && u != nil {
					buffer[k] = append(buffer[k], u.(string))
				} else if gt, ok := v["type"]; ok && gt.(string) == "Point" {
					buffer[k] = append(buffer[k], MustGeoJSON(ToGeoJSONPoint(v)).(string))
				} else if _, ok := v["human_address"]; ok {
					buffer[k] = append(buffer[k], MustGeoJSON(ToGeoJSONLocation(v)).(string))
				} else {
					log.Printf("unhandled type %T %#v", v, v)
				}
			default:
				log.Printf("unhandled type %T %#v", v, v)
			}
		}
	}

	out := make(map[string]string)
	for k, v := range buffer {
		out[k] = strings.Join(uniqExamples(v, 3), ", ")
	}
	return out
}

func uniqExamples(a []string, max int) []string {
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
