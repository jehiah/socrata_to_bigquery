package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"

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
	if err := initFlagSet.Parse(args); err != nil {
		log.Fatal(err)
	}

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

	ctx := context.Background()
	// Construct a ConfigFile just to use APIBase/DatasetID parsing
	cf := ConfigFile{Config: Config{Dataset: *apiEndpoint}}
	apiBase := cf.APIBase()
	datasetID := cf.DatasetID()

	md, err := FetchMetadata(ctx, apiBase, datasetID, *token)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Found Socrata Dataset: %s (%s) last modified %v\n", md.ID, md.Name, md.RowsUpdatedAtTime().Format("2006-01-02T15:04:05Z07:00"))

	if *debug {
		LogSocrataSchema(md.Columns)
	}

	examples, err := FetchExampleRecords(ctx, apiBase, datasetID, *token)
	if err != nil {
		log.Fatal(err)
	}

	filename := *fn
	if filename == "" {
		filename = fmt.Sprintf("%s.toml", strings.ReplaceAll(ToTableName(md.ID, md.Name), "_", "-"))
	}
	if *dataDir != "" {
		filename = filepath.Join(*dataDir, filename)
	}
	fmt.Printf("creating %s\n", filename)
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	encoder := toml.NewEncoder(f)
	c := NewConfig(*apiEndpoint, *md)
	c.BigQuery.ProjectID = *bqProject
	c.BigQuery.DatasetName = *bqDataset
	if err := encoder.Encode(c); err != nil {
		log.Fatal(err)
	}
	if err := encoder.Encode(map[string]TableSchema{"schema": NewSchema(*md, ExampleRecords(examples))}); err != nil {
		log.Fatal(err)
	}
}

func FetchExampleRecords(ctx context.Context, apiBase *url.URL, datasetID, token string) ([]map[string]interface{}, error) {
	fmt.Println("Fetching example records.")
	var examples []map[string]interface{}
	err := StreamV3(ctx, apiBase, datasetID, "SELECT :*, * LIMIT 10", token, func(row Record) error {
		examples = append(examples, map[string]interface{}(row))
		return nil
	})
	return examples, err
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
			case float64:
				buffer[k] = append(buffer[k], fmt.Sprintf("%f", v))
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
			case nil:
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
