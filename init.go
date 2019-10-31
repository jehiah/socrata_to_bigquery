package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	soda "github.com/SebastiaanKlippert/go-soda"
	toml "github.com/pelletier/go-toml"
)

func initDataset(args []string) {
	initFlagSet := flag.NewFlagSet("init", flag.ExitOnError)
	dataset := initFlagSet.String("dataset", "", "The URL to the socrata dataset")
	token := initFlagSet.String("socrata-app-token", "", "Socrata App Token (also src SOCRATA_APP_TOKEN env)")
	debug := initFlagSet.Bool("debug", false, "show debug output")
	dataDir := initFlagSet.String("data-dir", "", "directory to create config file in")
	fn := initFlagSet.String("filename", "", "defaults to ${ID}.toml")
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

	filename := *fn
	if filename == "" {
		filename = fmt.Sprintf("%s.toml", md.ID)
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
			case map[string]interface{}:
				if u, ok := v["url"]; ok && u != nil{
					buffer[k] = append(buffer[k], u.(string))
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
	return out, nil
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
