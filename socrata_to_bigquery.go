package main

import (
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

func usage() {
	fmt.Printf("Usage: %s [command]\n", os.Args[0])
	fmt.Println("https://github.com/jehiah/socrata_to_bigquery")
	fmt.Println("")
	fmt.Println("Commands:")
	fmt.Println(" - init")
	fmt.Println(" - download")
	fmt.Println(" - sync")
}

func main() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "init":
		initDataset(os.Args[2:])
	case "sync":
		syncCmd(os.Args[2:])
	case "download":
		downloadCmd(os.Args[2:])
	default:
		usage()
		os.Exit(1)
	}
}

func syncCmd(args []string) {
	flagSet := flag.NewFlagSet(fmt.Sprintf("%s sync", os.Args[0]), flag.ExitOnError)
	quiet := flagSet.Bool("quiet", false, "disable progress output")
	// https://support.socrata.com/hc/en-us/requests/37801
	// Socrata suggested 1M was too large a sync value
	pageSize := flagSet.Uint64("page-size", 500000, "socrata result set size")
	token := flagSet.String("socrata-app-token", "", "Socrata App Token (also src SOCRATA_APP_TOKEN env)")
	// limit := flag.Int("limit", 100000000, "limit")
	// where := flag.String("where", "", "$where clause")
	flagSet.Parse(args)
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
		syncOne(configFile, *quiet, *token, *pageSize)
	}
}

func downloadCmd(args []string) {
	flagSet := flag.NewFlagSet(fmt.Sprintf("%s download", os.Args[0]), flag.ExitOnError)
	quiet := flagSet.Bool("quiet", false, "disable progress output")
	token := flagSet.String("socrata-app-token", "", "Socrata App Token (also src SOCRATA_APP_TOKEN env)")
	downloadFile := flagSet.String("download-file", "", "re-process existing download file (gzip supported)")
	flagSet.Parse(args)
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
		var r io.ReadCloser
		if *downloadFile != "" {
			var err error
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
		}
		downloadOne(configFile, *quiet, r, *token)
	}
}
