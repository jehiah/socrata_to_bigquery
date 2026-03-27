package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

func usage() {
	fmt.Printf("Usage: %s [command]\n", os.Args[0])
	fmt.Println("https://github.com/jehiah/socrata_to_bigquery")
	fmt.Println("")
	fmt.Println("Commands:")
	fmt.Println(" - init")
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
	default:
		usage()
		os.Exit(1)
	}
}

func syncCmd(args []string) {
	flagSet := flag.NewFlagSet(fmt.Sprintf("%s sync", os.Args[0]), flag.ExitOnError)
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
		syncOne(configFile, *quiet, *token)
	}
}
