# socrata_to_bigquery

This tool facilitates replicating Open Data from the [Socrata Platform](https://socrata.com/) to [Google BigQuery](https://cloud.google.com/bigquery/)

Many Governemnt Open-Data projects are hosted on Socrata, and searchable through the [Open Data Network](https://www.opendatanetwork.com/)

* https://opendata.cityofnewyork.us/
* https://data.ny.gov/
* etc...

## Installing

```bash
go get github.com/jehiah/socrata_to_bigquery/...
```

## Quick Start

1. `socrata_to_bigquery init`

2. `socrata_to_bigquery download`

3. `socrata_to_bigquery sync`


## Documentation

### `init`

`init` initializes a yaml config file for synchronizing a Socrata dataset to BigQuery. 

Usage: `init -api-endpoint=https://path/to/api [-project-id -bq-dataset]`

i.e. `socrata_to_bigquery init -api-endpoint=https://data.cityofnewyork.us/resource/nc67-uf89 -data-dir=/path/to/data`

API endpoint is the published Socrata API endpoint for a dataset.

This config file defines all fields that will be loaded to BigQuery, and the target bigquery project and dataset. Optionally it can defines custom conversion from TEXT socrata field to richer DATE or TIME field types. It also defines the target bigquery field names.

For example, this `issue_date` is a `"text"` format in Socrata but it will be parsed using the Go format string `"01/02/2006"` and stored in a `DATE` column. `on_error = "SKIP_ROW"` indicates that any rows that do not meet this date format will be skipped.

```
  [schema.issue_date]
    bigquery_type = "DATE"
    description = "Issue Date"
    # example_values = "\"03/06/2017\", \"10/07/2017\", \"05/29/2016\""

    # SKIP_VALUE | SKIP_ROW | ERROR 
    on_error = "SKIP_ROW"
    required = true
    source_field = "issue_date"
    source_field_type = "text"

    # the time.Parse format string
    time_format = "01/02/2006"
```


```
Usage of socrata_to_bigquery init:
  -api-endpoint string
    	The URL to the socrata dataset
  -bq-dataset string
    	BigQuery Dataset
  -data-dir string
    	directory to create config file in
  -debug
    	show debug output
  -filename string
    	defaults to ${NAME}-${ID}.toml
  -project-id string
    	Google Cloud Project ID
  -socrata-app-token string
    	Socrata App Token (also src SOCRATA_APP_TOKEN env)
```

### `download`

Download does an initial copy from Socrata to Bigquery

Usage: `socrata_to_bigquery download /path/to/config.yaml`

i.e. `socrata_to_bigquery download open-parking-and-camera-violations-nc67-uf89.toml`

### `sync`

Sync does a periodic copy of new records from Socrata to BigQuery copying only new records since the most recent record in BigQuery.

Usage: `socrata_to_bigquery sync /path/to/config.yaml`

i.e. `socrata_to_bigquery sync open-parking-and-camera-violations-nc67-uf89.toml`

## Setup

Socrata API Token

https://dev.socrata.com/docs/authentication.html

```bash
export SOCRATA_APP_TOKEN=...
```
