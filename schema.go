package main

import (
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	soda "github.com/SebastiaanKlippert/go-soda"
)

// SchemaField represents a toml record which configures how data will be transformed from Socrata to BigQuery
type SchemaField struct {
	SourceField   string
	Description   string             `omitempty`
	Type          bigquery.FieldType `comment:"The Field Type in BigQuery"`
	TimeFormat    string             `comment:"the time.Parse format string" omitempty`
	Required      bool
	OnError       string `comment:"SKIP | ERROR" omitempty`
	ExampleValues string `commented:"true" omitempty`
}
type TableSchema map[string]SchemaField

type Config struct {
	Dataset                 string `comment:"The URL to the Socrata dataset"`
	GoogleStorageBucketName string
	BigQuery                BigQuery
}

// BigQuery Settings
type BigQuery struct {
	ProjectID   string
	DatasetName string
	TableName   string
	Description string
	WhereFilter string `comment:"restrict sync to $where=..."`
}

func ToTableName(id, name string) string {
	r := strings.NewReplacer("-", "_", " ", "_")
	return r.Replace(id + "-" + strings.ToLower(name))
}

func NewConfig(url string, md soda.Metadata) Config {
	return Config{
		Dataset: url,
		BigQuery: BigQuery{
			TableName:   ToTableName(md.ID, md.Name),
			Description: md.Name,
		},
	}
}

func GuessBQType(t, name string) (bigquery.FieldType, string) {
	switch t {
	case "text", "url":
		if strings.Contains(name, "date") {
			return bigquery.DateFieldType, "2006/01/02"
		}
		if strings.Contains(name, "time") {
			return bigquery.TimeFieldType, "03:04p"
		}
		return bigquery.StringFieldType, ""
	case "number":
		return bigquery.NumericFieldType, ""
	case "calendar_date":
		return bigquery.DateFieldType, ""
	case "point":
		return bigquery.GeographyFieldType, ""
	}
	panic(fmt.Sprintf("unknown type %q", t))
}

func NewSchema(s soda.Metadata) TableSchema {
	t := TableSchema{
		"_id": SchemaField{
			SourceField: ":id",
			Type:        bigquery.StringFieldType,
			Required:    true,
		},
		"_created_at": SchemaField{
			SourceField: ":created_at",
			Type:        bigquery.TimestampFieldType,
			Required:    true,
		},
		"_updated_at": SchemaField{
			SourceField: ":updated_at",
			Type:        bigquery.TimestampFieldType,
			Required:    true,
		},
		"_version": SchemaField{
			SourceField: ":version",
			Type:        bigquery.StringFieldType,
			Required:    false,
		},
	}
	for _, c := range s.Columns {
		fieldType, timeFormat := GuessBQType(c.DataTypeName, c.FieldName)
		t[c.FieldName] = SchemaField{
			SourceField: c.FieldName,
			Type:        fieldType,
			TimeFormat:  timeFormat,
			Required:    false,
			Description: c.Name,
		}
	}
	return t
}

func (t TableSchema) BigquerySchema(map[string]SchemaField) bigquery.Schema {
	var s bigquery.Schema
	for name, schema := range t {
		s = append(s, &bigquery.FieldSchema{
			Name:        name,
			Description: schema.Description,
			Type:        schema.Type,
			Required:    schema.Required,
		})
	}
	return s
}
