package main

import (
	"fmt"
	"os"
	"strings"

	"cloud.google.com/go/bigquery"
	soda "github.com/SebastiaanKlippert/go-soda"
	toml "github.com/pelletier/go-toml"
)

type OnError string

const (
	SkipValue  OnError = "SKIP_VALUE"
	SkipRow    OnError = "SKIP_ROW"
	RaiseError OnError = "ERROR"
)

// SchemaField represents a toml record which configures how data will be transformed from Socrata to BigQuery
type SchemaField struct {
	SourceField     string             `toml:"source_field"`
	SourceFieldType string             `toml:"source_field_type,omitempty"`
	Description     string             `toml:"description,omitempty"`
	Type            bigquery.FieldType `toml:"bigquery_type"`
	TimeFormat      string             `comment:"the time.Parse format string" toml:"time_format,omitempty"`
	Required        bool               `toml:"required"`
	OnError         OnError            `comment:"SKIP_VALUE | SKIP_ROW | ERROR " toml:"on_error,omitempty"`
	ExampleValues   string             `commented:"true" toml:"example_values,omitempty"`
}
type TableSchema map[string]SchemaField

type Config struct {
	Dataset                 string `comment:"The URL to the Socrata dataset"`
	GoogleStorageBucketName string
	BigQuery                BigQuery
}

func (c Config) GSBucket() string {
	return "gs://" + c.GoogleStorageBucketName
}

// BigQuery Settings
type BigQuery struct {
	ProjectID   string
	DatasetName string
	TableName   string
	Description string
	WhereFilter string `comment:"restrict sync to $where=..."`
}

func (bq BigQuery) SQLTableName() string {
	return fmt.Sprintf("`%s.%s.%s`", bq.ProjectID, bq.DatasetName, bq.TableName)
}

type ConfigFile struct {
	Config
	Schema TableSchema `toml:"schema"`
}

func LoadConfigFile(name string) (ConfigFile, error) {
	var cf ConfigFile
	f, err := os.Open(name)
	if err != nil {
		return cf, err
	}
	defer f.Close()
	err = toml.NewDecoder(f).Decode(&cf)
	return cf, err
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
			Description: strings.TrimSpace(md.Name),
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
			// TODO better guessing
			return bigquery.TimeFieldType, "03:04pm"
		}
		return bigquery.StringFieldType, ""
	case "number":
		return bigquery.NumericFieldType, ""
	case "calendar_date":
		return bigquery.DateFieldType, "2006-01-02T00:00:00.000"
	case "point":
		return bigquery.GeographyFieldType, ""
	}
	panic(fmt.Sprintf("unknown type %q", t))
}

func NewSchema(s soda.Metadata, examples map[string]string) TableSchema {
	t := TableSchema{
		"_id": SchemaField{
			SourceField:   ":id",
			Type:          bigquery.StringFieldType,
			Required:      true,
			ExampleValues: examples[":id"],
		},
		"_created_at": SchemaField{
			SourceField:   ":created_at",
			Type:          bigquery.TimestampFieldType,
			Required:      true,
			ExampleValues: examples[":created_at"],
		},
		"_updated_at": SchemaField{
			SourceField:   ":updated_at",
			Type:          bigquery.TimestampFieldType,
			Required:      true,
			ExampleValues: examples[":updated_at"],
		},
		"_version": SchemaField{
			SourceField:   ":version",
			Type:          bigquery.StringFieldType,
			Required:      false,
			ExampleValues: examples[":version"],
		},
	}
	for _, c := range s.Columns {
		fieldType, timeFormat := GuessBQType(c.DataTypeName, c.FieldName)
		var oe OnError
		if timeFormat != "" {
			oe = SkipValue
		}
		t[c.FieldName] = SchemaField{
			SourceField:     c.FieldName,
			SourceFieldType: c.DataTypeName,
			Type:            fieldType,
			TimeFormat:      timeFormat,
			Required:        false,
			Description:     strings.TrimSpace(c.Name),
			ExampleValues:   examples[c.FieldName],
			OnError:         oe,
		}
	}
	return t
}

func (t TableSchema) BigQuerySchema() bigquery.Schema {
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
