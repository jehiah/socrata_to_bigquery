package main

import (
	"fmt"
	"net/url"
	"os"
	"strings"

	"cloud.google.com/go/bigquery"
	toml "github.com/pelletier/go-toml"
)

type OnError string

const (
	SkipValue  OnError = "SKIP_VALUE"
	SkipRow    OnError = "SKIP_ROW"
	RaiseError OnError = "ERROR"
)

type TimePartition string

const (
	TimePartitionHour  TimePartition = "HOUR"
	TimePartitionDay   TimePartition = "DAY"
	TimePartitionMonth TimePartition = "MONTH"
	TimePartitionYear  TimePartition = "YEAR"
)

// SchemaField represents a toml record which configures how data will be transformed from Socrata to BigQuery
type SchemaField struct {
	SourceField     string             `toml:"source_field"`
	SourceFieldType string             `toml:"source_field_type,omitempty"`
	Description     string             `toml:"description,omitempty"`
	Type            bigquery.FieldType `toml:"bigquery_type"`
	TimeFormat      string             `comment:"the time.Parse format string" toml:"time_format,omitempty"`
	TimePartition   TimePartition      `comment:"HOUR | DAY | MONTH | YEAR" toml:"time_partition,omitempty"`
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

func (cf ConfigFile) DatasetID() string {
	c := strings.Split(cf.Dataset, "/")
	return c[len(c)-1]
}
func (cf ConfigFile) APIBase() *url.URL {
	u, err := url.Parse(cf.Dataset)
	if err != nil {
		panic(err.Error())
	}
	u.Path = "/"
	return u
}

func LoadConfigFile(name string) (ConfigFile, error) {
	var cf ConfigFile
	f, err := os.Open(name)
	if err != nil {
		return cf, err
	}
	defer func() { _ = f.Close() }()
	err = toml.NewDecoder(f).Decode(&cf)
	return cf, err
}

func ToTableName(id, name string) string {
	r := strings.NewReplacer("-", "_", " ", "_")
	return r.Replace(strings.ToLower(name) + "-" + id)
}

func NewConfig(datasetURL string, md SocrataMetadata) Config {
	return Config{
		Dataset: datasetURL,
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
		return bigquery.DateTimeFieldType, "2006-01-02T15:04:05.000"
	case "point":
		return bigquery.GeographyFieldType, ""
	case "location":
		return bigquery.GeographyFieldType, ""
	case "checkbox":
		return bigquery.BooleanFieldType, ""
	}
	panic(fmt.Sprintf("unknown type %q", t))
}

func NewSchema(s SocrataMetadata, examples map[string]string) TableSchema {
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
		switch c.FieldName {
		case ":id", ":created_at", ":updated_at", ":version":
			continue
		}
		switch c.DataTypeName {
		case "meta_data":
			continue
		}
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

func parseTimePartitioningType(tp TimePartition) (bigquery.TimePartitioningType, error) {
	switch tp {
	case TimePartitionHour:
		return bigquery.HourPartitioningType, nil
	case TimePartitionDay:
		return bigquery.DayPartitioningType, nil
	case TimePartitionMonth:
		return bigquery.MonthPartitioningType, nil
	case TimePartitionYear:
		return bigquery.YearPartitioningType, nil
	default:
		return "", fmt.Errorf("must be one of HOUR, DAY, MONTH, YEAR")
	}
}

// TimePartitioning builds BigQuery partition configuration from schema settings.
// It validates that only one partition field is set, and that field is REQUIRED
// and typed DATE or TIMESTAMP.
func (t TableSchema) TimePartitioning() (*bigquery.TimePartitioning, error) {
	var name string
	var field SchemaField
	for fieldName, schemaField := range t {
		if schemaField.TimePartition == "" {
			continue
		}
		if name != "" {
			return nil, fmt.Errorf("multiple schema fields have time_partition configured: %q and %q", name, fieldName)
		}
		name = fieldName
		field = schemaField
	}

	if name == "" {
		return nil, nil
	}

	if !field.Required {
		return nil, fmt.Errorf("time_partition field %q must be required", name)
	}
	if field.Type != bigquery.DateFieldType && field.Type != bigquery.TimestampFieldType {
		return nil, fmt.Errorf("time_partition field %q must be DATE or TIMESTAMP (got %s)", name, field.Type)
	}

	typeValue, err := parseTimePartitioningType(field.TimePartition)
	if err != nil {
		return nil, fmt.Errorf("time_partition field %q %w", name, err)
	}

	return &bigquery.TimePartitioning{
		Type:  typeValue,
		Field: name,
	}, nil
}

func (t TableSchema) PartitionWhereClause() string {
	tp, err := t.TimePartitioning()
	if err != nil || tp == nil {
		return ""
	}
	return fmt.Sprintf("WHERE %s IS NOT NULL", bqIdentifier(tp.Field))
}
