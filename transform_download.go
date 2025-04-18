package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	soda "github.com/SebastiaanKlippert/go-soda"
)

type DownloadFile struct {
	Meta DownloadMeta `json:"meta"`
	Data []ListRecord `json:"data"`
}

func (d DownloadFile) ExampleRecords() []map[string]interface{} {
	var out []map[string]interface{}
	for i := 0; i < 10 && i < len(d.Data); i++ {
		row := make(map[string]interface{})
		for j, v := range d.Data[i] {
			row[d.Meta.Columns[j].FieldName] = v
		}
		out = append(out, row)
	}
	return out
}

// This is the "view" dict found in the download response format
// i.e 'https://data.cityofnewyork.us/api/views/${ID}/rows.json?accessType=DOWNLOAD'
type DownloadMeta struct {
	soda.Metadata `json:"view"`
}

// TransformDownload converts a JSON export from Socrata download a JSON valid for the target schema on BigQuery
func TransformDownload(w io.Writer, r io.Reader, s TableSchema, quiet bool, estRows uint64) (uint64, error) {
	dec := json.NewDecoder(r)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	dec.UseNumber()

	// read open bracket
	var err error
	var rows uint64
	token := func() {
		if err != nil {
			return
		}
		_, err = dec.Token()
	}
	token() // {
	token() // "meta"
	if err != nil {
		return rows, err
	}
	var metadata DownloadMeta
	err = dec.Decode(&metadata)
	if err != nil {
		return rows, err
	}

	os := s.ToOrdered(metadata.Columns)

	token() // "data"
	token() // [
	if err != nil {
		return rows, err
	}
	start := time.Now()
	for dec.More() {
		rows += 1
		var l ListRecord
		err := dec.Decode(&l)
		if err != nil {
			return rows, fmt.Errorf("row %d %w", rows, err)
		}
		mm, err := TransformOneList(l, os)
		if err != nil {
			return rows, fmt.Errorf("row %d %w", rows, err)
		}
		if mm != nil {
			err = enc.Encode(mm)
			if err != nil {
				return rows, fmt.Errorf("row %d %w", rows, err)
			}
		}
		if !quiet && rows%100000 == 0 {
			duration := time.Since(start).Truncate(time.Second)
			speed := duration / time.Duration(rows)
			if estRows == 0 {
				fmt.Printf("processed %d rows (%s)\n", rows, duration)
			} else {
				remain := estRows - rows
				etr := (time.Duration(remain) * speed).Truncate(time.Second)
				fmt.Printf("processed %d rows (%s). Remaining: %d rows (%s)\n", rows, duration, remain, etr)
			}
		}
		// // FIXME: temp debugging limit
		// if rows == 100000 {
		// 	return rows, nil
		// }
	}
	if !quiet && rows%100000 != 0 {
		duration := time.Since(start).Truncate(time.Second)
		fmt.Printf("processed %d rows (%s)\n", rows, duration)
	}
	token() // close ]
	token() // close }
	return rows, err
}

type ListRecord []interface{}

func truncateWithPrecision(s string, p int) string {
	// Find the position of the decimal point
	if dotIndex := strings.Index(s, "."); dotIndex != -1 {
		// Count the number of characters after the decimal point
		precision := len(s) - dotIndex - 1
		if precision > p {
			return s[:len(s)-(precision-p)]
		}
		return s
	}
	return s
}

func TransformOneList(l ListRecord, s OrderedTableSchema) (Record, error) {
	out := make(Record, len(l))
	// log.Printf("%#v", l)
	for i, sourceValue := range l {
		schema := s[i]
		fieldName := schema.FieldName
		if fieldName == "" {
			continue
		}
		// log.Printf("[%d] %q <- %q:%#v", i, fieldName, schema.SourceField, sourceValue)
		var err error
		switch schema.Type {
		case bigquery.NumericFieldType, bigquery.FloatFieldType:
			switch schema.SourceFieldType {
			case "number":
				if s, ok := sourceValue.(string); ok && s != "" {
					out[fieldName] = json.Number(truncateWithPrecision(s, bigquery.NumericScaleDigits))
				}
			default:
				out[fieldName] = sourceValue
			}
		case bigquery.StringFieldType:
			switch schema.SourceFieldType {
			case "url":
				out[fieldName] = sourceValue.([]interface{})[0]
			case "text", "":
				if sourceValue != nil {
					out[fieldName] = sourceValue

				}
				if schema.Required {
					if sv, ok := sourceValue.(string); ok && sv == "" || sourceValue == nil {
						err = fmt.Errorf("missing required field %q", fieldName)
					}
				}
			default:
				return nil, fmt.Errorf("unhandled conversion from %q to %q for field %s", schema.SourceFieldType, schema.Type, fieldName)
			}

		case bigquery.GeographyFieldType:
			switch schema.SourceFieldType {
			case "point":
				out[fieldName], err = ToGeoJSONPoint(sourceValue)
			case "location":
				out[fieldName], err = ToGeoJSONLocation(sourceValue)
			default:
				return nil, fmt.Errorf("unhandled conversion from %q to %q for field %s", schema.SourceFieldType, schema.Type, fieldName)
			}
		case bigquery.DateFieldType:
			if sourceValue != nil {
				var v interface{}
				v, err = ToDate(schema.TimeFormat, sourceValue.(string))
				out[fieldName] = v
				if schema.Required && v == nil && err == nil {
					err = fmt.Errorf("missing required field %q", fieldName)
				}
			} else if schema.Required {
				err = fmt.Errorf("missing required field %q", fieldName)
			}
		case bigquery.TimeFieldType:
			if sourceValue != nil {
				out[fieldName], err = ToTime(schema.TimeFormat, sourceValue.(string))
			} else if schema.Required {
				err = fmt.Errorf("missing required field %q", fieldName)
			}
		case bigquery.TimestampFieldType, bigquery.DateTimeFieldType:
			switch schema.SourceFieldType {
			case "json.Number":
				var c int64
				c, err = sourceValue.(json.Number).Int64()
				if err == nil {
					out[fieldName] = time.Unix(c, 0).Format(time.RFC3339)
				}
			default:
				// TODO: improve conversion
				out[fieldName] = sourceValue
			}
		case bigquery.BooleanFieldType:
			out[fieldName] = sourceValue.(bool)
		default:
			return nil, fmt.Errorf("unhandled BigQuery type %q for field %q value %T %#v", schema.Type, fieldName, sourceValue, sourceValue)
		}
		if err != nil {
			switch schema.OnError {
			case SkipValue:
				log.Printf("skipping invalid value %q in field %q %v", sourceValue, schema.SourceField, err)
				out[fieldName] = nil
			case SkipRow, "":
				log.Printf("skipping row. invalid value %q in field %q %v", sourceValue, schema.SourceField, err)
				return nil, nil
			case RaiseError:
				return nil, err
			}
		}
	}
	return out, nil
}
