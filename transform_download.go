package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"time"

	"cloud.google.com/go/bigquery"
	soda "github.com/SebastiaanKlippert/go-soda"
)

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
				log.Printf("processed %d rows (%s)", rows, duration)
			} else {
				remain := estRows - rows
				etr := (time.Duration(remain) * speed).Truncate(time.Second)
				log.Printf("processed %d rows (%s). Remaining: %d rows (%s)", rows, duration, remain, etr)
			}
		}
	}
	if !quiet && rows%100000 != 0 {
		duration := time.Since(start).Truncate(time.Second)
		log.Printf("processed %d rows (%s)", rows, duration)
	}
	token() // close ]
	token() // close }
	return rows, err
}

type ListRecord []interface{}

func TransformOneList(l ListRecord, s OrderedTableSchema) (Record, error) {
	out := make(Record, len(l))
	out["_id"] = l[0].(string)
	// uuid
	c, err := l[3].(json.Number).Int64()
	if err != nil {
		return nil, err
	}
	out["_created_at"] = time.Unix(c, 0).Format(time.RFC3339)
	// cteated_meta
	c, err = l[5].(json.Number).Int64()
	if err != nil {
		return nil, err
	}
	out["_updated_at"] = time.Unix(c, 0).Format(time.RFC3339)
	// updated_meta
	// :meta

	for i, sourceValue := range l[5:] {
		schema := s[i]
		fieldName := schema.FieldName
		var err error
		switch schema.Type {
		case bigquery.NumericFieldType:
			out[fieldName] = sourceValue
		case bigquery.StringFieldType:
			switch schema.SourceFieldType {
			case "url":
				out[fieldName] = sourceValue.([]interface{})[0]
			case "text", "":
				out[fieldName] = sourceValue
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
				out[fieldName], err = ToGeoJSON(sourceValue)
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
		case bigquery.TimestampFieldType:
			out[fieldName] = sourceValue
			// TODO: improve conversion
		default:
			return nil, fmt.Errorf("unhandled BigQuery type %q for field %q", schema.Type, fieldName)
		}
		if err != nil {
			switch schema.OnError {
			case SkipValue:
				log.Printf("skipping invalid value %q in field %q %w", sourceValue, schema.SourceField, err)
				out[fieldName] = nil
			case SkipRow, "":
				log.Printf("skipping row. invalid value %q in field %q %w", sourceValue, schema.SourceField, err)
				return nil, nil
			case RaiseError:
				return nil, err
			}
		}
	}
	return out, nil
}
