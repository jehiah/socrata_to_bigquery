package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"
	"strconv"

	"cloud.google.com/go/bigquery"
)

type Record map[string]interface{}

// Transform converts a JSON export from Socrata to a JSON valid for the target schema on BigQuery
func Transform(w io.Writer, r io.Reader, s TableSchema, quiet bool, estRows uint64) (uint64, error) {
	dec := json.NewDecoder(r)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)

	// read open bracket
	var rows uint64
	_, err := dec.Token()
	if err != nil {
		return rows, fmt.Errorf("initial token; rows %d %w", rows, err)
	}
	start := time.Now()
	for dec.More() {
		rows += 1
		var m Record
		err := dec.Decode(&m)
		if err != nil {
			return rows, fmt.Errorf("row %d %w", rows, err)
		}
		mm, err := TransformOne(m, s)
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
			remain := estRows - rows
			etr := (time.Duration(remain) * speed).Truncate(time.Second)
			log.Printf("processed %d rows (%s). Remaining: %d rows (%s)", rows, duration, remain, etr)
		}
	}
	if !quiet && rows%100000 != 0 {
		duration := time.Since(start).Truncate(time.Second)
		log.Printf("processed %d rows (%s)", rows, duration)
	}
	// read the close bracket
	_, err = dec.Token()
	if err != nil {
		return rows, err
	}
	return rows, nil
}

func TransformOne(m Record, s TableSchema) (Record, error) {
	out := make(Record, len(m))
	for fieldName, schema := range s {
		sourceValue := m[schema.SourceField]
		var err error
		switch schema.Type {
		case bigquery.NumericFieldType:
			switch schema.SourceFieldType {
			case "text":
				if sourceValue != nil {
					out[fieldName], err = strconv.ParseFloat(strings.ReplaceAll(sourceValue.(string), ",", ""), 64)
				}
			default:
				out[fieldName] = sourceValue
			}
		case bigquery.StringFieldType:
			switch schema.SourceFieldType {
			case "url":
				out[fieldName] = sourceValue.(map[string]interface{})["url"]
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

func ToGeoJSON(v interface{}) (interface{}, error) {
	// {"type":"Point","coordinates":[-73.96481,40.633247]}
	// TODO: validate that this is a simple geo; Point, LineString, etc
	if v == nil {
		return nil, nil
	}
	b, err := json.Marshal(v)
	return string(b), err
}

func ToDate(format, s string) (interface{}, error) {
	if s == "" {
		return nil, nil
	}
	t, err := time.Parse(format, s)
	if err != nil {
		return nil, err
	}
	return t.Format("2006-01-02"), nil
}

func ToTime(format, s string) (interface{}, error) {
	if s == "" {
		return nil, nil
	}
	// lower string and format
	format = strings.ToLower(format)
	s = strings.ToLower(s)

	// convert a format of "0301p" -> "0301pm"
	if strings.HasSuffix(format, "p") {
		s = s + "m"
		format = format + "m"
	}
	t, err := time.Parse(format, s)
	if err != nil {
		return nil, err
	}
	return t.Format("15:04:05"), nil
}
