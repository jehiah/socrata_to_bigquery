package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

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
		case bigquery.NumericFieldType, bigquery.FloatFieldType:
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
			out[fieldName] = sourceValue
			// TODO: improve conversion
		case bigquery.BooleanFieldType:
			out[fieldName] = sourceValue.(bool)
		default:
			return nil, fmt.Errorf("unhandled BigQuery type %q for field %q value %T %#v", schema.Type, fieldName, sourceValue, sourceValue)
		}
		if err != nil {
			switch schema.OnError {
			case SkipValue:
				log.Printf("skipping invalid value %q in field %q %s", sourceValue, schema.SourceField, err)
				out[fieldName] = nil
			case SkipRow, "":
				log.Printf("skipping row. invalid value %q in field %q %s", sourceValue, schema.SourceField, err)
				return nil, nil
			case RaiseError:
				return nil, err
			}
		}
	}
	return out, nil
}

func MustGeoJSON(v interface{}, err error) interface{} {
	if err != nil {
		panic(err)
	}
	return v
}

func ToGeoJSONPoint(v interface{}) (interface{}, error) {
	// {"type":"Point","coordinates":[-73.96481,40.633247]}
	// TODO: validate that this is a simple geo; Point, LineString, etc
	if v == nil {
		return nil, nil
	}
	b, err := json.Marshal(v)
	return string(b), err
}
func ToGeoJSONLocation(v interface{}) (interface{}, error) {
	if v == nil {
		return nil, nil
	}
	var p interface{}
	switch m := v.(type) {
	case []interface{}:
		// []interface {}{"{\"address\": \"\", \"city\": \"\", \"state\": \"\", \"zip\": \"\"}", "40.79634697983548", "-73.97053598278849", interface {}(nil), false}
		var lat, lon json.Number
		if s, ok := m[1].(string); ok {
			lat = json.Number(s)
		} else if !ok {
			return nil, nil
		}
		if s, ok := m[2].(string); ok {
			lon = json.Number(s)
		} else if !ok {
			return nil, nil
		}
		p = map[string]interface{}{
			"type":        "Point",
			"coordinates": []interface{}{lon, lat},
		}
	case map[string]interface{}:
		// map[string]interface {}{"human_address":"{\"address\": \"\", \"city\": \"\", \"state\": \"\", \"zip\": \"\"}", "latitude":"40.60763791400439", "longitude":"-73.92158534567228"}
		lat, oklat := m["latitude"].(string)
		lon, oklon := m["longitude"].(string)
		if !oklat || !oklon {
			return nil, nil
		}
		p = map[string]interface{}{
			"type":        "Point",
			"coordinates": []interface{}{json.Number(lon), json.Number(lat)},
		}
	default:
		return nil, fmt.Errorf("ToGeoJSONLocation: unhandled type %T %#v", v, v)
	}
	b, err := json.Marshal(p)
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
