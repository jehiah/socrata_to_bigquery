package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/SebastiaanKlippert/go-soda"
)

// Transform converts a JSON export from Socrata to a JSON valid for the target schema on BigQuery
func Transform(w io.Writer, r io.Reader, sourceSchema []soda.Column, targetSchema bigquery.Schema, minCreated time.Time, quiet bool) (int64, error) {
	dec := json.NewDecoder(r)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)

	// read open bracket
	var rows int64
	_, err := dec.Token()
	if err != nil {
		return rows, fmt.Errorf("initial token; rows %d %s", rows, err)
	}
	start := time.Now()
	for dec.More() {
		rows += 1
		var m map[string]interface{}
		err := dec.Decode(&m)
		if err != nil {
			return rows, fmt.Errorf("row %d %s", rows, err)
		}
		mm, err := TransformOne(m, sourceSchema, targetSchema, minCreated)
		if err != nil {
			log.Printf("%s row:%d data:%#v", err, rows, m)
			continue
		}
		if mm != nil {
			enc.Encode(mm)
		}
		if !quiet && rows%100000 == 0 {
			log.Printf("processed %d rows (%s)", rows, time.Since(start).Truncate(time.Second))
		}
	}
	if !quiet && rows%100000 != 0 {
		log.Printf("processed %d rows (%s)", rows, time.Since(start).Truncate(time.Second))
	}
	_, err = dec.Token()
	if err != nil {
		return rows, err
	}
	return rows, nil
}

func TransformOne(m map[string]interface{}, sourceSchema []soda.Column, targetSchema bigquery.Schema, minCreated time.Time) (map[string]interface{}, error) {
	var offset int
	for i, f := range targetSchema {
		switch f.Name {
		case "_created_at":
			if !minCreated.IsZero() {
				v := m[":"+f.Name[1:]]
				ts, err := time.Parse(time.RFC3339, v.(string))
				if err != nil {
					return nil, fmt.Errorf("invalid %s time %s", f.Name, err)
				}
				if ts.Before(minCreated) {
					return nil, fmt.Errorf("record :created_at %s is before minimum %s", v, minCreated.Format(time.RFC3339))
				}
			}
			fallthrough
		case "_id", "_updated_at", "_version":
			offset += 1
			// move the field from ":${field}" to "_${field}"
			k := ":" + f.Name[1:]
			m[f.Name] = m[k]
			delete(m, k)
			continue
		}
		sourceDataType := sourceSchema[i-offset].DataTypeName
		switch f.Type {
		case bigquery.GeographyFieldType:

			switch sourceDataType {
			case "point":
				var err error
				geo, err := ToGeoJSON(m[f.Name])
				if err != nil {
					return nil, err
				}
				if geo != "" {
					m[f.Name] = geo
				} else {
					m[f.Name] = nil
				}
			default:
				return nil, fmt.Errorf("unable to convert point of type %s (%#v) to %s", sourceDataType, m[f.Name], bigquery.GeographyFieldType)
			}
		case bigquery.DateFieldType:
			switch sourceDataType {
			case "calendar_date":
				switch ss := m[f.Name].(type) {
				// example: 2019-06-28T00:00:00.000
				case string:
					m[f.Name] = strings.SplitN(ss, "T", 2)[0]
				}
			case "text":
				var s string
				switch ss := m[f.Name].(type) {
				case string:
					s = ss
				case nil:
					if f.Required {
						return nil, fmt.Errorf("missing required field %q", f.Name)
					}
					m[f.Name] = nil
					continue
				default:
					return nil, fmt.Errorf("expected string found %T for field %q (value %v)", ss, f.Name, ss)
				}
				if s == "" {
					if f.Required {
						return nil, fmt.Errorf("missing required field %q", f.Name)
					}
					m[f.Name] = nil
					continue
				}
				var d time.Time
				var err error
				for ti, timeFormat := range []string{"01/02/2006", "2006/01/02"} {
					d, err = time.Parse(timeFormat, s)
					if err == nil {
						break
					}
					// if the format is good, but the date is not
					if strings.Contains(err.Error(), "out of range") && ti != 0 {
						return nil, fmt.Errorf("invalid timestamp %q field %q err:%s format:%q", s, f.Name, err, timeFormat)
					}
				}
				if err != nil {
					return nil, fmt.Errorf("error parsing %s field %q value:%q", err, f.Name, m[f.Name])
				}
				m[f.Name] = d.Format("2006-01-02")
			}
		case bigquery.StringFieldType:
			switch sourceDataType {
			case "url":
				m[f.Name] = m[f.Name].(map[string]interface{})["url"]
			}
		}
		if f.Required && m[f.Name] == nil {
			return nil, fmt.Errorf("missing required field %q", f.Name)
		}
	}
	return m, nil
}

func ToGeoJSON(v interface{}) (string, error) {
	// {"type":"Point","coordinates":[-73.96481,40.633247]}
	if v == nil {
		return "", nil
	}
	b, err := json.Marshal(v)
	return string(b), err
}
