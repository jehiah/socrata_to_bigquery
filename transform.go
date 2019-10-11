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
func Transform(w io.Writer, r io.Reader, sourceSchema []soda.Column, targetSchema bigquery.Schema) (int64, error) {
	dec := json.NewDecoder(r)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)

	// read open bracket
	var rows int64
	_, err := dec.Token()
	if err != nil {
		return rows, fmt.Errorf("initial token; rows %d %s", rows, err)
	}
	for dec.More() {
		rows += 1
		var m map[string]interface{}
		err := dec.Decode(&m)
		if err != nil {
			return rows, fmt.Errorf("row %d %s", rows, err)
		}
		mm, err := TransformOne(m, sourceSchema, targetSchema)
		if err != nil {
			log.Printf("%s row:%d data:%#v", err, rows, m)
			continue
		}
		if mm != nil {
			enc.Encode(mm)
		}
	}
	_, err = dec.Token()
	if err != nil {
		return rows, err
	}
	return rows, nil
}

func TransformOne(m map[string]interface{}, sourceSchema []soda.Column, targetSchema bigquery.Schema) (map[string]interface{}, error) {
	var offset int
	for i, f := range targetSchema {
		switch f.Name {
		case "_id", "_created_at", "_updated_at", "_version":
			offset += 1
			// move the field from ":${field}" to "_${field}"
			k := ":" + f.Name[1:]
			m[f.Name] = m[k]
			delete(m, k)
			continue
		}
		sourceDataType := sourceSchema[i-offset].DataTypeName
		switch f.Type {
		case bigquery.DateFieldType:
			switch sourceDataType {
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
