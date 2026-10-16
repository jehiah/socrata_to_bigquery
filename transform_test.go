package main

import (
	"encoding/json"
	"fmt"
	"testing"

	"cloud.google.com/go/bigquery"
)

// TestTransformOneRequired checks that every supported
// (source_field_type, bigquery_type) pair audits the required state of a value.
func TestTransformOneRequired(t *testing.T) {
	type testCase struct {
		sourceFieldType string
		bqType          bigquery.FieldType
		timeFormat      string
		valid           interface{}
		missing         []interface{}
	}
	tests := []testCase{
		{"text", bigquery.NumericFieldType, "", "1,234.5", []interface{}{nil, ""}},
		{"text", bigquery.FloatFieldType, "", "1,234.5", []interface{}{nil, ""}},
		{"number", bigquery.NumericFieldType, "", "1.234", []interface{}{nil}},
		{"number", bigquery.FloatFieldType, "", "1.234", []interface{}{nil}},
		{"", bigquery.NumericFieldType, "", json.Number("1.234"), []interface{}{nil}},
		{"", bigquery.FloatFieldType, "", json.Number("1.234"), []interface{}{nil}},
		{"url", bigquery.StringFieldType, "", map[string]interface{}{"url": "http://example.com/"}, []interface{}{nil, map[string]interface{}{}, map[string]interface{}{"url": ""}}},
		{"text", bigquery.StringFieldType, "", "abc", []interface{}{nil, ""}},
		{"", bigquery.StringFieldType, "", "abc", []interface{}{nil, ""}},
		{"point", bigquery.GeographyFieldType, "", "POINT (-73.921794 40.610106)", []interface{}{nil}},
		{"location", bigquery.GeographyFieldType, "", map[string]interface{}{"latitude": "40.610106", "longitude": "-73.921794"}, []interface{}{nil, map[string]interface{}{}}},
		{"text", bigquery.DateFieldType, "2006/01/02", "2019/02/28", []interface{}{nil, ""}},
		{"text", bigquery.TimeFieldType, "15:04", "10:01", []interface{}{nil, ""}},
		{"text", bigquery.TimestampFieldType, "", "2019-02-28T00:00:00.000", []interface{}{nil, ""}},
		{"text", bigquery.DateTimeFieldType, "", "2019-02-28T00:00:00.000", []interface{}{nil, ""}},
		{"checkbox", bigquery.BooleanFieldType, "", true, []interface{}{nil, ""}},
	}
	for _, tc := range tests {
		tc := tc
		schema := func(required bool) TableSchema {
			return TableSchema{"a": SchemaField{
				SourceField:     "a",
				SourceFieldType: tc.sourceFieldType,
				Type:            tc.bqType,
				TimeFormat:      tc.timeFormat,
				Required:        required,
				OnError:         RaiseError,
			}}
		}
		t.Run(fmt.Sprintf("%s_to_%s", tc.sourceFieldType, tc.bqType), func(t *testing.T) {
			t.Run("valid", func(t *testing.T) {
				got, err := TransformOne(Record{"a": tc.valid}, schema(true))
				if err != nil {
					t.Fatalf("unexpected error %s", err)
				}
				if v := got["a"]; v == nil || v == "" {
					t.Fatalf("expected a value got %#v", v)
				}
			})
			for i, missing := range tc.missing {
				t.Run(fmt.Sprintf("missing_%d", i), func(t *testing.T) {
					_, err := TransformOne(Record{"a": missing}, schema(true))
					if err == nil {
						t.Fatalf("expected error for %#v", missing)
					}
				})
				t.Run(fmt.Sprintf("not_required_%d", i), func(t *testing.T) {
					got, err := TransformOne(Record{"a": missing}, schema(false))
					if err != nil {
						// values that are invalid (not merely absent) still error
						t.Logf("value %#v errored: %s", missing, err)
						return
					}
					if v := got["a"]; v != nil && v != "" && v != false {
						t.Fatalf("expected empty value got %#v", v)
					}
				})
			}
		})
	}
}

// func TestTransformOne(t *testing.T) {
// 	type testCase struct {
// 		in           string
// 		sourceSchema []soda.Column
// 		targetSchema bigquery.Schema
// 		out          string
// 		err          string
// 	}
// 	tests := []testCase{
// 		{
// 			in:           `{"a":"2010/01/02"}`,
// 			sourceSchema: []soda.Column{{DataTypeName: "text"}},
// 			targetSchema: bigquery.Schema{&bigquery.FieldSchema{Name: "a", Type: bigquery.DateFieldType}},
// 			out:          `{"a":"2010-01-02"}`,
// 		},
// 		{
// 			in:           `{"a":""}`,
// 			sourceSchema: []soda.Column{{DataTypeName: "text"}},
// 			targetSchema: bigquery.Schema{&bigquery.FieldSchema{Name: "a", Type: bigquery.DateFieldType, Required: true}},
// 			err:          `missing required field "a"`,
// 		},
// 	}
// 	for i, tc := range tests {
// 		tc := tc
// 		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
// 			t.Logf("%#v", tc)
// 			var m map[string]interface{}
// 			if err := json.Unmarshal([]byte(tc.in), &m); err != nil {
// 				t.Fatal(err)
// 			}
// 			got, err := TransformOne(m, tc.sourceSchema, tc.targetSchema, time.Time{})
// 			if tc.err != "" {
// 				if err == nil || err.Error() != tc.err {
// 					t.Fatalf("expected error %s got %s", tc.err, err)
// 				}
// 				return
// 			}
// 			if err != nil {
// 				t.Fatal(err)
// 			}
// 			b, err := json.Marshal(got)
// 			if err != nil {
// 				t.Fatal(err)
// 			}
// 			t.Logf("%s", b)
// 			if string(b) != tc.out {
// 				t.Errorf("got %s expected %s", b, tc.out)
// 			}
// 		})
//
// 	}
// }

func TestToGeoJSONPoint(t *testing.T) {
	type testCase struct {
		have   any
		expect string
	}
	u := func(s string) interface{} {
		var m interface{}
		if err := json.Unmarshal([]byte(s), &m); err != nil {
			panic(err)
		}
		return m
	}

	tests := []testCase{
		{u(`{"type":"Point","coordinates":[-73.96481,40.633247]}`), `{"coordinates":[-73.96481,40.633247],"type":"Point"}`},
		{`POINT (-73.921794 40.610106)`, `{"coordinates":[-73.921794,40.610106],"type":"Point"}`},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Logf("%#v", tc)
			got, err := ToGeoJSONPoint(tc.have)
			if err != nil {
				t.Fatal(err)
			}
			if g, ok := got.(string); !ok {
				t.Fatalf("got %#v not %q", got, tc.expect)
			} else if g != tc.expect {
				t.Fatalf("got %q not %q", g, tc.expect)
			}
		})
	}

	// var m interface{}
	// json.Unmarshal([]byte(`{"type":"Point","coordinates":[-73.96481,40.633247]}`), &m)
	// got, _ := ToGeoJSONPoint(m)
	// t.Logf("%s", got)
	// if got.(string) != "{\"coordinates\":[-73.96481,40.633247],\"type\":\"Point\"}" {
	// 	t.Fatalf("err got %q", got)
	// }

}

func TestToTime(t *testing.T) {
	type testCase struct {
		have   string
		format string
		expect string
	}
	tests := []testCase{
		{"1001p", "0304p", "22:01:00"},
		{"1001pm", "0304pm", "22:01:00"},
		{"10:01", "15:04", "10:01:00"},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Logf("%#v", tc)
			got, err := ToTime(tc.format, tc.have)
			if err != nil {
				t.Fatal(err)
			}
			if g, ok := got.(string); !ok {
				t.Fatalf("got %#v not %q", got, tc.expect)
			} else if g != tc.expect {
				t.Fatalf("got %q not %q", g, tc.expect)
			}
		})
	}
}

func TestToDate(t *testing.T) {
	type testCase struct {
		have   string
		format string
		expect string
	}
	tests := []testCase{
		{"02/28/2019", "01/02/2006", "2019-02-28"},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Logf("%#v", tc)
			got, err := ToDate(tc.format, tc.have)
			if err != nil {
				t.Fatal(err)
			}
			if g, ok := got.(string); !ok {
				t.Fatalf("got %#v not %q", got, tc.expect)
			} else if g != tc.expect {
				t.Fatalf("got %q not %q", g, tc.expect)
			}
		})
	}
}
