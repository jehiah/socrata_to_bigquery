package main

import (
	"encoding/json"
	"fmt"
	"testing"
)

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
		json.Unmarshal([]byte(s), &m)
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
