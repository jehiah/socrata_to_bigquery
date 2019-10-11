package main

import (
	"encoding/json"
	"fmt"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/SebastiaanKlippert/go-soda"
)

func TestTransform(t *testing.T) {
	type testCase struct {
		in           string
		sourceSchema []soda.Column
		targetSchema bigquery.Schema
		out          string
		err          string
	}
	tests := []testCase{
		{
			in:           `{"a":"2010/01/02"}`,
			sourceSchema: []soda.Column{{DataTypeName: "text"}},
			targetSchema: bigquery.Schema{&bigquery.FieldSchema{Name: "a", Type: bigquery.DateFieldType}},
			out:          `{"a":"2010-01-02"}`,
		},
		{
			in:           `{"a":""}`,
			sourceSchema: []soda.Column{{DataTypeName: "text"}},
			targetSchema: bigquery.Schema{&bigquery.FieldSchema{Name: "a", Type: bigquery.DateFieldType, Required: true}},
			err:          `missing required field "a"`,
		},
	}
	for i, tc := range tests {
		tc := tc
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Logf("%#v", tc)
			var m map[string]interface{}
			if err := json.Unmarshal([]byte(tc.in), &m); err != nil {
				t.Fatal(err)
			}
			got, err := TransformOne(m, tc.sourceSchema, tc.targetSchema)
			if tc.err != "" {
				if err == nil || err.Error() != tc.err {
					t.Fatalf("expected error %s got %s", tc.err, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			b, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("%s", b)
			if string(b) != tc.out {
				t.Errorf("got %s expected %s", b, tc.out)
			}
		})

	}
}
