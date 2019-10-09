package main

import (
	"bytes"
	"fmt"
	"strings"
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
	}
	tests := []testCase{
		{
			in:           `[{"a":"2010/01/02"}]`,
			sourceSchema: []soda.Column{{DataTypeName: "text"}},
			targetSchema: bigquery.Schema{&bigquery.FieldSchema{Name: "a", Type: bigquery.DateFieldType}},
			out:          `{"a":"2010-01-02"}`,
		},
	}
	for i, tc := range tests {
		tc := tc
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			t.Logf("%#v", tc)
			var b bytes.Buffer
			rows, err := Transform(&b, strings.NewReader(tc.in), tc.sourceSchema, tc.targetSchema)
			if err != nil {
				t.Fatal(err)
			}
			if strings.TrimSpace(b.String()) != tc.out {
				t.Errorf("got %s expected %s", b.String(), tc.out)
			}
			t.Logf("%d", rows)
		})

	}
}
