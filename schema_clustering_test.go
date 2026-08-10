package main

import (
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
)

func TestClustering_Valid(t *testing.T) {
	ts := TableSchema{
		"status": {
			SourceField: "status",
			Type:        bigquery.StringFieldType,
			Cluster:     2,
		},
		"borough": {
			SourceField: "borough",
			Type:        bigquery.StringFieldType,
			Cluster:     1,
		},
	}

	c, err := ts.Clustering()
	if err != nil {
		t.Fatal(err)
	}
	if c == nil {
		t.Fatal("expected clustering config")
	}
	want := []string{"borough", "status"}
	if len(c.Fields) != len(want) {
		t.Fatalf("unexpected fields %#v", c.Fields)
	}
	for i, f := range want {
		if c.Fields[i] != f {
			t.Fatalf("unexpected field order %#v", c.Fields)
		}
	}
}

func TestClustering_NoneConfigured(t *testing.T) {
	ts := TableSchema{
		"status": {
			SourceField: "status",
			Type:        bigquery.StringFieldType,
		},
	}

	c, err := ts.Clustering()
	if err != nil {
		t.Fatal(err)
	}
	if c != nil {
		t.Fatalf("expected nil clustering, got %#v", c)
	}
}

func TestClustering_Invalid(t *testing.T) {
	tests := []struct {
		name    string
		schema  TableSchema
		errLike string
	}{
		{
			name: "cluster order out of range",
			schema: TableSchema{
				"status": {
					SourceField: "status",
					Type:        bigquery.StringFieldType,
					Cluster:     5,
				},
			},
			errLike: "must have cluster between 1 and 4",
		},
		{
			name: "duplicate cluster order",
			schema: TableSchema{
				"status": {
					SourceField: "status",
					Type:        bigquery.StringFieldType,
					Cluster:     1,
				},
				"borough": {
					SourceField: "borough",
					Type:        bigquery.StringFieldType,
					Cluster:     1,
				},
			},
			errLike: "multiple schema fields have cluster",
		},
		{
			name: "unsupported field type",
			schema: TableSchema{
				"amount": {
					SourceField: "amount",
					Type:        bigquery.FloatFieldType,
					Cluster:     1,
				},
			},
			errLike: "unsupported bigquery_type",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.schema.Clustering()
			if err == nil {
				t.Fatalf("expected error containing %q", tc.errLike)
			}
			if !strings.Contains(err.Error(), tc.errLike) {
				t.Fatalf("got %q, expected it to contain %q", err.Error(), tc.errLike)
			}
		})
	}
}
