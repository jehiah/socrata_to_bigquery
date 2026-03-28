package main

import (
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
)

func TestTimePartitioning_ValidTimestamp(t *testing.T) {
	ts := TableSchema{
		"_updated_at": {
			SourceField:   ":updated_at",
			Type:          bigquery.TimestampFieldType,
			Required:      true,
			TimePartition: TimePartitionDay,
		},
	}

	tp, err := ts.TimePartitioning()
	if err != nil {
		t.Fatal(err)
	}
	if tp == nil {
		t.Fatal("expected partitioning config")
	}
	if tp.Field != "_updated_at" {
		t.Fatalf("unexpected field %q", tp.Field)
	}
	if tp.Type != bigquery.DayPartitioningType {
		t.Fatalf("unexpected type %q", tp.Type)
	}
}

func TestTimePartitioning_ValidDate(t *testing.T) {
	ts := TableSchema{
		"issue_date": {
			SourceField:   "issue_date",
			Type:          bigquery.DateFieldType,
			Required:      true,
			TimePartition: TimePartitionMonth,
		},
	}

	tp, err := ts.TimePartitioning()
	if err != nil {
		t.Fatal(err)
	}
	if tp == nil {
		t.Fatal("expected partitioning config")
	}
	if tp.Field != "issue_date" {
		t.Fatalf("unexpected field %q", tp.Field)
	}
	if tp.Type != bigquery.MonthPartitioningType {
		t.Fatalf("unexpected type %q", tp.Type)
	}
}

func TestTimePartitioning_NoneConfigured(t *testing.T) {
	ts := TableSchema{
		"_created_at": {
			SourceField: ":created_at",
			Type:        bigquery.TimestampFieldType,
			Required:    true,
		},
	}

	tp, err := ts.TimePartitioning()
	if err != nil {
		t.Fatal(err)
	}
	if tp != nil {
		t.Fatalf("expected nil partitioning, got %#v", tp)
	}
}

func TestTimePartitioning_Invalid(t *testing.T) {
	tests := []struct {
		name    string
		schema  TableSchema
		errLike string
	}{
		{
			name: "not required",
			schema: TableSchema{
				"_updated_at": {
					SourceField:   ":updated_at",
					Type:          bigquery.TimestampFieldType,
					Required:      false,
					TimePartition: TimePartitionDay,
				},
			},
			errLike: "must be required",
		},
		{
			name: "wrong field type",
			schema: TableSchema{
				"name": {
					SourceField:   "name",
					Type:          bigquery.StringFieldType,
					Required:      true,
					TimePartition: TimePartitionDay,
				},
			},
			errLike: "must be DATE or TIMESTAMP",
		},
		{
			name: "invalid partition type",
			schema: TableSchema{
				"_updated_at": {
					SourceField:   ":updated_at",
					Type:          bigquery.TimestampFieldType,
					Required:      true,
					TimePartition: TimePartition("WEEK"),
				},
			},
			errLike: "must be one of HOUR, DAY, MONTH, YEAR",
		},
		{
			name: "multiple partition fields",
			schema: TableSchema{
				"_updated_at": {
					SourceField:   ":updated_at",
					Type:          bigquery.TimestampFieldType,
					Required:      true,
					TimePartition: TimePartitionDay,
				},
				"_created_at": {
					SourceField:   ":created_at",
					Type:          bigquery.TimestampFieldType,
					Required:      true,
					TimePartition: TimePartitionHour,
				},
			},
			errLike: "multiple schema fields",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.schema.TimePartitioning()
			if err == nil {
				t.Fatalf("expected error containing %q", tc.errLike)
			}
			if !strings.Contains(err.Error(), tc.errLike) {
				t.Fatalf("got %q, expected it to contain %q", err.Error(), tc.errLike)
			}
		})
	}
}
