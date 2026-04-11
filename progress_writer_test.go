package main

import (
	"bytes"
	"testing"
	"time"
)

func TestProgressWriter_Total(t *testing.T) {
	var buf bytes.Buffer
	pw := NewProgressWriter(&buf, time.Hour) // long interval so no log fires
	defer pw.Stop()
	data := []byte("hello world")
	n, err := pw.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(data) {
		t.Fatalf("expected %d, got %d", len(data), n)
	}
	if pw.Total() != int64(len(data)) {
		t.Fatalf("expected total %d, got %d", len(data), pw.Total())
	}
	if buf.String() != "hello world" {
		t.Fatalf("unexpected inner content %q", buf.String())
	}
}

func TestProgressWriter_MultipleWrites(t *testing.T) {
	var buf bytes.Buffer
	pw := NewProgressWriter(&buf, time.Hour)
	defer pw.Stop()
	_, _ = pw.Write([]byte("aaa"))
	_, _ = pw.Write([]byte("bbbbb"))
	if pw.Total() != 8 {
		t.Fatalf("expected 8, got %d", pw.Total())
	}
}

func TestHumanBytes(t *testing.T) {
	tests := []struct {
		input    int64
		expected string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{123456, "120.56 KB"},
		{1048576, "1.00 MB"},
		{1073741824, "1.00 GB"},
		{1536, "1.50 KB"},
	}
	for _, tt := range tests {
		got := humanBytes(tt.input)
		if got != tt.expected {
			t.Errorf("humanBytes(%d) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}
