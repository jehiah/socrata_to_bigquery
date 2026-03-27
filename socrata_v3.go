package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// SocrataMetadata holds dataset metadata from the Socrata API
type SocrataMetadata struct {
	ID            string          `json:"id"`
	Name          string          `json:"name"`
	Columns       []SocrataColumn `json:"columns"`
	RowsUpdatedAt int64           `json:"rowsUpdatedAt"` // Unix timestamp
}

func (m SocrataMetadata) RowsUpdatedAtTime() time.Time {
	return time.Unix(m.RowsUpdatedAt, 0)
}

// SocrataColumn holds column metadata from the Socrata API
type SocrataColumn struct {
	ID           int    `json:"id"`
	FieldName    string `json:"fieldName"`
	Name         string `json:"name"`
	DataTypeName string `json:"dataTypeName"`
}

type v3QueryBody struct {
	SQL      string `json:"query"`
	Ordering string `json:"orderingSpecifier,omitempty"`
}

func setSocrataToken(req *http.Request, token string) {
	if token != "" {
		req.Header.Set("X-App-Token", token)
	}
}

func v3Endpoint(apiBase *url.URL, datasetID string) string {
	u := *apiBase
	u.Path = fmt.Sprintf("/api/v3/views/%s/query.json", url.PathEscape(datasetID))
	return u.String()
}

func v3Post(ctx context.Context, apiBase *url.URL, datasetID, sql, token string) (*http.Response, error) {
	body, err := json.Marshal(v3QueryBody{SQL: sql, Ordering: "discard"})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v3Endpoint(apiBase, datasetID), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	setSocrataToken(req, token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		return nil, fmt.Errorf("http status %d %s: %s", resp.StatusCode, v3Endpoint(apiBase, datasetID), errBody)
	}
	return resp, nil
}

// FetchMetadata retrieves dataset metadata from the Socrata API.
func FetchMetadata(ctx context.Context, apiBase *url.URL, datasetID, token string) (*SocrataMetadata, error) {
	u := *apiBase
	u.Path = fmt.Sprintf("/api/views/%s.json", url.PathEscape(datasetID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	setSocrataToken(req, token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("http status %d: %s", resp.StatusCode, body)
	}
	var md SocrataMetadata
	if err := json.NewDecoder(resp.Body).Decode(&md); err != nil {
		return nil, err
	}
	return &md, nil
}

// CountV3 returns the number of records matching the optional WHERE clause via the v3 API.
func CountV3(ctx context.Context, apiBase *url.URL, datasetID, where, token string) (int64, error) {
	sql := "SELECT COUNT(*) AS count"
	if where != "" {
		sql += " WHERE " + where
	}
	resp, err := v3Post(ctx, apiBase, datasetID, sql, token)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	var results []struct {
		Count string `json:"count"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return 0, err
	}
	if len(results) == 0 {
		return 0, nil
	}
	return strconv.ParseInt(results[0].Count, 10, 64)
}

// StreamV3 executes a v3 SQL query and calls handle for each row.
// The SQL should include SELECT, optional WHERE, and optional LIMIT clauses.
func StreamV3(ctx context.Context, apiBase *url.URL, datasetID, sql, token string, handle func(Record) error) error {
	resp, err := v3Post(ctx, apiBase, datasetID, sql, token)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	reader := bufio.NewReaderSize(resp.Body, 5*1024*1024) // 5MB buffer

	dec := json.NewDecoder(reader)
	startToken, err := dec.Token()
	if err != nil {
		return err
	}
	if delim, ok := startToken.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("unexpected response start token %v", startToken)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rowCh := make(chan Record, 150000)
	errCh := make(chan error, 1)

	go func() {
		defer close(rowCh)
		var count int64
		for dec.More() {
			count++
			var row Record
			if err := dec.Decode(&row); err != nil {
				errCh <- fmt.Errorf("row %d: %w", count, err)
				return
			}
			select {
			case rowCh <- row:
			case <-ctx.Done():
				return
			}
		}
		log.Printf("streamed %d rows from %s", count, datasetID)

		endToken, err := dec.Token()
		if err != nil {
			errCh <- err
			return
		}
		if delim, ok := endToken.(json.Delim); !ok || delim != ']' {
			errCh <- fmt.Errorf("unexpected response end token %v", endToken)
		}
	}()

	for row := range rowCh {
		if err := handle(row); err != nil {
			cancel()
			for range rowCh {
			}
			return err
		}
	}

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}
