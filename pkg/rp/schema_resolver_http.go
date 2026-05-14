package rp

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type clock interface {
	Sleep(d time.Duration)
}

const httpSchemaRegistryRetries = 5

// HTTPSchemaResolver queries a Confluent-compatible Schema Registry HTTP API
// to obtain the latest schema ID for a topic's value subject.
type HTTPSchemaResolver struct {
	URL    string
	Key    string
	Secret string
	Topic  string
	Clock  clock
}

func (r *HTTPSchemaResolver) Resolve(ctx context.Context) ([4]byte, error) {
	subject := fmt.Sprintf("%s-value", r.Topic)
	schemaURL := fmt.Sprintf("%s/subjects/%s/versions/latest", r.URL, subject)

	client := http.Client{Timeout: 30 * time.Second}
	var lastErr error
	for attempt := range httpSchemaRegistryRetries {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, schemaURL, http.NoBody)
		if err != nil {
			return [4]byte{}, err
		}
		if r.Key != "" {
			req.SetBasicAuth(r.Key, r.Secret)
		}

		resp, err := client.Do(req)
		if err != nil {
			return [4]byte{}, err
		}

		if resp.StatusCode == http.StatusOK {
			defer resp.Body.Close()
			var body struct {
				ID int `json:"id"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				return [4]byte{}, err
			}
			var id [4]byte
			binary.BigEndian.PutUint32(id[:], uint32(body.ID))
			return id, nil
		}
		resp.Body.Close()
		lastErr = fmt.Errorf("schema registry returned status %d", resp.StatusCode)
		if attempt < httpSchemaRegistryRetries-1 && r.Clock != nil {
			r.Clock.Sleep(time.Second * time.Duration(attempt))
		}
	}
	return [4]byte{}, fmt.Errorf("schema registry retries exhausted: %w", lastErr)
}
