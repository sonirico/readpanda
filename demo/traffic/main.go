package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/kgo"
)

const orderSchemaJSON = `{
	"type": "record",
	"name": "Order",
	"fields": [
		{"name": "order_id", "type": "string"},
		{"name": "customer_id", "type": "string"},
		{"name": "amount_cents", "type": "long"},
		{"name": "currency", "type": "string"},
		{"name": "status", "type": "string"},
		{"name": "created_at", "type": "long"}
	]
}`

const orderSubject = "shop.orders.v1-value"

const demoSourceHeader = "readpanda-demo"

// Order mirrors orderSchemaJSON for hamba/avro encoding.
type Order struct {
	OrderID     string `avro:"order_id"`
	CustomerID  string `avro:"customer_id"`
	AmountCents int64  `avro:"amount_cents"`
	Currency    string `avro:"currency"`
	Status      string `avro:"status"`
	CreatedAt   int64  `avro:"created_at"`
}

func main() {
	brokers := flag.String("brokers", "localhost:19092", "comma-separated Kafka brokers")
	srURL := flag.String("sr", "http://localhost:18081", "Schema Registry URL")
	flag.Parse()

	if err := run(*brokers, *srURL); err != nil {
		fmt.Fprintln(os.Stderr, "traffic:", err)
		os.Exit(1)
	}
}

func run(brokers, srURL string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	schemaID, err := registerOrderSchema(srURL)
	if err != nil {
		return fmt.Errorf("register order schema: %w", err)
	}

	orderSchema, err := avro.Parse(orderSchemaJSON)
	if err != nil {
		return fmt.Errorf("parse order schema: %w", err)
	}

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers))
	if err != nil {
		return fmt.Errorf("create producer client: %w", err)
	}
	defer producer.Close()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers),
		kgo.ConsumerGroup("demo-analytics"),
		kgo.ConsumeTopics("shop.orders.v1"),
	)
	if err != nil {
		return fmt.Errorf("create consumer client: %w", err)
	}
	defer consumer.Close()

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		produceOrders(ctx, producer, orderSchema, schemaID)
	}()
	go func() {
		defer wg.Done()
		produceJSONTopics(ctx, producer)
	}()
	go func() {
		defer wg.Done()
		laggingConsume(ctx, consumer)
	}()
	wg.Wait()

	return nil
}

// registerOrderSchema registers orderSchemaJSON under orderSubject and
// returns the schema ID assigned by the Schema Registry.
func registerOrderSchema(srURL string) (int, error) {
	body, err := json.Marshal(map[string]string{"schema": orderSchemaJSON})
	if err != nil {
		return 0, fmt.Errorf("marshal register request: %w", err)
	}

	url := fmt.Sprintf("%s/subjects/%s/versions", srURL, orderSubject)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("build register request: %w", err)
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("send register request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("read register response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("register schema: status %d: %s", resp.StatusCode, respBody)
	}

	var parsed struct {
		ID int `json:"id"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return 0, fmt.Errorf("unmarshal register response: %w", err)
	}
	return parsed.ID, nil
}

// produceOrders publishes one Avro-encoded order every 300ms.
func produceOrders(ctx context.Context, client *kgo.Client, schema avro.Schema, schemaID int) {
	statuses := []string{"pending", "paid", "shipped", "cancelled"}
	currencies := []string{"USD", "EUR", "GBP"}

	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	var n int
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n++
			order := Order{
				OrderID:     fmt.Sprintf("ord-%d", n),
				CustomerID:  fmt.Sprintf("cust-%d", n%50),
				AmountCents: int64(500 + rand.Intn(20000)),
				Currency:    currencies[n%len(currencies)],
				Status:      statuses[n%len(statuses)],
				CreatedAt:   time.Now().UnixMilli(),
			}

			value, err := encodeAvroWireFormat(schema, schemaID, order)
			if err != nil {
				log.Printf("encode order: %v", err)
				continue
			}

			record := &kgo.Record{
				Topic: "shop.orders.v1",
				Key:   []byte(order.OrderID),
				Value: value,
				Headers: []kgo.RecordHeader{
					{Key: "source", Value: []byte(demoSourceHeader)},
				},
			}
			client.Produce(ctx, record, func(_ *kgo.Record, err error) {
				if err != nil {
					log.Printf("produce order: %v", err)
				}
			})
		}
	}
}

// encodeAvroWireFormat wraps an Avro-encoded record in the Confluent wire
// format: magic byte 0x00, big-endian schema ID, then the Avro payload.
func encodeAvroWireFormat(schema avro.Schema, schemaID int, v any) ([]byte, error) {
	payload, err := avro.Marshal(schema, v)
	if err != nil {
		return nil, fmt.Errorf("avro marshal: %w", err)
	}

	buf := make([]byte, 5+len(payload))
	buf[0] = 0x00
	binary.BigEndian.PutUint32(buf[1:5], uint32(schemaID))
	copy(buf[5:], payload)
	return buf, nil
}

// produceJSONTopics publishes JSON payloads to every non-Avro demo topic on
// its own ticker cadence.
func produceJSONTopics(ctx context.Context, client *kgo.Client) {
	temperature := time.NewTicker(200 * time.Millisecond)
	defer temperature.Stop()
	humidity := time.NewTicker(200 * time.Millisecond)
	defer humidity.Stop()
	gps := time.NewTicker(250 * time.Millisecond)
	defer gps.Stop()
	payments := time.NewTicker(500 * time.Millisecond)
	defer payments.Stop()
	customers := time.NewTicker(2 * time.Second)
	defer customers.Stop()
	accessLogs := time.NewTicker(300 * time.Millisecond)
	defer accessLogs.Stop()
	errorLogs := time.NewTicker(5 * time.Second)
	defer errorLogs.Stop()
	dlq := time.NewTicker(10 * time.Second)
	defer dlq.Stop()

	var n int
	for {
		select {
		case <-ctx.Done():
			return
		case <-temperature.C:
			n++
			device := fmt.Sprintf("dev-%d", n%8)
			produceJSON(ctx, client, "iot.sensors.temperature", device, map[string]any{
				"device_id": device,
				"value":     18 + rand.Float64()*10,
				"unit":      "celsius",
				"ts":        time.Now().UnixMilli(),
			})
		case <-humidity.C:
			n++
			device := fmt.Sprintf("dev-%d", n%8)
			produceJSON(ctx, client, "iot.sensors.humidity", device, map[string]any{
				"device_id": device,
				"value":     30 + rand.Float64()*40,
				"unit":      "percent",
				"ts":        time.Now().UnixMilli(),
			})
		case <-gps.C:
			n++
			vehicle := fmt.Sprintf("veh-%d", n%5)
			produceJSON(ctx, client, "iot.fleet.gps", vehicle, map[string]any{
				"vehicle_id": vehicle,
				"lat":        40 + rand.Float64(),
				"lon":        -3 + rand.Float64(),
				"speed_kmh":  rand.Intn(120),
				"ts":         time.Now().UnixMilli(),
			})
		case <-payments.C:
			n++
			payment := fmt.Sprintf("pay-%d", n)
			produceJSON(ctx, client, "shop.payments.v1", payment, map[string]any{
				"payment_id":   payment,
				"order_id":     fmt.Sprintf("ord-%d", n),
				"amount_cents": 500 + rand.Intn(20000),
				"method":       []string{"card", "paypal", "wire"}[n%3],
				"status":       []string{"authorized", "captured", "failed"}[n%3],
			})
		case <-customers.C:
			n++
			customer := fmt.Sprintf("cust-%d", n%50)
			produceJSON(ctx, client, "shop.customers.v1", customer, map[string]any{
				"customer_id": customer,
				"name":        fmt.Sprintf("Customer %d", n%50),
				"email":       fmt.Sprintf("customer%d@example.com", n%50),
				"plan":        []string{"free", "pro", "enterprise"}[n%3],
			})
		case <-accessLogs.C:
			n++
			request := fmt.Sprintf("req-%d", n)
			produceJSON(ctx, client, "logs.api.access", request, map[string]any{
				"request_id": request,
				"method":     []string{"GET", "POST", "PUT", "DELETE"}[n%4],
				"path":       []string{"/orders", "/payments", "/customers"}[n%3],
				"status":     []int{200, 201, 404, 500}[n%4],
				"latency_ms": rand.Intn(500),
				"ts":         time.Now().UnixMilli(),
			})
		case <-errorLogs.C:
			n++
			request := fmt.Sprintf("req-%d", n)
			produceJSON(ctx, client, "logs.api.error", request, map[string]any{
				"request_id": request,
				"message":    "internal server error",
				"ts":         time.Now().UnixMilli(),
			})
		case <-dlq.C:
			n++
			order := fmt.Sprintf("ord-%d", n)
			produceJSON(ctx, client, "shop.orders.dlq", order, map[string]any{
				"order_id": order,
				"reason":   "schema validation failed",
				"ts":       time.Now().UnixMilli(),
			})
		}
	}
}

// produceJSON marshals payload and publishes it to topic with key and the
// demo source header.
func produceJSON(ctx context.Context, client *kgo.Client, topic, key string, payload map[string]any) {
	value, err := json.Marshal(payload)
	if err != nil {
		log.Printf("marshal %s payload: %v", topic, err)
		return
	}

	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
		Headers: []kgo.RecordHeader{
			{Key: "source", Value: []byte(demoSourceHeader)},
		},
	}
	client.Produce(ctx, record, func(_ *kgo.Record, err error) {
		if err != nil {
			log.Printf("produce %s: %v", topic, err)
		}
	})
}

// laggingConsume polls shop.orders.v1 as group demo-analytics but sleeps
// 2s per poll iteration so consumer lag accumulates.
func laggingConsume(ctx context.Context, client *kgo.Client) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		fetches := client.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			log.Printf("fetch error topic=%s partition=%d: %v", topic, partition, err)
		})
		fetches.EachRecord(func(*kgo.Record) {})

		select {
		case <-ctx.Done():
			return
		case <-time.After(2 * time.Second):
		}
	}
}
