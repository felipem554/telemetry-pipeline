package main

import (
    "context"
    "encoding/json"
    "io"
    "log"
    "net/http"
    "os"
    "strconv"
    "sync"
    "time"

    kafka "github.com/segmentio/kafka-go"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
    "go.opentelemetry.io/otel"
    "go.opentelemetry.io/otel/exporters/jaeger"
    "go.opentelemetry.io/otel/sdk/resource"
    sdktrace "go.opentelemetry.io/otel/sdk/trace"
    semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
    "golang.org/x/time/rate"
)

type Event struct {
    EventID  string `json:"event_id"`
    ClientID string `json:"client_id"`
    Ts       int64  `json:"ts"`
    Type     string `json:"type"`
    Payload  string `json:"payload"`
}

var (
    kafkaWriter *kafka.Writer

    // Prometheus metrics
    eventsAccepted = prometheus.NewCounter(prometheus.CounterOpts{
        Name: "events_accepted_total",
        Help: "Total number of events accepted",
    })
    eventsDropped = prometheus.NewCounter(prometheus.CounterOpts{
        Name: "events_dropped_total",
        Help: "Total number of events dropped due to errors or rate limiting",
    })
    kafkaWriteErrors = prometheus.NewCounter(prometheus.CounterOpts{
        Name: "kafka_write_errors_total",
        Help: "Total number of Kafka write errors",
    })
    httpRequestDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
        Name:    "http_request_duration_seconds",
        Help:    "HTTP request duration in seconds",
        Buckets: prometheus.ExponentialBuckets(0.001, 2, 15), // from 1ms to ~16s
    })

    limiter = rate.NewLimiter(100, 200) // 100 req/s with burst 200, tune as needed

    mu sync.Mutex
)

func initTracer() (*sdktrace.TracerProvider, error) {
    exp, err := jaeger.New(jaeger.WithAgentEndpoint())
    if err != nil {
        return nil, err
    }
    tp := sdktrace.NewTracerProvider(
        sdktrace.WithBatcher(exp),
        sdktrace.WithResource(resource.NewWithAttributes(
            semconv.SchemaURL,
            semconv.ServiceName("ingest-api"),
        )),
    )
    otel.SetTracerProvider(tp)
    return tp, nil
}

func ingestHandler(w http.ResponseWriter, r *http.Request) {
    ctx := r.Context()
    timer := prometheus.NewTimer(httpRequestDuration)
    defer timer.ObserveDuration()

    if !limiter.Allow() {
        eventsDropped.Inc()
        http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
        return
    }

    if r.ContentLength > 5<<20 { // 5MB limit
        eventsDropped.Inc()
        http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
        return
    }

    body, err := io.ReadAll(io.LimitReader(r.Body, 5<<20))
    if err != nil {
        eventsDropped.Inc()
        http.Error(w, "bad request", http.StatusBadRequest)
        return
    }

    var events []Event
    if err := json.Unmarshal(body, &events); err != nil {
        eventsDropped.Inc()
        http.Error(w, "invalid json", http.StatusBadRequest)
        return
    }

    if len(events) == 0 {
        http.Error(w, "empty event batch", http.StatusBadRequest)
        return
    }

    msgs := make([]kafka.Message, 0, len(events))
    for _, e := range events {
        key := []byte(e.ClientID)
        val, err := json.Marshal(e)
        if err != nil {
            // skip invalid event
            continue
        }
        msgs = append(msgs, kafka.Message{Key: key, Value: val})
    }

    ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
    defer cancel()

    if err := kafkaWriter.WriteMessages(ctx, msgs...); err != nil {
        kafkaWriteErrors.Inc()
        log.Printf("kafka write error: %v", err)
        http.Error(w, "service overloaded", http.StatusTooManyRequests)
        return
    }

    eventsAccepted.Add(float64(len(msgs)))

    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusAccepted)
    w.Write([]byte(`{"accepted":` + strconv.Itoa(len(msgs)) + `}`))
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(http.StatusOK)
    w.Write([]byte("ok"))
}

func main() {
    // Init Prometheus metrics
    prometheus.MustRegister(eventsAccepted, eventsDropped, kafkaWriteErrors, httpRequestDuration)

    // Init OpenTelemetry tracer
    tp, err := initTracer()
    if err != nil {
        log.Fatalf("failed to initialize tracer: %v", err)
    }
    defer func() {
        ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
        defer cancel()
        if err := tp.Shutdown(ctx); err != nil {
            log.Printf("error shutting down tracer provider: %v", err)
        }
    }()

    kafkaBrokers := []string{"kafka:9092"}
    if envBrokers := os.Getenv("KAFKA_BROKERS"); envBrokers != "" {
        kafkaBrokers = []string{envBrokers}
    }

    kafkaWriter = &kafka.Writer{
        Addr:         kafka.TCP(kafkaBrokers...),
        Topic:        "telemetry-events",
        Balancer:     &kafka.Hash{},
        RequiredAcks: kafka.RequireAll,
        Async:        true,
        Compression:  kafka.Snappy,
        BatchSize:    1000,
        BatchTimeout: 50 * time.Millisecond,
    }

    mux := http.NewServeMux()
    mux.Handle("/metrics", promhttp.Handler())
    mux.Handle("/v1/events", otelhttp.NewHandler(http.HandlerFunc(ingestHandler), "IngestEvents"))
    mux.HandleFunc("/health", healthHandler)

    srv := &http.Server{
        Addr:    ":8080",
        Handler: mux,
    }

    log.Println("starting ingest api on :8080")
    log.Fatal(srv.ListenAndServe())
}
