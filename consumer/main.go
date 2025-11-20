package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"

    kafka "github.com/segmentio/kafka-go"
)

func main() {
    kafkaBrokers := []string{"kafka:9092"}
    topic := "telemetry-events"
    groupID := "telemetry-consumer-group"

    r := kafka.NewReader(kafka.ReaderConfig{
        Brokers:  kafkaBrokers,
        GroupID:  groupID,
        Topic:    topic,
        MinBytes: 10e3,  // 10KB
        MaxBytes: 10e6,  // 10MB
    })
    defer r.Close()

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    sigchan := make(chan os.Signal, 1)
    signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

    go func() {
        <-sigchan
        log.Println("signal received, shutting down")
        cancel()
    }()

    batch := make([]kafka.Message, 0, 1000)
    batchTimeout := time.NewTimer(100 * time.Millisecond)

    for {
        select {
        case <-ctx.Done():
            return
        default:
            m, err := r.FetchMessage(ctx)
            if err != nil {
                if err == context.Canceled {
                    return
                }
                log.Printf("error fetching message: %v", err)
                continue
            }
            batch = append(batch, m)

            if len(batch) >= 1000 {
                processBatch(ctx, r, batch)
                batch = batch[:0]
                batchTimeout.Reset(100 * time.Millisecond)
            }

            select {
            case <-batchTimeout.C:
                if len(batch) > 0 {
                    processBatch(ctx, r, batch)
                    batch = batch[:0]
                }
                batchTimeout.Reset(100 * time.Millisecond)
            default:
            }
        }
    }
}

func processBatch(ctx context.Context, r *kafka.Reader, batch []kafka.Message) {
    // TODO: enrich, persist to DB, emit metrics
    log.Printf("processing batch of %d messages", len(batch))

    // Commit offsets after successful processing
    lastMsg := batch[len(batch)-1]
    if err := r.CommitMessages(ctx, lastMsg); err != nil {
        log.Printf("failed to commit offsets: %v", err)
    }
}
