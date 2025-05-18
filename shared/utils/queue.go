package utils

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ls1intum/hades/shared/payload"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type HadesProducer struct {
	natsConnection *nats.Conn
	js             jetstream.JetStream
}

type HadesConsumer struct {
	natsConnection *nats.Conn
	consumer       jetstream.Consumer
}

// SetupNatsConnection creates a connection to NATS server
func SetupNatsConnection(config NatsConfig) (*nats.Conn, error) {
	opts := []nats.Option{
		nats.Name("HadesAPI"),
		nats.Timeout(10 * time.Second),
		nats.ReconnectWait(5 * time.Second),
		nats.MaxReconnects(10),
	}

	// Add credentials if provided
	if config.Username != "" && config.Password != "" {
		opts = append(opts, nats.UserInfo(config.Username, config.Password))
	}

	// Add TLS if enabled
	if config.TLS {
		opts = append(opts, nats.Secure(&tls.Config{}))
	}

	// Connect to NATS
	nc, err := nats.Connect(config.URL, opts...)
	if err != nil {
		slog.Error("Failed to connect to NATS", "error", err)
		return nil, err
	}

	slog.Info("Connected to NATS server", "url", config.URL)
	return nc, nil
}

// SetupNatsJetStream creates a JetStream connection for persistent message delivery
func NewHadesProducer(nc *nats.Conn) (HadesProducer, error) {
	ctx := context.Background()
	js, err := jetstream.New(nc)
	if err != nil {
		slog.Error("Failed to create JetStream context", "error", err)
		return HadesProducer{}, err
	}

	s, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      "HADES_JOBS",
		Subjects:  []string{"hades.jobs.*"},
		Storage:   jetstream.FileStorage,
		Retention: jetstream.WorkQueuePolicy,
		MaxMsgs:   -1,
		MaxAge:    24 * time.Hour, // Retain jobs for 24 hours by default
	})
	if err != nil {
		slog.Error("Failed to create JetStream stream", "error", err)
		return HadesProducer{}, err
	}
	slog.Info("Created JetStream stream", "stream", s)

	return HadesProducer{
		natsConnection: nc,
		js:             js,
	}, nil
}

func NewHadesConsumer(nc *nats.Conn) (HadesConsumer, error) {
	ctx := context.Background()
	js, err := jetstream.New(nc)
	if err != nil {
		slog.Error("Failed to create JetStream context", "error", err)
		return HadesConsumer{}, err
	}
	cons, err := js.CreateConsumer(ctx, "HADES_JOBS", jetstream.ConsumerConfig{
		Durable:   "foo",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		slog.Error("Failed to create JetStream consumer", "error", err)
		return HadesConsumer{}, err
	}
	slog.Info("Created JetStream consumer", "consumer", cons)
	return HadesConsumer{
		natsConnection: nc,
		consumer:       cons,
	}, nil
}

func (hp HadesProducer) EnqueueJob(ctx context.Context, queuePayloud payload.QueuePayload) error {
	bytesPayload, err := json.Marshal(queuePayloud)
	if err != nil {
		slog.Error("Failed to marshal payload", "error", err)
	}
	_, err = hp.js.PublishAsync("hades.jobs", bytesPayload)
	return err
}

func (hc HadesConsumer) DequeueJob(ctx context.Context, processing func(payload payload.QueuePayload)) {
	// Get message iterator with max 1 message at a time - good for work queues
	iter, err := hc.consumer.Messages(jetstream.PullMaxMessages(1))
	if err != nil {
		slog.Error("Failed to create message iterator", "error", err)
		return
	}
	defer iter.Stop()

	// Create a worker pool with limited concurrency
	numWorkers := 1 // TODO make configurable
	sem := make(chan struct{}, numWorkers)

	// Create a wait group to track active workers
	var wg sync.WaitGroup

	// Process messages until context is cancelled
	for {
		select {
		case <-ctx.Done():
			slog.Info("Context cancelled, stopping message consumption")
			wg.Wait() // Wait for in-progress workers to complete
			return
		default:
			// Add a worker to the semaphore
			select {
			case sem <- struct{}{}:
				// Only proceed if we can acquire the semaphore
				wg.Add(1)

				// Process message in a goroutine
				go func() {
					defer wg.Done()
					defer func() { <-sem }() // Release the semaphore when done

					// Fetch next message
					msg, err := iter.Next()
					if err != nil {
						slog.Error("Error fetching message", "error", err)
						return
					}

					// Process the message
					var job payload.QueuePayload
					if err := json.Unmarshal(msg.Data(), &job); err != nil {
						slog.Error("Failed to unmarshal message payload", "error", err, "data", string(msg.Data()))
						msg.Nak() // Negative acknowledgment, message will be redelivered
						return
					}

					slog.Info("Processing job", "id", job.ID.String(), "subject", msg.Subject, "worker", fmt.Sprintf("%d/%d", len(sem), numWorkers))

					// Execute the processing function
					processing(job)

					// Acknowledge after processing
					if err := msg.Ack(); err != nil {
						slog.Error("Failed to acknowledge message", "error", err)
					}
				}()
			case <-ctx.Done():
				// Context was cancelled while waiting for a worker slot
				slog.Info("Context cancelled while waiting for worker")
				wg.Wait()
				return
			}
		}
	}
}
