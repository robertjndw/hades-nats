package main

import (
	"context"
	"os"

	"github.com/ls1intum/hades/shared/payload"
	"github.com/ls1intum/hades/shared/utils"
	"github.com/nats-io/nats.go"

	"log/slog"
)

var NatsConnection *nats.Conn
var NatsJetStream nats.JetStreamContext
var subscription *nats.Subscription

type JobScheduler interface {
	ScheduleJob(ctx context.Context, job payload.QueuePayload) error
}

type HadesSchedulerConfig struct {
	Concurrency       uint   `env:"CONCURRENCY" envDefault:"1"`
	FluentdAddr       string `env:"FLUENTD_ADDR" envDefault:""`
	FluentdMaxRetries uint   `env:"FLUENTD_MAX_RETRIES" envDefault:"3"`
	NatsConfig        utils.NatsConfig
}

func main() {
	if is_debug := os.Getenv("DEBUG"); is_debug == "true" {
		slog.SetLogLoggerLevel(slog.LevelDebug)
		slog.Warn("DEBUG MODE ENABLED")
	}

	var cfg HadesSchedulerConfig
	utils.LoadConfig(&cfg)

	var executorCfg utils.ExecutorConfig
	utils.LoadConfig(&executorCfg)
	slog.Debug("Executor config: ", "config", executorCfg)

	// // Set up NATS connection
	// var err error
	// NatsConnection, err = utils.SetupNatsConnection(cfg.NatsConfig)
	// if err != nil {
	// 	slog.Error("Failed to connect to NATS", "error", err)
	// 	os.Exit(1)
	// }
	// defer NatsConnection.Close()

	// // Set up NATS JetStream
	// NatsJetStream, err = utils.SetupNatsJetStream(NatsConnection)
	// if err != nil {
	// 	slog.Error("Failed to set up JetStream", "error", err)
	// 	os.Exit(1)
	// }

	// var scheduler JobScheduler
	// switch executorCfg.Executor {
	// case "k8s":
	// 	slog.Info("Started HadesScheduler in Kubernetes mode")
	// 	scheduler = k8s.NewK8sScheduler()
	// case "docker":
	// 	slog.Info("Started HadesScheduler in Docker mode")
	// 	scheduler = docker.NewDockerScheduler().SetFluentdLogging(cfg.FluentdAddr, cfg.FluentdMaxRetries)
	// default:
	// 	slog.Error("Invalid executor specified: ", "executor", executorCfg.Executor)
	// 	os.Exit(1)
	// }

	// slog.Info("Subscribing to hades.jobs.* with work queue model")

	// // Create a consumer for all job priorities
	// subscription, err = NatsJetStream.QueueSubscribe(
	// 	"hades.jobs.*",
	// 	"hades-scheduler",
	// 	func(msg *nats.Msg) {
	// 		slog.Info("Received message", "subject", msg.Subject, "data_len", len(msg.Data))

	// 		var job payload.QueuePayload
	// 		if err := json.Unmarshal(msg.Data, &job); err != nil {
	// 			slog.Error("Failed to unmarshal message payload", "error", err, "data", string(msg.Data))
	// 			msg.Nak() // Negative acknowledgment, message will be redelivered
	// 			return
	// 		}

	// 		slog.Info("Received job", "id", job.ID.String(), "subject", msg.Subject)

	// 		// Process the job
	// 		ctx := context.Background()
	// 		if err := scheduler.ScheduleJob(ctx, job); err != nil {
	// 			slog.Error("Failed to schedule job", "error", err, "id", job.ID.String())
	// 			// Only NAK if it's a temporary error that could be resolved by retrying
	// 			if msg.Header.Get("retry-count") == "" {
	// 				msg.Header.Set("retry-count", "1")
	// 			} else {
	// 				count := msg.Header.Get("retry-count")
	// 				// Simple retry logic - would need more sophisticated approach in production
	// 				if count == "3" { // Max retries reached
	// 					slog.Error("Max retries reached for job", "id", job.ID.String())
	// 					msg.Term() // Terminal error, don't retry
	// 					return
	// 				}
	// 			}
	// 			msg.Nak() // Request redelivery
	// 			return
	// 		}

	// 		// Successfully processed the message
	// 		msg.Ack()
	// 	},
	// 	nats.ManualAck(),             // We will explicitly acknowledge messages
	// 	nats.DeliverAll(),            // Deliver all messages (required for work queue)
	// 	nats.AckWait(30*time.Second), // How long to wait for an ack before redelivery
	// )

	// if err != nil {
	// 	slog.Error("Failed to subscribe to jobs", "error", err)
	// 	os.Exit(1)
	// }

	// slog.Info("Subscribed to jobs on hades.jobs.*")

	// // Wait for termination signal
	// signalCh := make(chan os.Signal, 1)
	// signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	// <-signalCh

	// slog.Info("Shutting down scheduler...")
	// if subscription != nil {
	// 	subscription.Unsubscribe()
	// }
}
