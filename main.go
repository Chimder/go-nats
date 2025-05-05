package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal("Failed to connect to NATS:", err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal("Failed to create JetStream context:", err)
	}

	streamName := "TRITCH_STATS"
	consumerName := "TRITCH_STATS_CONSUMER"
	cons, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Name:          consumerName,
		Durable:       consumerName,
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       30 * time.Second,
		MaxDeliver:    5,
		FilterSubject: "tritch.stats",
	})
	if err != nil {
		log.Fatal("Failed to create consumer:", err)
	}

	_, err = cons.Consume(func(msg jetstream.Msg) {
		fmt.Printf("Received message: %s\n", string(msg.Data()))
		if err := msg.Ack(); err != nil {
			log.Printf("Failed to ack: %v", err)
		}
	})
	if err != nil {
		log.Fatal("Failed to start consumer:", err)
	}

	slog.Info("Server started...")
	<-ctx.Done()
	slog.Info("Shutting down server...")
}

func isStreamAlreadyExists(err error) bool {
	return err.Error() == "nats: stream already in use"
}
