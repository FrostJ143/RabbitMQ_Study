package main

import (
	"encoding/json"

	"github.com/google/uuid"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

type Event struct {
	Name string
}

func main() {
	// Connect to the Stream Plugin on RabbitMQ
	env, err := stream.NewEnvironment(stream.NewEnvironmentOptions().
		SetHost("localhost").
		SetPort(5552).
		SetUser("guest").
		SetPassword("guest"))
	if err != nil {
		panic(err)
	}

	// Declare stream, set segment size and stream size
	err = env.DeclareStream("events", stream.NewStreamOptions().
		SetMaxSegmentSizeBytes(stream.ByteCapacity{}.MB(1)).
		SetMaxLengthBytes(stream.ByteCapacity{}.MB(2)))
	if err != nil {
		panic(err)
	}

	// Create Producer Options
	// Batch 100 messages in the same message, and then compress that those messages
	// SetSubEntry allows duplicated messages
	producerOptions := stream.NewProducerOptions().
		SetProducerName("producer").
		SetSubEntrySize(100).
		SetCompression(stream.Compression{}.Gzip())

		// Create a Producer
	producer, err := env.NewProducer("events", producerOptions)
	if err != nil {
		panic(err)
	}

	// Publish 6001 messages
	for i := 0; i <= 6000; i++ {
		event := Event{
			Name: "test",
		}

		payload, err := json.Marshal(event)
		if err != nil {
			panic(err)
		}

		msg := amqp.NewMessage(payload)
		msg.Properties = &amqp.MessageProperties{
			CorrelationID: uuid.NewString(),
		}

		// SetPublishingId prevents Duplication
		msg.SetPublishingId(int64(i))

		// Sending the message
		if err := producer.Send(msg); err != nil {
			panic(err)
		}
	}

	// Wait for all messages to be sent
	producer.Close()
}
