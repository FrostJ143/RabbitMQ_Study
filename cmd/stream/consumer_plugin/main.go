package main

import (
	"fmt"
	"time"

	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/amqp"
	"github.com/rabbitmq/rabbitmq-stream-go-client/pkg/stream"
)

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

	// Create Consumer Options, set Offset
	consumerOptions := stream.NewConsumerOptions().
		SetConsumerName("events_consumer").
		SetOffset(stream.OffsetSpecification{}.First())

		// Start a Consumer, run in background (using go routine)
	consumer, err := env.NewConsumer("events", messageHandler, consumerOptions)
	if err != nil {
		panic(err)
	}

	time.Sleep(10 * time.Second)

	consumer.Close()
}

func messageHandler(consumerContext stream.ConsumerContext, message *amqp.Message) {
	fmt.Printf("Event: %s\n", message.Properties.CorrelationID)
	fmt.Printf("Data: %v\n", message.GetData())
}
