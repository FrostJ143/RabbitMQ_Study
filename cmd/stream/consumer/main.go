package main

import (
	"fmt"

	"github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp091.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", "guest", "guest", "localhost:5672", ""))
	if err != nil {
		panic(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	// When consuming stream, need to set Qos or error will occur
	err = ch.Qos(50, 0, false)
	if err != nil {
		panic(err)
	}

	// When consuming stream autoAck needed to false
	messages, err := ch.Consume("events", "events_consumer", false, false, false, false, amqp091.Table{
		"x-stream-offset": 1105,
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("Starting to consume stream")
	// Loop forever and just read the messages
	for msg := range messages {
		fmt.Printf("Event: %s\n", msg.CorrelationId)
		fmt.Printf("Headers: %v\n", msg.Headers)
		fmt.Printf("Data: %v\n", msg.Body)
	}

	ch.Close()
}
