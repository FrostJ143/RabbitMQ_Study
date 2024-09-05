package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
)

type Event struct {
	Name string
}

func main() {
	conn, err := amqp091.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", "guest", "guest", "localhost:5672", ""))
	if err != nil {
		panic(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	q, err := ch.QueueDeclare("events", true, false, false, false, amqp091.Table{
		"x-queue-type":                    "stream",
		"x-stream-max-segment-size-bytes": 30000,  // EACH SEGMENT FILE IS ALLOWED 0.03MB
		"x-max-length-bytes":              150000, // TOAL STREAM SIZE IS 0.15MB
	})
	if err != nil {
		panic(err)
	}

	for i := 0; i <= 1000; i++ {
		event := Event{
			Name: "test",
		}
		payload, err := json.Marshal(event)
		if err != nil {
			panic(err)
		}

		err = ch.PublishWithContext(context.Background(), "", q.Name, true, false, amqp091.Publishing{
			Body:          payload,
			CorrelationId: uuid.NewString(),
		})
		if err != nil {
			panic(err)
		}
	}

	// Close the channel to wait all messages being sent
	ch.Close()
	fmt.Println("Done")
}
