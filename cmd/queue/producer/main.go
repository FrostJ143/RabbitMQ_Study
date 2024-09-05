package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/FrostJ143/RabbitMQ/internal"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	CACertificate     = string(http.Dir("./tls-gen/basic/result/ca_certificate.pem"))
	ClientCertificate = string(http.Dir("./tls-gen/basic/result/client_archlinux_certificate.pem"))
	ClientKey         = string(http.Dir("./tls-gen/basic/result/client_archlinux_key.pem"))
)

func main() {
	conn, err := internal.ConnectRabbitMQ("frostj", "secret", "localhost:5671", "customers",
		CACertificate,
		ClientCertificate,
		ClientKey,
	)
	if err != nil {
		panic(err)
	}

	consumeConn, err := internal.ConnectRabbitMQ("frostj", "secret", "localhost:5671", "customers",
		CACertificate,
		ClientCertificate,
		ClientKey,
	)
	if err != nil {
		panic(err)
	}

	client, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	consumeClient, err := internal.NewRabbitMQClient(consumeConn)
	if err != nil {
		panic(err)
	}
	defer consumeClient.Close()

	q, err := client.CreateQueue("", true, true)
	if err != nil {
		panic(err)
	}

	if err := client.CreateBinding(q.Name, q.Name, "customer_callbacks"); err != nil {
		panic(err)
	}

	messages, err := client.Consume(q.Name, "customer-api", true)
	if err != nil {
		panic(err)
	}

	go func() {
		for message := range messages {
			log.Printf("Message Callback %d\n", message.CorrelationId)
		}
	}()

	// if err := client.CreateQueue("customers_created", true, false); err != nil {
	// 	panic(err)
	// }
	//
	// if err := client.CreateBinding("customers_created", "customers.created.*", "customer_events"); err != nil {
	// 	panic(err)
	// }

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 10; i++ {
		if err := client.Send(ctx, "customer_events", "customers.created.us", amqp.Publishing{
			ContentType:   "text/plain",
			DeliveryMode:  amqp.Persistent,
			Body:          []byte("An cool message between services"),
			ReplyTo:       q.Name,
			CorrelationId: fmt.Sprintf("customer_created_%d", i),
		}); err != nil {
			panic(err)
		}
	}

	var blocking chan struct{}
	<-blocking
}
