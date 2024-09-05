package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/FrostJ143/RabbitMQ/internal"
	amqp "github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
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

	client, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		panic(err)
	}

	publishConn, err := internal.ConnectRabbitMQ("frostj", "secret", "localhost:5671", "customers",
		CACertificate,
		ClientCertificate,
		ClientKey,
	)
	if err != nil {
		panic(err)
	}

	publishClient, err := internal.NewRabbitMQClient(publishConn)
	if err != nil {
		panic(err)
	}

	q, err := client.CreateQueue("", true, true)
	if err != nil {
		panic(err)
	}

	if err := client.CreateBinding(q.Name, "", "customer_events"); err != nil {
		panic(err)
	}

	messages, err := client.Consume(q.Name, "email-service", false)
	if err != nil {
		panic(err)
	}

	var blocking chan struct{}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	// Apply a Hard Limit on the Consumer
	if err := client.QoS(10, 0, true); err != nil {
		panic(err)
	}
	// allows maximum 10 go routines
	g.SetLimit(10)

	go func() {
		for message := range messages {
			g.Go(func() error {
				log.Println("New Message: ", string(message.Body))

				time.Sleep(10 * time.Second)

				if err := message.Ack(false); err != nil {
					log.Println("Failed to ack message")
					return err
				}

				// Reply to
				if err := publishClient.Send(ctx, "customer_callbacks", message.ReplyTo, amqp.Publishing{
					ContentType:   "text/plain",
					DeliveryMode:  amqp.Persistent,
					Body:          []byte("RPC Completed"),
					CorrelationId: message.CorrelationId,
				}); err != nil {
					return err
				}

				log.Printf("Acknoledge message %s\n", message.MessageId)
				return nil
			})
		}
	}()

	log.Println("Consuming, to close the program press CTRL+C")
	<-blocking
}
