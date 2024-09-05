package internal

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQClient struct {
	// The connection used by the client
	conn *amqp.Connection
	// Channel is used to process / Send messages
	ch *amqp.Channel
}

func ConnectRabbitMQ(username, password, host, vhost, caCert, clientCert, clientKey string) (*amqp.Connection, error) {
	// Loads CA Cert from file
	ca, err := os.ReadFile(caCert)
	if err != nil {
		return nil, err
	}

	// Load keypair
	cert, err := tls.LoadX509KeyPair(clientCert, clientKey)
	if err != nil {
		return nil, err
	}

	// Add root CA to the Cert pool
	rootCAs := x509.NewCertPool()
	rootCAs.AppendCertsFromPEM(ca)

	tlsCfg := &tls.Config{
		RootCAs:      rootCAs,
		Certificates: []tls.Certificate{cert},
	}

	return amqp.DialTLS(fmt.Sprintf("amqps://%s:%s@%s/%s", username, password, host, vhost), tlsCfg)
}

func NewRabbitMQClient(conn *amqp.Connection) (RabbitMQClient, error) {
	ch, err := conn.Channel()
	err = ch.Confirm(false)
	if err != nil {
		return RabbitMQClient{}, err
	}

	return RabbitMQClient{
		conn: conn,
		ch:   ch,
	}, nil
}

func (rc RabbitMQClient) Close() error {
	return rc.ch.Close()
}

func (rc RabbitMQClient) CreateQueue(queueName string, durable, autoDelete bool) (amqp.Queue, error) {
	q, err := rc.ch.QueueDeclare(queueName, durable, autoDelete, false, false, nil)
	if err != nil {
		return amqp.Queue{}, err
	}

	return q, err
}

func (rc RabbitMQClient) CreateBinding(queueName, bindingRule, exchangeName string) error {
	// leaving noWait to false will make the channel return an error if its fails to bind
	return rc.ch.QueueBind(queueName, bindingRule, exchangeName, false, nil)
}

func (rc RabbitMQClient) Send(ctx context.Context, exchangeName, routingKey string, options amqp.Publishing) error {
	// Confirmation confirms client if the message has been sent to the exchange successfully or not
	confirmation, err := rc.ch.PublishWithDeferredConfirmWithContext(ctx,
		exchangeName,
		routingKey,
		// mandatory is used to determined if an error should be returned upon failure
		true,
		false,
		options,
	)
	if err != nil {
		return err
	}

	log.Println(confirmation.Wait())
	return nil
}

func (rc RabbitMQClient) Consume(queueName, consumerName string, autoAck bool) (<-chan amqp.Delivery, error) {
	return rc.ch.Consume(queueName, consumerName, autoAck, false, false, false, nil)
}

func (rc RabbitMQClient) QoS(count, size int, global bool) error {
	return rc.ch.Qos(count, size, global)
}
