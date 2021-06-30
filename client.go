package service

import (
	"log"

	"github.com/streadway/amqp"
)

type RPCClient struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	msgs <-chan amqp.Delivery
	q    amqp.Queue
}

func NewRPCClient() *RPCClient {
	var err error
	ret := &RPCClient{}
	ret.conn, err = amqp.Dial("amqp://guest:guest@localhost:5672/")
	FailOnError(err, "Failed to connect to RabbitMQ")

	ret.ch, err = ret.conn.Channel()
	FailOnError(err, "Failed to open a channel")

	ret.q, err = ret.ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	FailOnError(err, "Failed to declare a queue")

	ret.msgs, err = ret.ch.Consume(
		ret.q.Name, // queue
		"",         // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	FailOnError(err, "Failed to register a consumer")

	return ret
}

func (cl *RPCClient) Request(srvName, corrID string, args []byte) {
	err := cl.ch.Publish(
		"",      // exchange
		srvName, // routing key
		false,   // mandatory
		false,   // immediate
		amqp.Publishing{
			ContentType:   "application/json",
			CorrelationId: corrID,
			ReplyTo:       cl.q.Name,
			Body:          args,
		})
	FailOnError(err, "Failed to publish a message")
}

func (cl *RPCClient) LogResult(corrID string) []byte {
	for d := range cl.msgs {
		if corrID == d.CorrelationId {
			log.Println(string(d.Body))
			return d.Body
		}
	}
	return nil
}

func (cl *RPCClient) Close() {
	cl.conn.Close()
	cl.ch.Close()
}
