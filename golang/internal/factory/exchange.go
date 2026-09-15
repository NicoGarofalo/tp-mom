package factory

import (
	"context"
	"time"
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)


type ExchangeMiddleware struct {
	conn *amqp.Connection
	ch *amqp.Channel
	exchangeName string
	topicKeys []string
	queue amqp.Queue
	isConsuming bool
}


func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	
	q, err := em.ch.QueueDeclare(
		"", // name
		false, // durability
		false, // delete when unused
		true, // exclusive
		false, // no-wait
		nil, // args
	)
	if err != nil {
		if qm.conn.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	em.queue = q

	for _, topicKey := range em.topicKeys {
		err = em.ch.QueueBind(
			em.queue.Name, // queue
			topicKey, // routing key
			em.exchangeName, // exchange
			false, // no-wait
			nil, // args
		)
		if err != nil {
			return m.ErrMessageMiddlewareMessage
		}
	}
	em.isConsuming = true
	msgs, err := em.ch.Consume(
		em.queue.Name, // queue
		"tag-" + em.exchangeName, // consumer tag
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil, // args
	)
	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	for d := range msgs {
		msg := m.Message{Body: string(d.Body)}

		ackFn := func() {
			d.Ack(false)
		}

		nackFn := func() {
			d.Nack(false, false)
		}

		callbackFunc(msg, ackFn, nackFn)
	}

	if em.conn.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	return nil
}

func (em *ExchangeMiddleware) StopConsuming() error {
	if em.conn.IsClosed(){
		return m.ErrMessageMiddlewareDisconnected
	}
	if em.isConsuming{
		err := em.ch.Cancel("tag-" + em.exchangeName, false)
		if err != nil {
			return m.ErrMessageMiddlewareClose
		}
		em.isConsuming = false
	}
	return nil
}


func (em *ExchangeMiddleware) Send(msg m.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := msg.Body
	for _, topicKey := range em.topicKeys {
		err := em.ch.PublishWithContext(ctx,
		em.exchangeName,     // exchange
		topicKey, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing {
			ContentType: "text/plain",
			Body:        []byte(body),
		})
		if err != nil {
			if em.conn.IsClosed() {
				return m .ErrMessageMiddlewareDisconnected
			}
			return m.ErrMessageMiddlewareMessage
		}
	}
	
	return nil
}

func (em *ExchangeMiddleware) Close() error {
	if em.conn.IsClosed() {
		return nil
	}
	
	chErr := em.ch.Close()
	if chErr != nil {
		return m.ErrMessageMiddlewareClose
	}
	err := em.conn.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
