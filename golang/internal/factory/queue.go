package factory

import (
	"context"
	"time"
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	conn *amqp.Connection
	ch *amqp.Channel
	queue amqp.Queue
	isConsuming bool
}


func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	msgs, err := qm.ch.Consume(
		qm.queue.Name, // queue
		"tag-" + qm.queue.Name, // consumer tag
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil, // args
	)
	if err != nil {
		if qm.conn.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}
	qm.isConsuming = true
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

	if qm.conn.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	return nil
}



func (qm *QueueMiddleware) StopConsuming() error {
	if(qm.conn.IsClosed()){
		return m.ErrMessageMiddlewareDisconnected
	}
	if qm.isConsuming {
		err := qm.ch.Cancel("tag-" + qm.queue.Name, false)
		if err != nil {
			return m.ErrMessageMiddlewareClose
		}
		qm.isConsuming = false
	}
	return nil
}


func (qm *QueueMiddleware) Send(msg m.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := msg.Body
	err := qm.ch.PublishWithContext(ctx,
	"",     // exchange
	qm.queue.Name, // routing key
	false,  // mandatory
	false,  // immediate
	amqp.Publishing {
		ContentType: "text/plain",
		Body:        []byte(body),
	})
	if err != nil {
		if qm.conn.IsClosed() {
			return m .ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	
	return nil
}

func (qm *QueueMiddleware) Close() error {
	if qm.conn.IsClosed() {
		return nil
	}

	chErr := qm.ch.Close()
	if chErr != nil {
		return m.ErrMessageMiddlewareClose
	}
	connErr := qm.conn.Close()
	if connErr != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
