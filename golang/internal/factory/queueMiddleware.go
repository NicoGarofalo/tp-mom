package factory

import (
	"context"
	"time"
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	BaseMiddleware
}


func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	//Corroboro que no se quiera llamar a StartConsuming luego de otro StartConsuming
	if qm.isConsuming {
		return m.ErrMessageMiddlewareMessage
	}

	qm.tag = "tag-" + qm.queue.Name
	msgs, err := qm.ch.Consume(
		qm.queue.Name, // queue
		qm.tag, // consumer tag
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil, // args
	)
	if err != nil {
		// Se corrobora que no haya fallado porque se cerró la conexión (esta validación está en varios lugares)
		if qm.conn.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	qm.isConsuming = true
	consumeMessages(msgs, callbackFunc)
	qm.isConsuming = false

	// Si se cortó el consumo porque se cortó la conexión, devolvemos error
	if qm.conn.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	return nil
}


func (qm *QueueMiddleware) Send(msg m.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := msg.Body
	err := qm.ch.PublishWithContext(ctx,
	"",     // exchange
	qm.queue.Name, // publish to this queue
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

