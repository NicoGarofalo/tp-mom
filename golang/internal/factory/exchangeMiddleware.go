package factory

import (
	"context"
	"time"
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)


type ExchangeMiddleware struct {
	BaseMiddleware
	exchangeName string
	topicKeys []string
}


func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	//Corroboro que no se quiera llamar a StartConsuming luego de otro StartConsuming
	if em.isConsuming {
		return m.ErrMessageMiddlewareMessage
	}
	
	q, err := em.ch.QueueDeclare(
		"", // name
		false, // durability
		false, // delete when unused
		true, // exclusive
		false, // no-wait
		nil, // args
	)
	if err != nil {
		if em.conn.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}

	em.queue = q
	em.tag = "tag-" + em.exchangeName

	// Bindeo / vinculo la queue a cada topic key que hay en el topicKeys del struct
	for _, topicKey := range em.topicKeys {
		err = em.ch.QueueBind(
			em.queue.Name, // queue
			topicKey, // routing key
			em.exchangeName, // exchange
			false, // no-wait
			nil, // args
		)
		if err != nil {
			if em.conn.IsClosed() {
				return m.ErrMessageMiddlewareDisconnected
			}
			return m.ErrMessageMiddlewareMessage
		}
	}

	// Consumo todos los topics bindeados
	msgs, err := em.ch.Consume(
		em.queue.Name, // queue
		em.tag, // consumer tag
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil, // args
	)
	if err != nil {
		if em.conn.IsClosed() {
			return m.ErrMessageMiddlewareDisconnected
		}
		return m.ErrMessageMiddlewareMessage
	}
	
	em.isConsuming = true
	consumeMessages(msgs, callbackFunc)
	em.isConsuming = false

	if em.conn.IsClosed() {
		return m.ErrMessageMiddlewareDisconnected
	}

	return nil
}



func (em *ExchangeMiddleware) Send(msg m.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Publico a todos los topicKeys que tengo definido en el struct
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
