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

	chanCloseNotify := make(chan *amqp.Error, 1)
	em.conn.NotifyClose(chanCloseNotify)

	for d := range msgs {
		select {
		case <- chanCloseNotify:
			return m.ErrMessageMiddlewareDisconnected
		default:
			msg := m.Message{Body: string(d.Body)}

			ackFn := func() {
				d.Ack(false)
			}

			nackFn := func() {
				d.Nack(false, false)
			}

			callbackFunc(msg, ackFn, nackFn)
		}
		
	}

	return nil
}

func (em *ExchangeMiddleware) StopConsuming() error {
	err := em.ch.Cancel("tag-" + em.exchangeName, false)
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}

// Falta handlear el otro error ademas de otras mejoras
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
			return m.ErrMessageMiddlewareMessage
		}
	}
	
	return nil
}

func (em *ExchangeMiddleware) Close() error {
	err := em.conn.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}
