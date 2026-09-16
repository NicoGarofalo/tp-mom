package factory

import (
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type BaseMiddleware struct {
	conn *amqp.Connection
	ch *amqp.Channel
	queue amqp.Queue
	tag string
	isConsuming bool
}

func (bm *BaseMiddleware) StopConsuming() error {
	if(bm.conn.IsClosed()){
		return m.ErrMessageMiddlewareDisconnected
	}
	if bm.isConsuming {
		err := bm.ch.Cancel(bm.tag, false)
		if err != nil {
			return m.ErrMessageMiddlewareClose
		}
		bm.isConsuming = false
	}
	return nil
}

func (bm *BaseMiddleware) Close() error {
	if bm.conn.IsClosed() {
		return nil
	}

	chErr := bm.ch.Close()
	if chErr != nil {
		return m.ErrMessageMiddlewareClose
	}
	connErr := bm.conn.Close()
	if connErr != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}

// Funcion auxiliar que tienen en común tanto Queue como Exchange.
// Obtiene mensajes del channel, y llamo a la callback function con el msg y las funciones de ack y nack.
func consumeMessages(msgs <-chan amqp.Delivery, callbackFunc func(msg m.Message, ack func(), nack func())) {
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
}