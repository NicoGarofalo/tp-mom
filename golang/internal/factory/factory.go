package factory

import (
	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	conn amqp.Connection
	ch amqp.Channel
	queue amqp.Queue
}


func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	msgs, err := qm.ch.Consume(
		qm.queueName,
		"",     
		false, 
		false, 
		false, 
		false, 
		nil,   
	)

	chanCloseNotify := make(chan *amqp.Error, 1)
	qm.conn.NotifyClose(chanCloseNotify)

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

	return nil
}

func (qm *QueueMiddleware) StopConsuming() error {
	return nil
}

func (qm *QueueMiddleware) Send(msg m.Message) error {
	return nil
}

func (qm *QueueMiddleware) Close() error {
	return nil
}

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	conn, err := amqp.Dial("amqp://guest:guest@" + connectionSettings.Hostname + ":" + connectionSettings.Port + "/")
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		queueName, // name
		true,    // durability
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	)
	
	qm := QueueMiddleware{
		conn: conn,
		ch: ch,
		queue: q
	}
	return &qm, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	return nil, nil
}
