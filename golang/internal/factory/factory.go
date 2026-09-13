package factory

import (
	"github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	conn amqp.Connection
	ch amqp.Channel
	queue amqp.Queue
	consumerTag string
}


func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	qm.consumerTag = "tag-" + qm.queue.Name

	msgs, err := qm.ch.Consume(
		qm.queueName,
		qm.consumerTag,
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
	err := qm.ch.Cancel(qm.consumerTag)
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
	return nil
}

// Falta handlear el otro error ademas de otras mejoras
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
		return m.ErrMessageMiddlewareMessage
	}

	
	return nil
}

func (qm *QueueMiddleware) Close() error {
	err := qm.conn.Close()
	if err != nil {
		return m.ErrMessageMiddlewareClose
	}
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
	if err != nil {
		return nil, m.ErrMessageMiddlewareMessage
	}
	
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
