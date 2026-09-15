package factory

import (
	"fmt"
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

func CreateQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	connString := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(connString)
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
		queue: q,
		isConsuming: false,
	}
	return &qm, nil
}


func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	connString := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	conn, err := amqp.Dial(connString)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchange, // name
		"topic", // type
		true,     // durability
		false,    // auto delete
		false,    // internal
		false,    // no-wait
		nil,  // arsgs
	)
	if err != nil {
		return nil, m.ErrMessageMiddlewareMessage
	}
	
	em := ExchangeMiddleware{
		conn: conn,
		ch: ch,
		exchangeName: exchange,
		topicKeys: keys,
		isConsuming: false,
	}
	return &em, nil
}
