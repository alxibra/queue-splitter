package servers

import (
	"fmt"
	"log"

	"github.com/rabbitmq/amqp091-go"
)

type Server struct {
	PublisherConnection *amqp091.Connection
	ConsumerConnection  *amqp091.Connection
	ConsumerChannel     *amqp091.Channel
	MainChannel         *amqp091.Channel
	CanaryChannel       *amqp091.Channel
}

func Init() Server {
	pCon, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	cCon, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	cChn, err := cCon.Channel()
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	mChn, err := pCon.Channel()
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	cnryChn, err := pCon.Channel()
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	return Server{
		PublisherConnection: pCon,
		ConsumerConnection:  cCon,
		ConsumerChannel:     cChn,
		MainChannel:         mChn,
		CanaryChannel:       cnryChn,
	}
}

func (s Server) Run() error {
	msgs, err := s.ConsumerChannel.Consume(
		"lightweight", // queue name
		"",            // consumer name (auto-generated)
		false,         // auto-acknowledge
		false,         // exclusive
		false,         // no-local
		false,         // no-wait
		nil,           // arguments
	)
	if err != nil {
		log.Fatalf("Failed to register consumer for %s: %v", "lightweight", err)
	}
	done := make(chan bool)
	go func() {
		log.Println("Application start....")
		for msg := range msgs {
			log.Printf("Message from %s: %s", "lightweight", string(msg.Body))
			log.Printf("Processing message from %s...", "lightweight")

			err := s.MainChannel.Publish(
				"",
				"main",
				false,
				false,
				amqp091.Publishing{
					ContentType: msg.ContentType,
					Body:        msg.Body,
				},
			)
			if err != nil {
				log.Printf("Failed to publish message to %s: %v", "main", err)
				continue
			}

			err = msg.Ack(false)
			if err != nil {
				log.Printf("Failed to acknowledge message from %s: %v", "lightweight", err)
			}
			log.Printf("Acknowledged message from %s", "lightweight")
		}
		done <- true
	}()
	<-done
	return nil
}

func (s Server) Close() {
	fmt.Println("Close dependencies")
	s.ConsumerChannel.Close()
	s.MainChannel.Close()
	s.CanaryChannel.Close()
	s.ConsumerConnection.Close()
	s.PublisherConnection.Close()
}
