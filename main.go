package main

import (
	"log"

	"github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	err = ch.Publish("", "lightweight", false, false, amqp091.Publishing{
		ContentType: "text/plain",
		Body:        []byte("Hello RabbitMQ"),
	})
	if err != nil {
		log.Fatalf("Failed to publish a message: %v", err)
	}
	log.Println("Message sent!")
}
