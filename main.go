package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os/exec"
	"strings"

	"github.com/streadway/amqp"
	"gopkg.in/yaml.v2"
)

// Config represents the mail YAML configuration
type Config struct {
	Rabbit struct {
		ConnectionString string   `yaml:"connectionString"`
		ConsumerTag      string   `yaml:"consumerTag"`
		Queues           []string `yaml:"queues"`
	}

	Command struct {
		Proc string `yaml:"proc"`
		Args string `yaml:"args"`
	}
}

var (
	config Config
	waiter chan (bool)
)

func loadConfig() {
	buffer, err := ioutil.ReadFile("config.yml")
	if err != nil {
		log.Fatalf("error: could not open config.yml: %s", err)
	}

	err = yaml.Unmarshal(buffer, &config)
	if err != nil {
		log.Fatalf("error: could not deserialize config.yml: %s", err)
	}
}

func invokeCommand(delivery amqp.Delivery) error {
	args := strings.Split(config.Command.Args, " ")
	cmd := exec.Command(config.Command.Proc, args...)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}

	_, err = stdin.Write(delivery.Body)
	if err != nil {
		return err
	}

	err = cmd.Start()
	if err != nil {
		return err
	}

	err = stdin.Close()
	if err != nil {
		return err
	}

	return cmd.Wait()
}

func main() {
	log.Println("Passthrough started")
	loadConfig()

	conn, err := amqp.Dial(config.Rabbit.ConnectionString)
	if err != nil {
		log.Fatalf("error: could not connect: %s\n", err)
	}

	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("error: could not open channel: %s\n", err)
	}

	// start a coroutine for each queue
	for _, queue := range config.Rabbit.Queues {
		msgs, err := ch.Consume(queue, "", false, false, false, false, nil)
		if err != nil {
			log.Fatalf("error: could not subscribe: %s\n", err)
		}

		log.Printf("Bound to queue %s\n", queue)

		go func() {
			for m := range msgs {
				fmt.Printf("Received message: %+v\n", m)

				if err = invokeCommand(m); err == nil {
					log.Printf("Message processed, acking")
					m.Ack(false)
				} else {
					// todo: send to error queue
					log.Printf("Command handler returned error: %s\n", err)
				}
			}
		}()
	}

	<-waiter
}
