package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os/exec"
	"strings"

	"github.com/streadway/amqp"
	"gopkg.in/yaml.v2"
)

// Config represents the main YAML configuration
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

	HTTP struct {
		Address string `yaml:"address"`
	}
}

var (
	config Config
	waiter chan (bool)
	stats  map[string]int64
)

// loads config.yml into the global config object
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

// invokes the command handler with the specified message
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

// http status endpoint
func statusHandler(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(stats)
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

	stats = make(map[string]int64)
	stats["ok"] = 0
	stats["errors"] = 0

	// start HTTP endpoint
	http.HandleFunc("/status", statusHandler)
	go func() {
		log.Printf("Http endpoint accepting requests on %s\n", config.HTTP.Address)
		if err := http.ListenAndServe(config.HTTP.Address, nil); err != nil {
			log.Printf("warning: could not start HTTP endpoint (specified address: %s)\n", config.HTTP.Address)
		}
	}()

	// start a coroutine for each queue
	for _, queue := range config.Rabbit.Queues {
		msgs, err := ch.Consume(queue, "", false, false, false, false, nil)
		if err != nil {
			log.Fatalf("error: could not subscribe: %s\n", err)
		}

		log.Printf("Bound to queue %s\n", queue)

		go func() {
			for m := range msgs {
				log.Printf("Received message: %+v\n", m)

				if err = invokeCommand(m); err == nil {
					log.Printf("Message processed, acking")
					m.Ack(false)
					stats["ok"]++
				} else {
					// todo: send to error queue
					log.Printf("Command handler returned error: %s\n", err)
					stats["errors"]++
				}
			}
		}()
	}

	<-waiter
}
