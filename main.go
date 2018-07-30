package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

type Result struct {
	ID       string
	Name     string
	Text     string
	Endpoint int
}

func fetchAPI(url string, c chan Result) {
	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	var result Result
	json.Unmarshal(body, &result)
	c <- result
}

func fetch(postID string, c chan Result, sem chan bool) {
	fetchC := make(chan Result)
	go fetchAPI("http://127.0.0.1:3000/posts/"+postID, fetchC)
	go fetchAPI("http://127.0.0.1:3001/posts/"+postID, fetchC)
	result := <-fetchC
	<-sem
	fmt.Printf("Unlock: current buffer size is %d\n", len(sem))
	c <- result
}

func createQueue(ch *amqp.Channel, queueName string) amqp.Queue {
	q, err := ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	failOnError(err, "Failed to declare a queue")
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")
	return q
}

func consumeQueue(ch *amqp.Channel, queueName string) <-chan amqp.Delivery {
	msgs, err := ch.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	failOnError(err, "Failed to register a consumer")
	return msgs
}

func main() {
	conn, err := amqp.Dial("amqp://root:root@127.0.0.1:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q := createQueue(ch, "input")
	q2 := createQueue(ch, "base")
	q3 := createQueue(ch, "result")

	inputConsumer := consumeQueue(ch, q.Name)
	baseConsumer := consumeQueue(ch, q2.Name)

	forever := make(chan bool)
	base := 0
	c := make(chan Result)
	concurrency := 5
	sem := make(chan bool, concurrency)
	go func() {
		for {
			select {
			case d := <-inputConsumer:
				body := string(d.Body)
				fmt.Printf("Get input: %s\n", body)
				numbers := strings.Split(body, ",")
				for _, v := range numbers {
					n, _ := strconv.Atoi(v)
					sem <- true
					fmt.Printf("lock: current buffer size is %d\n", len(sem))
					go fetch(fmt.Sprintf("%d", n+base), c, sem)
				}
				for i := 0; i < len(numbers); i++ {
					s, _ := json.Marshal(<-c)
					fmt.Println(string(s))
					err = ch.Publish(
						"",      // exchange
						q3.Name, // routing key
						false,   // mandatory
						false,
						amqp.Publishing{
							DeliveryMode: amqp.Persistent,
							ContentType:  "application/json",
							Body:         s,
						})
				}
				d.Ack(false)
			case d := <-baseConsumer:
				base, _ = strconv.Atoi(string(d.Body))
				fmt.Printf("Got new base: %d\n", base)
				d.Ack(false)
			}
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
