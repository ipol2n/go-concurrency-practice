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

func createQueue()

func fetch(postID string, c chan Result) {
	fetchC := make(chan Result)
	go fetchAPI("http://192.168.1.54:3000/posts/"+postID, fetchC)
	go fetchAPI("http://192.168.1.54:3001/posts/"+postID, fetchC)
	result := <-fetchC
	c <- result
}

func main() {
	conn, err := amqp.Dial("amqp://root:root@127.0.0.1:5672/")
	fmt.Println(conn.type())
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	ch2, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch2.Close()

	q, err := ch.QueueDeclare(
		"input", // name
		true,    // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")
	q2, err := ch2.QueueDeclare(
		"base", // name
		true,   // durable
		false,  // delete when unused
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
	)
	failOnError(err, "Failed to declare a queue")
	q3, err := ch.QueueDeclare(
		"result", // name
		true,     // durable
		false,    // delete when unused
		false,    // exclusive
		false,    // no-wait
		nil,      // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")
	err = ch2.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")
	msgs2, err := ch2.Consume(
		q2.Name, // queue
		"",      // consumer
		false,   // auto-ack
		false,   // exclusive
		false,   // no-local
		false,   // no-wait
		nil,     // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)
	base := 0
	c := make(chan Result)
	go func() {
		for {
			select {
			case d := <-msgs:
				body := string(d.Body)
				numbers := strings.Split(body, ",")
				for _, v := range numbers {
					n, _ := strconv.Atoi(v)
					go fetch(fmt.Sprintf("%d", n+base), c)
				}
				for i := 0; i < len(numbers); i++ {
					// fmt.Printf("%+v\n", <-c)
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
			case d := <-msgs2:
				base, _ = strconv.Atoi(string(d.Body))
				d.Ack(false)
			}
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
