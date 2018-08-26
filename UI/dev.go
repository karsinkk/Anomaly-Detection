package main

import (
	// "encoding/json"
	"encoding/csv"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gorilla/websocket"
	"log"
	"net/http"
	"os"
	"os/signal"
)

type Json_Data struct {
	square_id int
}

var Upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var f, _ = os.Open("../UI/sydney.csv")
var r = csv.NewReader(f)
var Sydney, _ = r.ReadAll()

var f1, _ = os.Open("../UI/australia.csv")
var r1 = csv.NewReader(f1)
var Australia, _ = r1.ReadAll()

func DataHandler(res http.ResponseWriter, req *http.Request) {

	conn, err := Upgrader.Upgrade(res, req, nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Specify brokers address. This is default one
	brokers := []string{"localhost:9092"}

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// Create new consumer
	master, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	topic := "result"
	consumer, err := master.ConsumePartition(topic, 0, -1)
	if err != nil {
		panic(err)
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	// Count how many message processed
	msgCount := 0
	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++

				err = conn.WriteMessage(websocket.TextMessage, msg.Value)
				if err != nil {
					fmt.Println(err)

				}
			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()
	<-doneCh
	_ = consumer.Close()
	// fmt.Println("Processed", msgCount, "messages")
	// for i := 0; i < 10; i++ {
	// 	fmt.Println("sending message")

	// }

}

func main() {
	fs := http.FileServer(http.Dir("./public"))
	http.Handle("/", fs)
	http.HandleFunc("/data", DataHandler)
	fmt.Println(Sydney[0], Australia[0])
	log.Println("Listening...")
	http.ListenAndServe(":4000", nil)
}
