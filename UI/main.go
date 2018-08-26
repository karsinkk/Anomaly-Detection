package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gorilla/websocket"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"time"
)

type Json_Data struct {
	Sms_in    string
	Sms_out   string
	Internet  string
	Time      string
	Call_in   string
	Call_out  string
	IsAnomaly bool
	Lat       string
	Long      string
}

var Upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var f, _ = os.Open("./sydney.csv")
var r = csv.NewReader(f)
var Sydney, _ = r.ReadAll()

var f1, _ = os.Open("./australia.csv")
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

	var data Json_Data
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msgCount++
				_ = json.Unmarshal(msg.Value, &data)
				// str := fmt.Sprintf("%+v \n", data)
				// fmt.Print(str)
				if msgCount < 40 {
					data.Long = Australia[r1.Intn(30)][0]
					data.Lat = Australia[r1.Intn(30)][1]
				} else {
					data.Long = Sydney[r1.Intn(933)][0]
					data.Lat = Sydney[r1.Intn(933)][1]
				}
				b, _ := json.Marshal(data)
				_ = conn.WriteMessage(websocket.TextMessage, b)
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
	fmt.Println(Sydney[0][0], Sydney[0][1], Australia[0][0], Australia[0][1])
	log.Println("Listening...")
	http.ListenAndServe(":4000", nil)
}
