package main

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/gorilla/mux"
	"github.com/gorilla/websocket"
	"log"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

var brokers = []string{"127.0.0.1:9094"}

func main () {
	router := mux.NewRouter()
	//router.HandleFunc("/", rootHandler).Methods("GET")
	router.HandleFunc("/ws", wsHandler)

	srv := &http.Server{
		Handler:      router,
		Addr:         "127.0.0.1:8080",
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}
	log.Fatal(srv.ListenAndServe())
}

func parseParams(params url.Values) (topic string, start, stop int64, err error) {
	topic = params.Get("topic")
	if topic == "" {
		return "", 0, 0, errors.New("bad topic param")
	}

	startParam:=params.Get("start")
	if startParam != "" {
		start, err=strconv.ParseInt(startParam,10,64)
		if err != nil {
			return "", 0, 0, err
		}
	} else {
		start = 0
	}

	stopParam:=params.Get("stop")
	if stopParam != "" {
		stop, err = strconv.ParseInt(stopParam, 10, 64)
		if err != nil {
			return "", 0, 0, err
		}
	} else {
		stop = math.MaxInt64
	}
	return topic, getOffsetFromTimestamp(start), getOffsetFromTimestamp(stop),nil
}

func wsHandler(writer http.ResponseWriter, request *http.Request) {
	ws, err := websocket.Upgrade(writer, request, nil, 1024, 1024)
	if _, ok := err.(websocket.HandshakeError); ok {
		http.Error(writer, "Not a websocket handshake", 400)
		return
	} else if err != nil {
		return
	}

	params:=request.URL.Query()
	topic, start, stop, err:=parseParams(params)
	if err != nil {
		http.Error(writer, "Bad params" + err.Error(), 400)
		return
	}

	err=performRequest(topic, start, stop, ws)
	if err!=nil {
		http.Error(writer, "Internal error" + err.Error(), 400)
		return
	}
	ws.Close()
}

func performRequest(topic string, start, stop int64, ws *websocket.Conn) error {
	fmt.Println("kek")
	consumer, err:=sarama.NewConsumer(brokers, nil)
	if err != nil {
		return err
	}

	partitions, err := consumer.Partitions(topic)
	if err!=nil {
		return err
	}

	if len(partitions) !=1 {
		return errors.New("partitions number in topic != 1")
	}

	pc, err:=consumer.ConsumePartition(topic, partitions[0], start)
	if err!= nil {
		return err
	}

	for curOffset:=start; curOffset < stop; curOffset++ {
		message:= <-pc.Messages()
		err:=ws.WriteMessage(websocket.TextMessage, message.Value)
		if err!= nil {
			log.Println("Could not write to socket")
			return err
		}
	}

	return nil
}
/*
func rootHandler(writer http.ResponseWriter, request *http.Request) {
	body, err := ioutil.ReadFile("index.html")
	if err != nil {
		http.Error(writer, "Internal error" + err.Error(), 400)
	}

	writer.Write(body)
}
*/