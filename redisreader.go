package main

import (
	"encoding/json"
	"fmt"
	"github.com/robinjoseph08/redisqueue/v2"
	"log"
	"path"
	"strconv"
	"time"
)

type RedisMessage struct {
	Connector string `json:"connector"`
	Exchange  string `json:"exchange"`
	Ts        string `json:"ts"`
	Seq       string `json:"seq"`
	Type      string `json:"type"`
	Price     string `json:"price"`
	Volume    string `json:"volume"`
	Market    string `json:"market"`
}

func (r *RedisMessage) ToKey() string {

	//[Exchange]/[Market_Initial]/[Market]/YYYY/MM/DD/{YYMMDDHH}_{ASK/BID}.JSON

	ts, _ := strconv.ParseInt(r.Ts, 10, 64)

	timestamp := time.Unix(0, ts)

	parsedTimePath := timestamp.Format("2006/01/02/06010215") + "_" + r.Type
	marketInitial := "-"
	if len(r.Market) > 0 {
		marketInitial = r.Market[0:1]
	}

	return fmt.Sprintf(path.Join(r.Exchange, marketInitial, r.Market, parsedTimePath))

}

type RedisReader struct {
	consumer    *redisqueue.Consumer
	diskManager SerializationManager
}

func (r *RedisReader) Register(name string) {

	r.consumer.Register(name, r.Consume)

}

func (r *RedisReader) Consume(message *redisqueue.Message) error {

	rawData, err := json.Marshal(message.Values)
	if err != nil {
		return err
	}

	var redisMessage RedisMessage

	if err := json.Unmarshal(rawData, &redisMessage); err != nil {
		return err
	}

	return r.diskManager.Serialize(SerializableData{
		Key:  redisMessage.ToKey(),
		Data: rawData,
	})

}

func (r *RedisReader) Start() {

	go func() {
		for err := range r.consumer.Errors {
			log.Println(err)
		}
	}()

	r.consumer.Run()

}

func NewRedisReader(consumer *redisqueue.Consumer, diskManager SerializationManager) *RedisReader {

	r := &RedisReader{
		consumer:    consumer,
		diskManager: diskManager,
	}

	go func() {
		for err := range consumer.Errors {
			log.Printf("err: %+v\n", err)
		}
	}()

	return r
}
