package main

import (
	"encoding/json"
	"github.com/robinjoseph08/redisqueue/v2"
	"log"
)

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

	return r.diskManager.Serialize(SerializableData{
		Key:  message.Stream,
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
