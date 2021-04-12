package main

import (
	"flag"
	"github.com/BurntSushi/toml"
	"github.com/go-redis/redis/v7"
	"github.com/robinjoseph08/redisqueue/v2"
	"log"
	"time"
)

func main() {
	// Read config file, default to config.toml
	configFile := flag.String("config", "config.toml", "Path to config")
	flag.Parse()

	var config Config
	if _, err := toml.DecodeFile(*configFile, &config); err != nil {
		log.Fatalln(err)
	}

	// Create consumer, this will take care of creating consumer group
	consumer, err := redisqueue.NewConsumerWithOptions(&redisqueue.ConsumerOptions{
		VisibilityTimeout: 60 * time.Second,
		BlockingTimeout:   5 * time.Second,
		ReclaimInterval:   1 * time.Second,
		BufferSize:        100,
		Concurrency:       len(config.Streams),
		RedisClient: redis.NewClient(&redis.Options{
			Addr:     config.Redis.RedisHost,
			Password: config.Redis.RedisPassword,
		}),
	})

	if err != nil {
		log.Fatalln(err)
	}

	// Create disk manager, default write timeout of 1 minute
	diskManager := NewDiskManager(nil)
	diskManager.Start()
	defer diskManager.Stop()

	redisReader := NewRedisReader(consumer, diskManager)

	// Need to register streams before starting
	for _, stream := range config.Streams {
		redisReader.Register(stream)
	}

	redisReader.Start()

	// Wait forever
	var waitChan chan bool
	<-waitChan

}
