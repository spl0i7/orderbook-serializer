package main

type RedisConfig struct {
	RedisHost     string `toml:"redis_host"`
	RedisPassword string `toml:"redis_password"`
}

type Config struct {
	Redis     RedisConfig `toml:"redis"`
	Streams   []string    `toml:"streams"`
	Directory string      `toml:"directory"`
}
