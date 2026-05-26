package config

import (
    "github.com/kelseyhightower/envconfig"
)

type Config struct {
    // Адреса брокеров Kafka
    KafkaBrokers  []string `envconfig:"KAFKA_BROKERS" required:"true" default:"localhost:9092"`
    KafkaTopic    string   `envconfig:"KAFKA_TOPIC"    required:"true" default:"search-events"`
    ConsumerGroup string   `envconfig:"CONSUMER_GROUP" required:"true" default:"trending-group"`
    HTTPPort      string   `envconfig:"HTTP_PORT"      default:"8080"`
    // Размер окна в минутах
    WindowMinutes int      `envconfig:"WINDOW_MINUTES" default:"5"`
    // Длительность одного бакета в секундах
    BucketSeconds int      `envconfig:"BUCKET_SECONDS" default:"60"`
    // Лимит запросов от одного пользователя на один терм в минуту (защита от накрутки)
    RateLimit     int      `envconfig:"RATE_LIMIT"     default:"20"`
    // Время жизни кеша топа в секундах
    TopCacheTTL   int      `envconfig:"TOP_CACHE_TTL"  default:"2"`
}

func Load() (*Config, error) {
    var cfg Config
    err := envconfig.Process("", &cfg)
    return &cfg, err
}