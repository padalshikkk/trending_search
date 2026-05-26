package main

import (
    "context"
    "log"
    "net/http"
    "os"
    "os/signal"
    "syscall"
    "time"
    "trending-search/internal/api"
    "trending-search/internal/bucket"
    "trending-search/internal/consumer"
    "trending-search/internal/ratelimit"
    "trending-search/internal/stoplist"
    "trending-search/pkg/config"

    "github.com/IBM/sarama"
    "github.com/gorilla/mux"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "go.uber.org/zap"
)

func main() {
    // Загрузка конфига
    cfg, err := config.Load()
    if err != nil {
        log.Fatalf("Ошибка загрузки конфига: %v", err)
    }

    // Логгер
    logger, _ := zap.NewProduction()
    defer logger.Sync()

    // Компоненты
    sw := bucket.New(cfg.BucketSeconds, cfg.WindowMinutes)
    rl := ratelimit.New(cfg.RateLimit, cfg.BucketSeconds)
    sl := stoplist.New()

    // Kafka consumer
    saramaConfig := sarama.NewConfig()
    saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
    saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

    consumerGroup, err := sarama.NewConsumerGroup(cfg.KafkaBrokers, cfg.ConsumerGroup, saramaConfig)
    if err != nil {
        logger.Fatal("Не удалось создать consumer group", zap.Error(err))
    }
    defer consumerGroup.Close()

    cons := consumer.NewConsumer(sw, rl, logger)
    ctx, cancel := context.WithCancel(context.Background())
    go func() {
        for {
            if err := consumerGroup.Consume(ctx, []string{cfg.KafkaTopic}, cons); err != nil {
                logger.Error("Ошибка в consumer", zap.Error(err))
            }
            if ctx.Err() != nil {
                return
            }
        }
    }()
    <-cons.Ready()
    logger.Info("Consumer готов")

    // HTTP сервер
    handler := api.NewHandler(sw, sl, cfg.TopCacheTTL)
    router := mux.NewRouter()
    router.HandleFunc("/top", handler.GetTop).Methods("GET")
    router.HandleFunc("/stoplist", handler.AddStoplist).Methods("POST")
    router.HandleFunc("/stoplist", handler.RemoveStoplist).Methods("DELETE")
    router.HandleFunc("/stoplist", handler.ListStoplist).Methods("GET")
    router.Handle("/metrics", promhttp.Handler())

    httpServer := &http.Server{
        Addr:    ":" + cfg.HTTPPort,
        Handler: router,
    }
    go func() {
        logger.Info("HTTP сервер запущен", zap.String("port", cfg.HTTPPort))
        if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            logger.Fatal("HTTP сервер упал", zap.Error(err))
        }
    }()

    // Graceful shutdown
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    <-quit
    logger.Info("Завершение работы...")
    cancel()
    ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancelShutdown()
    if err := httpServer.Shutdown(ctxShutdown); err != nil {
        logger.Error("Ошибка остановки HTTP", zap.Error(err))
    }
}