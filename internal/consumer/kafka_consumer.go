package consumer

import (
    "encoding/json"
    "trending-search/internal/bucket"
    "trending-search/internal/metrics"
    "trending-search/internal/ratelimit"

    "github.com/IBM/sarama"
    "go.uber.org/zap"
)

// SearchEvent – структура сообщения из Kafka
type SearchEvent struct {
    Term   string `json:"term"`
    UserID string `json:"user_id"`
}

// Consumer реализует sarama.ConsumerGroupHandler
type Consumer struct {
    ready  chan bool
    sw     *bucket.SlidingWindow
    rl     *ratelimit.RateLimiter
    logger *zap.Logger
}

// NewConsumer создаёт потребителя
func NewConsumer(sw *bucket.SlidingWindow, rl *ratelimit.RateLimiter, logger *zap.Logger) *Consumer {
    return &Consumer{
        ready:  make(chan bool),
        sw:     sw,
        rl:     rl,
        logger: logger,
    }
}

// Ready возвращает канал готовности
func (c *Consumer) Ready() <-chan bool {
    return c.ready
}

// Setup вызывается при старте
func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
    close(c.ready)
    return nil
}

// Cleanup вызывается при остановке
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
    return nil
}

// ConsumeClaim обрабатывает сообщения
func (c *Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
    for msg := range claim.Messages() {
        var event SearchEvent
        if err := json.Unmarshal(msg.Value, &event); err != nil {
            c.logger.Warn("Ошибка парсинга JSON", zap.Error(err))
            sess.MarkMessage(msg, "")
            continue
        }

        allowed := c.rl.Allow(event.UserID, event.Term)
        if !allowed {
            metrics.MessagesTotal.WithLabelValues("rate_limited").Inc()
            sess.MarkMessage(msg, "")
            continue
        }
        metrics.MessagesTotal.WithLabelValues("success").Inc()
        c.sw.Increment(event.Term, false)
        sess.MarkMessage(msg, "")
    }
    return nil
}