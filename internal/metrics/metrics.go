package metrics

import (
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promauto"
)

var (
    // Количество обработанных сообщений из Kafka (success / rate_limited)
    MessagesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "trending_search_messages_total",
        Help: "Всего обработанных поисковых сообщений",
    }, []string{"status"})

    // Количество запросов к API /top
    TopRequestsTotal = promauto.NewCounter(prometheus.CounterOpts{
        Name: "trending_search_top_requests_total",
        Help: "Количество запросов на получение топа",
    })

    // Сколько раз топ был отдан из кеша
    TopCacheHits = promauto.NewCounter(prometheus.CounterOpts{
        Name: "trending_search_top_cache_hits_total",
        Help: "Попадания в кеш топа",
    })

    // Операции со стоп-листом
    StoplistOps = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "trending_search_stoplist_ops_total",
        Help: "Количество операций со стоп-листом",
    }, []string{"op"})

    // Текущий размер стоп-листа
    ActiveStoplistSize = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "trending_search_stoplist_size",
        Help: "Сколько слов сейчас в стоп-листе",
    })
)