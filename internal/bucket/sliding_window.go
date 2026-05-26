package bucket

import (
    "sort"
    "sync"
    "time"
)

// SlidingWindow – скользящее окно для подсчёта частоты термов
// Кольцевой буфер бакетов (каждый бакет – одна минута)
type SlidingWindow struct {
    mu            sync.RWMutex
    buckets       []map[string]int64
    bucketSeconds int
    windowSize    int // количество бакетов
    currentIdx    int
    lastRotate    time.Time
}

// New создаёт новое окно
func New(bucketSeconds, windowMinutes int) *SlidingWindow {
    bucketCount := windowMinutes * 60 / bucketSeconds
    buckets := make([]map[string]int64, bucketCount)
    for i := range buckets {
        buckets[i] = make(map[string]int64)
    }
    return &SlidingWindow{
        buckets:       buckets,
        bucketSeconds: bucketSeconds,
        windowSize:    bucketCount,
        currentIdx:    0,
        lastRotate:    time.Now(),
    }
}

// rotateIfNeeded переключает бакеты по таймеру
func (sw *SlidingWindow) rotateIfNeeded() {
    now := time.Now()
    elapsed := int(now.Sub(sw.lastRotate).Seconds())
    if elapsed < sw.bucketSeconds {
        return
    }
    rotations := elapsed / sw.bucketSeconds
    if rotations > sw.windowSize {
        rotations = sw.windowSize
    }
    for i := 0; i < rotations; i++ {
        sw.currentIdx = (sw.currentIdx + 1) % sw.windowSize
        sw.buckets[sw.currentIdx] = make(map[string]int64)
    }
    sw.lastRotate = now.Add(-time.Duration(elapsed%sw.bucketSeconds) * time.Second)
}

// Increment добавляет единицу к счётчику терма
func (sw *SlidingWindow) Increment(term string, rateLimited bool) {
    if rateLimited {
        return
    }
    sw.mu.Lock()
    defer sw.mu.Unlock()
    sw.rotateIfNeeded()
    sw.buckets[sw.currentIdx][term]++
}

// GetTopN возвращает топ-N термов, исключая стоп-слова
func (sw *SlidingWindow) GetTopN(limit int, stoplist map[string]bool) []string {
    sw.mu.RLock()
    defer sw.mu.RUnlock()
    sw.rotateIfNeeded()

    // Суммируем все бакеты
    counts := make(map[string]int64)
    for _, b := range sw.buckets {
        for term, cnt := range b {
            if stoplist[term] {
                continue
            }
            counts[term] += cnt
        }
    }

    // Сортируем
    type pair struct {
        term  string
        count int64
    }
    pairs := make([]pair, 0, len(counts))
    for t, c := range counts {
        pairs = append(pairs, pair{t, c})
    }
    sort.Slice(pairs, func(i, j int) bool {
        if pairs[i].count == pairs[j].count {
            return pairs[i].term < pairs[j].term
        }
        return pairs[i].count > pairs[j].count
    })
    if limit > len(pairs) {
        limit = len(pairs)
    }
    result := make([]string, limit)
    for i := 0; i < limit; i++ {
        result[i] = pairs[i].term
    }
    return result
}