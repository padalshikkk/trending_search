package ratelimit

import (
    "sync"
    "time"
)

// RateLimiter ограничивает частоту запросов (user, term) в минуту
// Использует два бакета: текущий и предыдущий
type RateLimiter struct {
    mu            sync.RWMutex
    current       map[string]int // ключ "userID:term"
    previous      map[string]int
    limit         int
    bucketSeconds int
    lastRotate    time.Time
}

// New создаёт новый ограничитель
func New(limit, bucketSeconds int) *RateLimiter {
    return &RateLimiter{
        current:       make(map[string]int),
        previous:      make(map[string]int),
        limit:         limit,
        bucketSeconds: bucketSeconds,
        lastRotate:    time.Now(),
    }
}

// Allow проверяет, можно ли пропустить запрос
func (r *RateLimiter) Allow(userID, term string) bool {
    r.mu.Lock()
    defer r.mu.Unlock()
    r.rotateIfNeeded()

    key := userID + ":" + term
    cnt := r.current[key]
    if cnt >= r.limit {
        return false
    }
    r.current[key] = cnt + 1
    return true
}

// rotateIfNeeded переключает бакеты, если прошло bucketSeconds
func (r *RateLimiter) rotateIfNeeded() {
    now := time.Now()
    elapsed := int(now.Sub(r.lastRotate).Seconds())
    if elapsed < r.bucketSeconds {
        return
    }
    r.previous = r.current
    r.current = make(map[string]int)
    r.lastRotate = now
}