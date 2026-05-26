package api

import (
    "encoding/json"
    "net/http"
    "strconv"
    "sync"
    "time"
    "trending-search/internal/bucket"
    "trending-search/internal/metrics"
    "trending-search/internal/stoplist"
)

type Handler struct {
    sw *bucket.SlidingWindow
    sl *stoplist.Stoplist
    topCache struct {
        mu        sync.RWMutex
        result    []string
        updatedAt time.Time
    }
    cacheTTL time.Duration
}

func NewHandler(sw *bucket.SlidingWindow, sl *stoplist.Stoplist, cacheTTLSeconds int) *Handler {
    return &Handler{
        sw:       sw,
        sl:       sl,
        cacheTTL: time.Duration(cacheTTLSeconds) * time.Second,
    }
}

func (h *Handler) GetTop(w http.ResponseWriter, r *http.Request) {
    metrics.TopRequestsTotal.Inc()

    limit := 10
    if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
        if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
            limit = l
        }
    }

    // Проверяем кеш
    h.topCache.mu.RLock()
    cacheValid := time.Since(h.topCache.updatedAt) < h.cacheTTL
    cached := h.topCache.result
    h.topCache.mu.RUnlock()

    if cacheValid && cached != nil {
        metrics.TopCacheHits.Inc()
        respondJSON(w, http.StatusOK, map[string]interface{}{
            "top":   cached,
            "limit": limit,
        })
        return
    }

    stopMap := h.sl.Map()
    top := h.sw.GetTopN(limit, stopMap)

    h.topCache.mu.Lock()
    h.topCache.result = top
    h.topCache.updatedAt = time.Now()
    h.topCache.mu.Unlock()

    respondJSON(w, http.StatusOK, map[string]interface{}{
        "top":   top,
        "limit": limit,
    })
}

func (h *Handler) AddStoplist(w http.ResponseWriter, r *http.Request) {
    var req struct{ Word string `json:"word"` }
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Word == "" {
        http.Error(w, "Нужно передать {\"word\": \"...\"}", http.StatusBadRequest)
        return
    }
    h.sl.Add(req.Word)
    metrics.StoplistOps.WithLabelValues("add").Inc()
    metrics.ActiveStoplistSize.Set(float64(len(h.sl.List())))
    respondJSON(w, http.StatusOK, map[string]string{"status": "added"})
}

func (h *Handler) RemoveStoplist(w http.ResponseWriter, r *http.Request) {
    word := r.URL.Query().Get("word")
    if word == "" {
        http.Error(w, "Параметр word обязателен", http.StatusBadRequest)
        return
    }
    h.sl.Remove(word)
    metrics.StoplistOps.WithLabelValues("remove").Inc()
    metrics.ActiveStoplistSize.Set(float64(len(h.sl.List())))
    respondJSON(w, http.StatusOK, map[string]string{"status": "removed"})
}

func (h *Handler) ListStoplist(w http.ResponseWriter, r *http.Request) {
    words := h.sl.List()
    respondJSON(w, http.StatusOK, map[string]interface{}{"words": words})
}

func respondJSON(w http.ResponseWriter, code int, data interface{}) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(code)
    json.NewEncoder(w).Encode(data)
}