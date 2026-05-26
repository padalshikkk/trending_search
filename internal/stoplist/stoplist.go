package stoplist

import (
    "sync"
)

// Stoplist – потокобезопасное множество запрещённых слов
type Stoplist struct {
    mu    sync.RWMutex
    words map[string]struct{}
}

func New() *Stoplist {
    return &Stoplist{
        words: make(map[string]struct{}),
    }
}

// Add добавляет слово
func (s *Stoplist) Add(word string) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.words[word] = struct{}{}
}

// Remove удаляет слово
func (s *Stoplist) Remove(word string) {
    s.mu.Lock()
    defer s.mu.Unlock()
    delete(s.words, word)
}

// Contains проверяет наличие.
func (s *Stoplist) Contains(word string) bool {
    s.mu.RLock()
    defer s.mu.RUnlock()
    _, ok := s.words[word]
    return ok
}

// List возвращает список всех слов
func (s *Stoplist) List() []string {
    s.mu.RLock()
    defer s.mu.RUnlock()
    res := make([]string, 0, len(s.words))
    for w := range s.words {
        res = append(res, w)
    }
    return res
}

// Map возвращает карту для быстрой проверки
func (s *Stoplist) Map() map[string]bool {
    s.mu.RLock()
    defer s.mu.RUnlock()
    m := make(map[string]bool, len(s.words))
    for w := range s.words {
        m[w] = true
    }
    return m
}