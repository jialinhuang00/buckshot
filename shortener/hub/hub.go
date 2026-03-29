package hub

import "sync"

// Hub broadcasts short_key strings to all subscribed SSE clients.
// Each redirect publishes the clicked key; clients increment their local count.
type Hub struct {
	mu      sync.Mutex
	clients map[chan string]struct{}
}

var Global = &Hub{clients: make(map[chan string]struct{})}

func (h *Hub) Subscribe() chan string {
	ch := make(chan string, 64)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *Hub) Unsubscribe(ch chan string) {
	h.mu.Lock()
	delete(h.clients, ch)
	h.mu.Unlock()
}

// Broadcast sends key to all subscribers. Slow clients are dropped (non-blocking send).
func (h *Hub) Broadcast(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.clients {
		select {
		case ch <- key:
		default:
		}
	}
}
