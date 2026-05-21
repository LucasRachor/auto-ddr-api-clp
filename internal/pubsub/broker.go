package pubsub

import (
	"sync"

	"go_auto_ddr_clp/internal/plc"
)

type Update struct {
	Tag   string
	Value plc.TagValue
}

// Broker faz fan-out de TagUpdates para múltiplos subscribers (futuro:
// compartilhar uma única sessão de polling entre vários SubscribeTags
// concorrentes em vez de cada stream pollar o CLP separadamente).
type Broker struct {
	mu   sync.RWMutex
	subs map[chan Update]struct{}
}

func New() *Broker {
	return &Broker{subs: make(map[chan Update]struct{})}
}

func (b *Broker) Subscribe(buf int) chan Update {
	ch := make(chan Update, buf)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

func (b *Broker) Unsubscribe(ch chan Update) {
	b.mu.Lock()
	delete(b.subs, ch)
	b.mu.Unlock()
	close(ch)
}

func (b *Broker) Publish(u Update) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.subs {
		select {
		case ch <- u:
		default: // drop em subscriber lento
		}
	}
}
