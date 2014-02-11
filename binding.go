package goa

import (
	"math/rand"
	"sync"
	"time"
)

type Binding struct {
	// TODO: Atomic immutable map replacement?
	mtx    sync.RWMutex
	routes []*Route
}

type ClientBinding struct {
	*Binding
	rnd *rand.Rand
}

func newBinding(r *Route) *Binding {
	return &Binding{routes: []*Route{r}}
}

func newClientBinding(b *Binding) *ClientBinding {
	return &ClientBinding{
		b,
		rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (cb *ClientBinding) NewRequest(payload []byte) *Request {
	return &Request{
		bind:  cb,
		payld: payload,
		rsp:   make(chan []byte, 1),
	}
}

func (cb *ClientBinding) NewMessage(payload []byte) *Request {
	return &Request{
		bind:  cb,
		payld: payload,
	}
}

func (cb *ClientBinding) pickRandRoute() *Route {
	cb.mtx.RLock()
	r := cb.routes[cb.rnd.Intn(len(cb.routes))]
	cb.mtx.RUnlock()
	return r
}
