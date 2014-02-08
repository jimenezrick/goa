package goa

import (
	"errors"
	"log"
	"net"
	"sync"
)

type Domain struct {
	name string

	mtx      sync.RWMutex
	bindings map[string]*Binding // service -> binding
}

var ErrNoRoute = errors.New("nonexistent route")

var (
	domainsMtx sync.Mutex
	domains    = make(map[string]*Domain)
)

func newDomain(name string) *Domain {
	return &Domain{
		name:     name,
		bindings: make(map[string]*Binding),
	}
}

func GetDomain(name string) *Domain {
	domainsMtx.Lock()
	defer domainsMtx.Unlock()

	dom, ok := domains[name]
	if !ok {
		dom = newDomain(name)
		domains[name] = dom
	}

	return dom
}

func (d *Domain) AddRoute(service, address string) error {
	d.mtx.Lock()
	defer d.mtx.Unlock()

	r := newRoute(address)
	if err := r.connect(); err != nil {
		return err
	}

	if b, ok := d.bindings[service]; ok {
		b.routes = append(b.routes, r)
	} else {
		d.bindings[service] = newBinding(r)
	}

	return nil
}

func (d *Domain) Bind(service string) (*ClientBinding, error) {
	d.mtx.RLock()
	defer d.mtx.RUnlock()

	b, ok := d.bindings[service]
	if !ok {
		return nil, ErrNoRoute
	}

	return newClientBinding(b), nil
}

// ///////////////////////////////////////////////////////////////////////////////////////
// XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX XXX
// ///////////////////////////////////////////////////////////////////////////////////////

func (d *Domain) Announce(service string, handler func([]byte, uint64) ([]byte, uint64)) error {
	lis, err := net.Listen("tcp", ":8000")
	if err != nil {
		return err
	}

	//
	// TODO: Optimize with batching like the client side
	//

	go func() {
		for {
			netconn, err := lis.Accept()
			if err != nil {
				log.Fatal(err)
			}
			log.Print("connection accepted")

			conn := newConn(netconn)
			go func() {
				for {
					payld, seq, err := conn.recv()
					if err != nil {
						log.Fatal(err)
					}
					log.Print("RECV ", seq, " ", string(payld))

					log.Print("handler executing")
					payld, seq = handler(payld, seq)

					req := newRequest(payld, seq)
					if err := conn.sendOne(req); err != nil {
						log.Fatal(err)
					}
					log.Print("SEND ", seq, " ", string(payld))
				}

				//
				// XXX
				//
			}()
		}
	}()

	return nil
}
