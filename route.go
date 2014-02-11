package goa

import (
	"math"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/jimenezrick/goa/log"
)

const (
	routeConnectRetries = 5
	routeQueueLen       = 128
	minBatchSize        = 1024
	maxBatchLen         = routeQueueLen / 8
)

type Route struct {
	addr  string
	conn  *conn
	seq   uint64
	queue chan *Request

	closed chan struct{}
	exited chan struct{}

	//
	// TODO: atomic replacement of map
	//
	mtx     sync.Mutex
	pending map[uint64]chan<- []byte
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func newRoute(addr string) *Route {
	return &Route{
		addr:    addr,
		queue:   make(chan *Request, routeQueueLen),
		closed:  make(chan struct{}),
		exited:  make(chan struct{}),
		pending: make(map[uint64]chan<- []byte),
	}
}

// TODO: connectAsync()
func (ro *Route) connect() error {
	var netconn net.Conn
	var err error

	for i := 0; i < routeConnectRetries; i++ {
		netconn, err = net.Dial("tcp", ro.addr)
		if err == nil {
			break
		}

		log.Debug(err)
		waitBackoff(i)
	}
	if err != nil {
		// TODO: Resend queue to another route?
		return err
	}

	log.Debug("Connected to", netconn.RemoteAddr())
	ro.conn = newConn(netconn)
	ro.closed = make(chan struct{})
	ro.exited = make(chan struct{})

	go ro.send()
	go ro.recv()

	return nil
}

func waitBackoff(i int) {
	s := rand.Float64() * (math.Pow(2, float64(i)) - 1)
	time.Sleep(time.Duration(s * float64(time.Second)))
}

func (ro *Route) send() {
	defer ro.conn.close()

	var reqsBatch = make([]*Request, 0, routeQueueLen)
	var batchSize = 0

	for {
		reqsBatch = reqsBatch[:0:cap(reqsBatch)]
		batchSize = 0
	batch:
		for {
			if len(ro.queue) == 0 && len(reqsBatch) > 0 {
				break batch
			}

			select {
			//
			// TODO: Add a cancel channel to remove timeouted replies from the client
			//
			case req, ok := <-ro.queue:
				if !ok {
					panic("queue closed")
				}

				reqsBatch = append(reqsBatch, req)
				batchSize += len(req.payld)
				if len(reqsBatch) == maxBatchLen || batchSize >= minBatchSize {
					break batch
				}
			case <-ro.closed:
				cancelBatch(reqsBatch)
				close(ro.exited)
				return
			}
		}

		for _, req := range reqsBatch {
			req.seq = ro.seq
			ro.seq++
		}
		ro.setPending(reqsBatch)

		if err := ro.conn.sendBatch(reqsBatch); err != nil {
			close(ro.exited)
			cancelBatch(reqsBatch)
			return
		}
	}
}

func cancelBatch(reqs []*Request) {
	for _, r := range reqs {
		r.cancel()
	}
}

// XXX XXX XXX
// TODO: Resend missing responses?
//       Clean pending map?
// XXX XXX XXX
func (ro *Route) recv() {
	defer ro.conn.close()
	for {
		payld, seq, err := ro.conn.recv()
		if err != nil {
			close(ro.closed)
			<-ro.exited
			ro.cancelPending()
			ro.connect()
			return
		}

		rsp := ro.getPending(seq)
		if rsp == nil {
			panic("unknown sequence number")
		}
		rsp <- payld
	}
}

func (ro *Route) setPending(reqs []*Request) {
	ro.mtx.Lock()
	for _, r := range reqs {
		if r.rsp != nil {
			ro.pending[r.seq] = r.rsp
		}
	}
	ro.mtx.Unlock()
}

func (ro *Route) getPending(seq uint64) chan<- []byte {
	ro.mtx.Lock()
	rsp := ro.pending[seq]
	delete(ro.pending, seq)
	ro.mtx.Unlock()
	return rsp
}

func (ro *Route) cancelPending() {
	ro.mtx.Lock()
	for _, c := range ro.pending {
		close(c)
	}
	ro.mtx.Unlock()
}
