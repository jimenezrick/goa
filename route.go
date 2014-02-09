package goa

import (
	"errors"
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
	queue chan *Request // TODO: atomic ring

	closed chan struct{}
	exited chan struct{}

	//
	// TODO: atomic replacement of map
	//
	mtx     sync.Mutex
	pending map[uint64]chan<- []byte
}

var ErrUnknownSeq = errors.New("unknown sequence number")

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
		if err != nil {
			log.Debug(err)
		} else {
			break
		}

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
				// XXX: Take care of batched requests
				panic("closing")
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
			//reenqueueReq(req, ro.queue)
			return
		}
		//log.Print("SEND ", req.seq)
	}
}

// FIXME: NO: Send first next connection
//
// TODO
// TODO: Return error to all non-delivered messages, push retry logic to the sender
// TODO
//
func reenqueueReq(req *Request, queue chan<- *Request) {
	// XXX: Timeout
	queue <- req
}

// TODO: Resend missing responses?
func (ro *Route) recv() {
	defer ro.conn.close()
	for {
		payld, seq, err := ro.conn.recv()
		if err != nil {
			close(ro.closed)
			<-ro.exited

			ro.connect()
			return
		}

		rsp := ro.getPending(seq)
		if rsp == nil {
			log.Debug("Dropping msg:", ErrUnknownSeq)
		}

		select {
		case rsp <- payld:
		default:
			panic("blocked sending respose")
		}
	}
}

func (ro *Route) setPending(reqs []*Request) {
	ro.mtx.Lock()
	for _, r := range reqs {
		ro.pending[r.seq] = r.rsp
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
