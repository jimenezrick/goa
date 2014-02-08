package goa

import (
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"sync"
	"time"
)

const (
	routeConnectRetries = 5
	routeQueueSize      = 128
	minBatchSize        = 1024
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
		queue:   make(chan *Request, routeQueueSize),
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
			log.Print(err)
		} else {
			break
		}

		waitBackoff(i)
	}
	if err != nil {
		// TODO: Resend queue to another route?
		return err
	}

	log.Print("route connected")
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

	var batchReqs = make([]*Request, 0, routeQueueSize)
	var batchSize = 0

	for {
		batchReqs = batchReqs[:0:cap(batchReqs)]
		batchSize = 0
	batch:
		for {
			println("AGAIN ", len(ro.queue), batchSize)
			if len(ro.queue) == 0 && batchSize > 0 {
				fmt.Println("empty queue, send batch")
				break batch
			}

			select {
			//
			// TODO: read from chan in batches and conn.sendBatch() + flush()
			//
			case req, ok := <-ro.queue:
				if !ok {
					log.Fatal("connection closed")
				}

				fmt.Println("batching")

				batchReqs = append(batchReqs, req)
				batchSize += len(req.payld)
				if batchSize >= minBatchSize {
					fmt.Println("full batch")
					break batch
				}
			//case <-time.After(time.Second):
			//println(len(ro.queue), len(batchReqs)) // XXX
			//panic("SHIT")                          // XXX
			//log.Fatal("queue blocked ", len(ro.queue))
			case <-ro.closed:
				// XXX: Take care of batched requests
				log.Print("1")
				log.Print("closing sender ", len(ro.queue))
				close(ro.exited)
				return
			}
		}

		for _, req := range batchReqs {
			req.seq = ro.seq
			ro.seq++
			ro.setPending(req)
		}

		fmt.Println("sending full batch")
		if err := ro.conn.sendBatch(batchReqs); err != nil {
			log.Print("2 ", err)
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
	log.Print("XXX reenqueueReq")
	queue <- req
}

// TODO: Resend missing responses?
func (ro *Route) recv() {
	defer ro.conn.close()
	for {
		payld, seq, err := ro.conn.recv()
		if err != nil {
			log.Print("3 ", err)
			close(ro.closed)
			<-ro.exited
			log.Print("3.1", err)
			log.Print("exit signaled")

			ro.connect()
			log.Print("3.2", err)
			return
		}
		log.Print("RECV ", seq, string(payld))

		rsp := ro.getPending(seq)
		if rsp == nil {
			log.Print("Dropping msg:", ErrUnknownSeq)
		}

		select {
		case rsp <- payld:
		default:
			panic("blocked sending respose")
		}
	}
}

// TODO: Receive batches
func (ro *Route) setPending(req *Request) {
	ro.mtx.Lock()
	ro.pending[req.seq] = req.rsp
	ro.mtx.Unlock()
}

func (ro *Route) getPending(seq uint64) chan<- []byte {
	ro.mtx.Lock()
	rsp := ro.pending[seq]
	delete(ro.pending, seq)
	ro.mtx.Unlock()
	return rsp
}
