package goa

import (
	"errors"
	"time"
)

type Request struct {
	bind  *ClientBinding
	payld []byte
	rsp   chan []byte
	seq   uint64
	tout  *time.Duration // XXX: Use socket deadlines instead?
}

var ErrTimeout = errors.New("timeout exceeded")

func newRequest(payld []byte, seq uint64) *Request {
	return &Request{
		payld: payld,
		seq:   seq,
	}
}

func (req *Request) SetTimeout(t time.Duration) {
	req.tout = &t
}

func (req *Request) Rsp() <-chan []byte {
	return req.rsp
}

func (req *Request) Send() error {
	//
	// TODO: req.SetRetry()
	//       implement here retry logic?
	//
	var t <-chan time.Time

	if req.tout != nil {
		t = time.After(*req.tout)
	}

	r := req.bind.pickRandRoute()
	select {
	case r.queue <- req:
		return nil
	case <-t:
		return ErrTimeout
	}
}

func (req *Request) Recv() ([]byte, error) {
	var t <-chan time.Time

	if req.tout != nil {
		t = time.After(*req.tout)
	}

	select {
	case rsp := <-req.rsp:
		return rsp, nil
	case <-t:
		return nil, ErrTimeout
	}
}
