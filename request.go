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
	tout  *time.Duration
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

func (req *Request) createTimeout() <-chan time.Time {
	if req.tout != nil {
		return time.After(*req.tout)
	}

	return nil
}

func (req *Request) Rsp() <-chan []byte {
	return req.rsp
}

func (req *Request) Send() error {
	//
	// TODO: req.SetRetry(N), implement here retry logic
	//
	r := req.bind.pickRandRoute()
	select {
	case r.queue <- req:
		return nil
	case <-req.createTimeout():
		return ErrTimeout
	}
}

func (req *Request) Recv() ([]byte, error) {
	select {
	case rsp := <-req.rsp:
		return rsp, nil
	case <-req.createTimeout():
		return nil, ErrTimeout
	}
}
