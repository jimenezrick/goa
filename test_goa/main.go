package main

// XXX XXX XXX
//
// Terminology:
//
// Service
// Endpoint
//
// Announce a service plus endpoints (do not specify host, only scope)
// Bind to a service
//
// Call an endpoints
// Cast to an endpoints (no receive)
//
// Scope: broadcast, multicast, fixed list, discovery service (Raftd)
//
// XXX XXX XXX

// XXX XXX XXX
//
// PLANTEARSE LA SEMANTICA:
// Queremos async recv? recv chan?
// Abrir varios sockets para trabajar
//
// XXX XXX XXX

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"
)

import "github.com/jimenezrick/goa"

// 120.000 eventos sin el batching

const (
	routes = 8
	procs  = 256
	secs   = 10
)

var cntr uint64

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	// XXX
	log.SetOutput(ioutil.Discard)
	// XXX

	//
	// XXX: Optional logging
	//
	dom := goa.GetDomain("foo")
	if len(os.Args) == 1 {
		dom.Announce("com.bar",
			func(req []byte, seq uint64) ([]byte, uint64) {
				rsp := append([]byte("pong("+strconv.FormatUint(seq, 10)+"):"), req...)
				return rsp, seq
			})
		select {}
	} else {
		for i := 0; i < routes; i++ {
			dom.AddRoute("com.bar", "localhost:8000") // XXX: Hack
		}

		for i := 0; i < procs; i++ {
			go test(dom)
		}

		// TODO: req.RecvTimeout()
		//<-req.Rsp()
		//req.Seq()

		// TODO <-bind.Call([]byte("ping"))
		// TODO bind.Cast([]byte("ping"))

		<-time.After(secs * time.Second)
		fmt.Println(atomic.LoadUint64(&cntr) / secs)
	}
}

func test(dom *goa.Domain) {
	bind, err := dom.Bind("com.bar")
	if err != nil {
		panic(err)
	}
	// TODO: bind := dom.Bind("com.bar", "endpoint")

	for {
		req := bind.NewRequest([]byte("ping2"))
		req.SetTimeout(time.Second)
		if err := req.Send(); err != nil {
			fmt.Println("Send:", err)
			return
		}
		//println("Send()")

		_, err := req.Recv()
		if err != nil {
			fmt.Println("Recv:", err)
			return
		}
		//println("Recv()", string(rsp))
		atomic.AddUint64(&cntr, 1)
	}
}
