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

// XXX
// 515.501 msg/s sin timeouts
// XXX

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/jimenezrick/goa"
	"github.com/jimenezrick/goa/log"
)

const (
	routes = 8
	procs  = 256
	secs   = 10
)

var (
	client     = flag.Bool("client", false, "Run in client mode")
	debug      = flag.Bool("debug", false, "Enable debug logging")
	profile    = flag.Bool("profile", false, "Enable cpu profiling")
	memprofile = flag.Bool("memprofile", false, "Enable memory profiling")

	cntr uint64
)

func main() {
	defer log.Flush()

	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()

	if *debug {
		log.EnableDebug()
	}

	if *profile {
		fd, err := os.Create("goa.prof")
		if err != nil {
			panic(err)
		}

		pprof.StartCPUProfile(fd)
		defer pprof.StopCPUProfile()
	}

	dom := goa.GetDomain("foo")
	if !*client {
		dom.Announce("com.bar",
			func(req []byte, seq uint64) ([]byte, uint64) {
				return []byte("pong" + string(req)), seq
			})
		goa.WaitExit()
	} else {
		for i := 0; i < routes; i++ {
			dom.AddRoute("com.bar", "localhost:8000") // XXX: Hack
		}

		for i := 0; i < procs; i++ {
			go test(i, dom)
		}

		// TODO: bind := dom.Bind("com.bar", "endpoint")
		// TODO <-bind.Call([]byte("ping"))
		// TODO bind.Cast([]byte("ping"))

		<-time.After(secs * time.Second)
		fmt.Println(atomic.LoadUint64(&cntr) / secs)
	}

	if *memprofile {
		fd, err := os.Create("goa.memprof")
		if err != nil {
			panic(err)
		}

		pprof.WriteHeapProfile(fd)
		fd.Close()
	}
}

func test(n int, dom *goa.Domain) {
	bind, err := dom.Bind("com.bar")
	if err != nil {
		panic(err)
	}

	payld := []byte("ping" + strconv.Itoa(n))
	rsp2 := "pong" + string(payld)
	for {
		req := bind.NewRequest(payld)
		// XXX req.SetTimeout(time.Second)
		if err := req.Send(); err != nil {
			panic(err)
		}
		log.Debug("SEND ", string(payld))

		rsp, err := req.Recv()
		if err != nil {
			panic(err)
		}

		if rsp2 != string(rsp) {
			panic("bad request")
		}

		log.Debug("RECV ", string(rsp))
		atomic.AddUint64(&cntr, 1)
	}
}
