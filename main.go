package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	runtime.GOMAXPROCS(runtime.NumCPU())
}

const (
	ScheduleShutDown = 1 // receive signal, schedule shutdown
	CloseCalled      = 2 // close callded
	StringKeyPrefix  = 's'
	ListKeyPrefix    = 'l'
)

type HandlerFn func(client *redisClient, req *Request) (Reply, error)

type RockRedisConf struct {
	Addr        string
	Dir         string
	Http        string
	Compression string
	Loglevel    string
	Logfile     string
	Databases   int
	Cache       int

	// How many list element saved inline
	ListMaxZiplistEntries int
}

type Store interface {
	Get(a *Arena, key []byte) ([]byte, error)
	Set(key, value []byte) error
	Scan(a *Arena, start []byte, collector func(key, val []byte) bool) error
	Batch(ks, vs [][]byte) error
	Delete(key []byte) error
	Close() error
	Flush() error
}

type DbHandler struct {
	server *Server
}

type Server struct {
	conf     *RockRedisConf
	handlers map[string]HandlerFn
	dbs      []Store
	shutdown AtomicInt
	clients  AtomicInt
}

func main() {
	var cfgfile string
	flag.StringVar(&cfgfile, "conf", "rockredis.conf", "Rockredis Configration File")
	flag.Parse()

	cfg := &RockRedisConf{}
	if err := ReadCfg(cfg, cfgfile); err != nil {
		log.Fatal(err)
	}

	if s, err := NewServer(cfg); err != nil {
		log.Fatal(err)
	} else {
		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGKILL)
		go func() {
			si := <-signalCh
			log.Printf("Get signal '%v', schedule shutdown", si)
			s.shutdown.Set(ScheduleShutDown)
			if s.clients.Get() == 0 {
				s.Shutdown()
			}
			// wait 200ms for all on going commands processed
			time.Sleep(time.Millisecond * 200)
			log.Printf("Wait 200ms, remaining clients %v, shutdown anyway", s.clients.Get())
			s.Shutdown()
		}()

		log.Printf("Using %v, listen on %v, dbs: %v, lru cache: %v", cfgfile, cfg.Addr, cfg.Databases, cfg.Cache)

		// go tool pprof rockredis http://localhost:6666/debug/pprof/profile
		// go tool pprof rockredis http://localhost:6666/debug/pprof/heap
		go func() {
			log.Fatal(http.ListenAndServe(cfg.Http, nil))
		}()

		if err := s.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}
}
