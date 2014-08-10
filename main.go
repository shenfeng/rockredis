package main

import (
	"flag"
	"log"
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
	ScheduleShutDown      = 1 // receive signal, schedule shutdown
	CloseCalled           = 2 // close callded
	MetaPrefix            = 0
	StringKeyPrefix       = 's'
	ListKeyPrefix         = 'l'
	ListMaxZiplistEntries = 32
)

type HandlerFn func(client *redisClient, req *Request) (Reply, error)

type RockRedisConf struct {
	Addr        string
	Dir         string
	Compression string
	Loglevel    string
	Logfile     string
	Databases   int
	Cache       int

	// How many list element saved inline
	ListMaxZiplistEntries int
}

type Store interface {
	Get(key []byte) ([]byte, error)
	Set(key, value []byte) error
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
		if err := s.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}
}
