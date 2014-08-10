package main

import (
	"flag"
	// "fmt"
	// "github.com/tecbot/gorocksdb"
	"log"
	"runtime"
)

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

const (
	MetaPrefix      = 0
	StringKeyPrefix = 's'
	ListKeyPrefix   = 'l'
)

type HandlerFn func(client *redisClient, req *Request) (Reply, error)

type Server struct {
	conf     *RockRedisConf
	handlers map[string]HandlerFn
	dbs      []map[string][]byte
}

type DbHandler struct {
	server *Server
}

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}

func NewServer(cfg *RockRedisConf) (*Server, error) {
	dbs := make([]map[string][]byte, cfg.Databases)
	for i := 0; i < cfg.Databases; i++ {
		dbs[i] = make(map[string][]byte)
	}

	s := &Server{
		conf:     cfg,
		handlers: make(map[string]HandlerFn),
		dbs:      dbs,
	}

	if err := s.RegisterHandlers(&DbHandler{server: s}); err != nil {
		return nil, err
	}
	return s, nil
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
		log.Printf("using %v, listen on %v, dbs: %v, lru cache: %v", cfgfile, cfg.Addr, cfg.Databases, cfg.Cache)
		log.Fatal(s.ListenAndServe())
	}
}
