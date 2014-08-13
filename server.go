package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
)

func NewServer(cfg *RockRedisConf) (*Server, error) {
	dbs := make([]Store, cfg.Databases)
	for i := 0; i < cfg.Databases; i++ {
		dir := path.Join(cfg.Dir, "db-"+strconv.Itoa(i+1))
		if db, err := NewRockdbStore(dir, cfg.Cache, cfg.Compression); err != nil {
			return nil, err
		} else {
			dbs[i] = db
		}
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

func (s *Server) ListenAndServe() error {
	if l, err := net.Listen("tcp", s.conf.Addr); err == nil {
		for s.shutdown.Get() == 0 {
			conn, err := l.Accept()
			if err != nil {
				return err
			}
			go s.ServeClient(conn)
		}
	} else {
		return err
	}
	return nil
}

func (s *Server) ServeClient(c net.Conn) {
	client := NewReisClient(c)
	client.db = s.dbs[0] // default is database 0
	s.clients.Add(1)

	for s.shutdown.Get() == 0 {
		req, err := client.ReadRequest()
		if err != nil {
			c.Close()
			break
		}

		res, err := s.Handle(client, req)
		if err != nil {
			c.Close()
			break
		}
		res.Write(client.bw)
		if client.bw.Flush() != nil {
			c.Close()
			break
		}
	}

	// no runing clients, server get shutdown signal
	if s.clients.Add(-1) == 0 && s.shutdown.Get() != 0 {
		s.Shutdown()
	}
}

func (s *Server) Shutdown() {
	if s.shutdown.CompareAndSwap(ScheduleShutDown, CloseCalled) { // run only once
		log.Printf("Closing all %v dbs", len(s.dbs))
		for i := 0; i < len(s.dbs); i++ {
			if err := s.dbs[i].Close(); err != nil {
				log.Printf("Call close on db %v, get error: %v", i, err)
			}
		}
		log.Printf("All %v dbs closed. Bye", len(s.dbs))
		os.Exit(0)
	}
}

func (s *Server) Handle(client *redisClient, req *Request) (Reply, error) {
	if fn, ok := s.handlers[req.Command]; ok {
		return fn(client, req)
	} else {
		return ErrMethodNotSupported, nil
	}
}

func (s *Server) RegisterHandlers(handler interface{}) error {
	hType := reflect.TypeOf(handler)

	for i := 0; i < hType.NumMethod(); i++ {
		method := hType.Method(i)
		if len(method.Name) < 1 || method.Name[0] > 'Z' || method.Name[0] < 'A' {
			continue // not exported
		}
		mt := method.Type

		if !mt.Out(mt.NumOut() - 1).Implements(reflect.TypeOf(s.RegisterHandlers).Out(0)) {
			return fmt.Errorf("%v's last return value should implement error", method.Name)
		}

		// first In is handler(receiver), second is client
		convfns := make([]func(idx int, req *Request) (reflect.Value, error), mt.NumIn()-2)

		for i := 1; i < mt.NumIn(); i++ {
			switch mt.In(i) {
			case reflect.TypeOf(1):
				convfns[i-2] = func(idx int, req *Request) (reflect.Value, error) {
					n, err := parseInt(req.Arguments[idx])
					return reflect.ValueOf(n), err
				}
			case reflect.TypeOf([]byte{}):
				convfns[i-2] = func(idx int, req *Request) (reflect.Value, error) {
					return reflect.ValueOf(req.Arguments[idx]), nil
				}
			case reflect.TypeOf([][]byte{}):
				convfns[i-2] = func(idx int, req *Request) (reflect.Value, error) {
					return reflect.ValueOf(req.Arguments[idx:req.Size]), nil
				}
			}
		}

		fn := reflect.ValueOf(handler).Method(method.Index) // receiver is bind
		name := strings.ToUpper(method.Name)                // HandleGet => get
		isvariadic := mt.IsVariadic()

		s.handlers[name] = func(client *redisClient, req *Request) (Reply, error) {
			// TODO, check args
			ins := make([]reflect.Value, len(convfns)+1)
			ins[0] = reflect.ValueOf(client)

			for i := 0; i < len(convfns); i++ {
				if v, err := convfns[i](i, req); err == nil {
					ins[i+1] = v
				} else {
					return nil, err
				}
			}

			var results []reflect.Value
			if isvariadic {
				results = fn.CallSlice(ins)
			} else {
				results = fn.Call(ins)
			}

			if err := results[len(results)-1].Interface(); err != nil {
				return ErrorReply{err.(error).Error()}, nil
			}

			if len(results) > 1 {
				v := results[0].Interface()
				switch v := v.(type) {
				case []byte:
					return BulkReply{v}, nil
				case [][]byte:
					return MultiBulkReply{v}, nil
				case int:
					return IntReply{v}, nil
				}
			}
			return StatusReply{"OK"}, nil
		}
	}

	return nil
}
