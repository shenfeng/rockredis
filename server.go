package main

import (
	"fmt"
	"net"
	"reflect"
	"strings"
)

func (s *Server) ListenAndServe() error {
	if l, err := net.Listen("tcp", s.conf.Addr); err == nil {
		for {
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
	for {
		req, err := client.ReadRequest()
		if err != nil {
			c.Close()
			return
		}

		res, err := s.Handle(client, req)
		if err != nil {
			c.Close()
			return
		}

		_, err = res.WriteTo(client.bw)
		if err != nil {
			c.Close()
			return
		}

		if client.bw.Flush() != nil {
			c.Close()
		}
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
					return reflect.ValueOf(req.Arguments[idx:]), nil
				}
			}
		}

		fn := reflect.ValueOf(handler).Method(method.Index) // receiver is bind
		name := strings.ToLower(method.Name)                // HandleGet => get
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
