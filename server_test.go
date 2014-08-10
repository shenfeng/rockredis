package main

import (
	"testing"
)

type testInt struct {
	i int
}

var testClient = NewReisClient(&MockConn{
	data: []byte("*1\r\nping\r\n" + "*2\r\nget\r\nkey\r\n" + "*3\r\ncommand\r\narg1\r\narg2\r\n")})

func (t *testInt) Add(c *redisClient, j int) (int, error) {
	return t.i + j, nil
}

func (t *testInt) Adds(c *redisClient, a int, bytes ...[]byte) (int, error) {
	return t.i + a, nil
}

func (t *testInt) test(c *redisClient, a int, bytes ...[]byte) (int, error) {
	return t.i + a, nil
}

func TestRegisterHandlers(t *testing.T) {
	s := &Server{handlers: make(map[string]HandlerFn)}
	h := &testInt{i: 10}

	if err := s.RegisterHandlers(h); err == nil {
		req := &Request{Command: "ADD", Arguments: [][]byte{[]byte("1")}}

		if r, err := s.Handle(testClient, req); err == nil {
			switch reply := r.(type) {
			case IntReply:
				if reply.number != 11 {
					t.Error("10 + 1 != 11")
				}
			default:
				t.Errorf("IntReply expected, get %v", reply)
			}
		} else {
			t.Error(err)
		}

		req = &Request{
			Command:   "ADDS",
			Arguments: [][]byte{[]byte("2"), []byte("aaa"), []byte("bbb")},
		}

		if r, err := s.Handle(testClient, req); err == nil {
			switch reply := r.(type) {
			case IntReply:
				if reply.number != 12 {
					t.Error("10 + 2 != 12")
				}
			default:
				t.Errorf("IntReply expected, get %v", reply)
			}
		} else {
			t.Error(err)
		}
	} else {
		t.Error(err)
	}
}

func BenchmarkCallHandler(b *testing.B) {
	s := &Server{handlers: make(map[string]HandlerFn)}
	h := &testInt{i: 10}

	s.RegisterHandlers(h)
	// req := &Request{Command: "add", Arguments: [][]byte{[]byte("1")}}
	req := &Request{
		Command:   "ADDS",
		Arguments: [][]byte{[]byte("2"), []byte("aaa"), []byte("bbb")},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Handle(testClient, req)
	}
}
