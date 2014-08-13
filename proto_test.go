package main

import (
	"bytes"
	"encoding/binary"
	"net"
	"strconv"
	"testing"
	"time"
)

type MockConn struct {
	data   []byte
	offset int
}

func (c *MockConn) Close() error                       { panic("not implemented") }
func (c *MockConn) LocalAddr() net.Addr                { panic("not implemented") }
func (c *MockConn) RemoteAddr() net.Addr               { panic("not implemented") }
func (c *MockConn) SetDeadline(t time.Time) error      { panic("not implemented") }
func (c *MockConn) SetReadDeadline(t time.Time) error  { panic("not implemented") }
func (c *MockConn) SetWriteDeadline(t time.Time) error { panic("not implemented") }

func (c *MockConn) Read(b []byte) (n int, err error) {
	n = copy(b, c.data)
	c.offset += n
	return n, nil
}

func (c *MockConn) Write(b []byte) (n int, err error) {
	c.data = append(c.data, b...)
	return len(b), nil
}

func TestReadRequest(t *testing.T) {
	c := NewReisClient(&MockConn{
		data: []byte("*1\r\n$4\r\nping\r\n" +
			"*2\r\n$3\r\nget\r\n$3\r\nkey\r\n" +
			"*3\r\n$7\r\ncommand\r\n$4\r\narg1\r\n$4\r\narg2\r\n"),
	})

	if r, err := c.ReadRequest(); err != nil || r.Command != "PING" {
		t.Fail()
	}

	if r, err := c.ReadRequest(); err != nil || r.Command != "GET" {
		t.Fail()
	}

	if r, err := c.ReadRequest(); err != nil || r.Command != "COMMAND" {
		t.Fail()
	}
}

func BenchmarkReadRequest(b *testing.B) {
	data := []byte("*2\r\n$3\r\nget\r\n$3\r\nkey\r\n")

	con := &MockConn{data: data}
	c := NewReisClient(con)

	for i := 0; i < b.N; i++ {
		con.data = data
		c.ReadRequest()
	}
}

func TestParseInt(t *testing.T) {
	for i := 0; i < 100; i++ {
		s := strconv.Itoa(i)
		if n, _ := parseInt([]byte(s)); n != i {
			t.Fail()
		}
	}
}

func TestEncodingBulkReply(t *testing.T) {
	br := BulkReply{[]byte("test")}
	mc := &MockConn{}
	bc := &BufferedConn{buffer: &ByteBuffer{buffer: make([]byte, 8912)}, conn: mc}
	br.Write(bc)
	bc.Flush()
	if !bytes.Equal(mc.data, []byte("$4\r\ntest\r\n")) {
		t.Errorf("not equal")
	}
}

func TestEncodingMultiBulkReply(t *testing.T) {
	br := MultiBulkReply{[][]byte{[]byte("test0123456789"), []byte("test2")}}
	mc := &MockConn{}
	bc := &BufferedConn{buffer: &ByteBuffer{buffer: make([]byte, 8912)}, conn: mc}
	br.Write(bc)
	bc.Flush()
	if !bytes.Equal(mc.data, []byte("*2\r\n$14\r\ntest0123456789\r\n$5\r\ntest2\r\n")) {
		t.Errorf("not equal")
	}
}

func BenchmarkAtoi(b *testing.B) {
	data := make([]string, 100)
	for i := 0; i < 10; i++ {
		data[i] = strconv.Itoa(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		strconv.Atoi(data[i%10])
	}
}

func BenchmarkParseInt(b *testing.B) {
	data := make([][]byte, 100)
	for i := 0; i < 10; i++ {
		data[i] = []byte(strconv.Itoa(i))
	}
	b.ResetTimer()

	// does not significant faster than Atoi: 20.8ns vs 27.0ns
	for i := 0; i < b.N; i++ {
		parseInt(data[i%10])
	}
}

// is slightly faster than varint: 14.9ns/op vs 3.90ns/op
func BenchmarkInt(b *testing.B) {
	d := [20]byte{}
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint32(d[:], uint32(i))
		binary.BigEndian.Uint32(d[:])
	}
}

func BenchmarkVarint(b *testing.B) {
	d := [20]byte{}
	for i := 0; i < b.N; i++ {
		binary.PutUvarint(d[:], uint64(i))
		binary.Uvarint(d[:])
	}
}
