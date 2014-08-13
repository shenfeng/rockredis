package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
)

type ByteBuffer struct {
	buffer []byte
	pos    int
	limit  int
}

type BufferedConn struct {
	conn   net.Conn // the destination
	buffer *ByteBuffer
}

type redisClient struct {
	conn net.Conn
	// bw   *bufio.Writer
	rbuf *ByteBuffer // zero malloc for most requests
	bw   *BufferedConn
	// wbuf ByteBuffer

	req   *Request // reuse. goroutine safe: access by only one goroutine sequentially
	dbIdx int      // which db to use
	db    Store
}

func NewReisClient(conn net.Conn) *redisClient {
	return &redisClient{
		rbuf: &ByteBuffer{buffer: make([]byte, 8912)},
		bw:   &BufferedConn{buffer: &ByteBuffer{buffer: make([]byte, 8912)}, conn: conn},
		conn: conn,
		req:  &Request{Arguments: make([][]byte, 8)},
	}
}

func parseInt(b []byte) (int, error) {
	if len(b) == 1 { // optimize the common path
		return int(b[0] - '0'), nil
	}
	return strconv.Atoi(string(b))
}

func (p *ByteBuffer) writeInt(i int) {
	if i < 10 {
		p.buffer[p.pos] = byte(i + '0')
		p.pos += 1
	} else {
		pos := p.pos
		start := pos
		for ; i > 0; i = i / 10 {
			p.buffer[pos] = byte(i%10 + '0')
			pos += 1
		}

		p.pos = pos
		pos -= 1
		for start < pos {
			p.buffer[start], p.buffer[pos] = p.buffer[pos], p.buffer[start]
			start += 1
			pos -= 1
		}
	}
	p.buffer[p.pos] = '\r'
	p.buffer[p.pos+1] = '\n'
	p.pos += 2
}

func (p *ByteBuffer) write(data []byte) {
	p.moreSpace(len(data))
	copy(p.buffer[p.pos:], data)
	p.pos += len(data)
}

func (p *ByteBuffer) moreSpace(n int) {
	if p.pos+n > cap(p.buffer) {
		size := cap(p.buffer) * 2
		if size < p.pos+n {
			size = p.pos + n
		}
		tmp := make([]byte, size)
		copy(tmp, p.buffer)
		p.buffer = tmp
	}
}

func (c *redisClient) readMore() error {
	n, err := c.conn.Read(c.rbuf.buffer[c.rbuf.limit:])
	c.rbuf.limit += n
	if err != nil {
		return err
	}
	return nil
}

func (c *redisClient) readNBytes(length int) ([]byte, error) {
	c.rbuf.moreSpace(length + 2)
	for c.rbuf.pos+length+2 > c.rbuf.limit {
		err := c.readMore()
		if err != nil {
			return nil, err
		}
	}
	start := c.rbuf.pos
	c.rbuf.pos += length + 2
	return c.rbuf.buffer[start : c.rbuf.pos-2], nil
}

func (c *redisClient) readLength() (int, error) {
	line, err := c.readLine()
	if err != nil {
		return 0, err
	}

	size, err := parseInt(line[1:]) // $, or *
	if err != nil {
		return 0, fmt.Errorf("Redis Error: request expected a number")
	}
	return size, err
}

func (c *redisClient) readLine() ([]byte, error) {
	start := c.rbuf.pos
	for {
		if c.rbuf.pos >= c.rbuf.limit {
			c.rbuf.moreSpace(128)
			err := c.readMore()
			if err != nil {
				return nil, err
			}
		}
		if c.rbuf.buffer[c.rbuf.pos] == '\n' {
			c.rbuf.pos += 1
			break
		}
		c.rbuf.pos += 1
	}
	return c.rbuf.buffer[start : c.rbuf.pos-2], nil
}

func (c *redisClient) ReadRequest() (*Request, error) {
	if c.rbuf.pos == c.rbuf.limit {
		c.rbuf.pos = 0
		c.rbuf.limit = 0
		c.rbuf.moreSpace(128)
		err := c.readMore()
		if err != nil {
			return nil, err
		}
	}

	size, err := c.readLength()
	if err != nil {
		return nil, err
	}

	req := c.req // reuse req, zero malloc
	req.Size = size - 1
	if size > len(req.Arguments) {
		req.Arguments = make([][]byte, size-1)
	}

	for i := 0; i < size; i++ {
		if l, err := c.readLength(); err == nil {
			if data, err := c.readNBytes(l); err == nil {
				if i == 0 {
					req.Command = strings.ToUpper(string(data))
				} else {
					req.Arguments[i-1] = data
				}
			}
		} else {
			return nil, err
		}
	}

	return req, nil
}

type Request struct {
	Command   string
	Size      int
	Arguments [][]byte
}

type Reply interface {
	Write(bw *BufferedConn) error
}
type ErrorReply struct{ message string }
type StatusReply struct{ code string }
type IntReply struct{ number int }
type BulkReply struct{ value []byte }
type MultiBulkReply struct{ values [][]byte }

var (
	ErrMethodNotSupported   = &ErrorReply{"Method is not supported"}
	ErrNotEnoughArgs        = &ErrorReply{"Not enough arguments for the command"}
	ErrTooMuchArgs          = &ErrorReply{"Too many arguments for the command"}
	ErrWrongArgsNumber      = &ErrorReply{"Wrong number of arguments"}
	ErrExpectInteger        = &ErrorReply{"Expected integer"}
	ErrExpectPositivInteger = &ErrorReply{"Expected positive integer"}
	ErrExpectMorePair       = &ErrorReply{"Expected at least one key val pair"}
	ErrExpectEvenPair       = &ErrorReply{"Got uneven number of key val pairs"}
)

func (er ErrorReply) Write(bw *BufferedConn) error {
	bw.buffer.write([]byte("-ERROR " + er.message + "\r\n"))
	return nil
}

func (r StatusReply) Write(bw *BufferedConn) error {
	bw.buffer.write([]byte("+" + r.code + "\r\n"))
	return nil
}

func (r IntReply) Write(bw *BufferedConn) error {
	bw.buffer.write([]byte(":" + strconv.Itoa(r.number) + "\r\n"))
	return nil
}

func (r BulkReply) Write(bw *BufferedConn) error {
	bw.writeBytes(r.value)
	return nil
}

func (r MultiBulkReply) Write(bw *BufferedConn) error {
	bw.buffer.write([]byte("*" + strconv.Itoa(len(r.values)) + "\r\n"))
	for _, value := range r.values {
		bw.writeBytes(value)
	}
	return nil
}

func (bw *BufferedConn) writeBytes(data []byte) {
	if data == nil {
		bw.buffer.write([]byte("$-1\r\n"))
	} else {
		p := bw.buffer
		p.moreSpace(len(data) + 10)
		p.buffer[p.pos] = '$'
		p.pos += 1
		p.writeInt(len(data))

		copy(p.buffer[p.pos:], data)
		p.pos += len(data)
		p.buffer[p.pos] = '\r'
		p.buffer[p.pos+1] = '\n'
		p.pos += 2
		// d := "$" + strconv.Itoa(len(data)) + "\r\n" + string(data) + "\r\n"
		// bw.buffer.write([]byte(d))
	}
}

func (bw *BufferedConn) Flush() error {
	_, err := bw.conn.Write(bw.buffer.buffer[:bw.buffer.pos])
	bw.buffer.pos = 0
	return err
}

type AtomicInt int64

func (i *AtomicInt) Add(n int64) int64 {
	return atomic.AddInt64((*int64)(i), n)
}

func (i *AtomicInt) Set(n int64) {
	atomic.StoreInt64((*int64)(i), n)
}

func (i *AtomicInt) Get() int64 {
	return atomic.LoadInt64((*int64)(i))
}

func (i *AtomicInt) CompareAndSwap(old, new int64) bool {
	return atomic.CompareAndSwapInt64((*int64)(i), old, new)
}

func (i *AtomicInt) String() string {
	return strconv.FormatInt(i.Get(), 10)
}
