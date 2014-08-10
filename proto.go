package main

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	// "fmt"
)

type ByteBuffer struct {
	buffer []byte
	pos    int
	limit  int
}

type RedisError string

func (err RedisError) Error() string { return "Redis Error: " + string(err) }

type redisClient struct {
	conn net.Conn

	bw    *bufio.Writer
	rbuf  ByteBuffer // zero malloc for most requests
	wbuf  ByteBuffer
	req   *Request // reuse. goroutine safe: access by only one goroutine sequentially
	dbIdx int      // which db to use
	db    Store
}

func NewReisClient(conn net.Conn) *redisClient {
	return &redisClient{
		rbuf: ByteBuffer{buffer: make([]byte, 8912)},

		bw: bufio.NewWriter(conn),

		wbuf: ByteBuffer{buffer: make([]byte, 8912)},
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

func (p *ByteBuffer) moreSpace(n int, write bool) {
	if p.pos+n > cap(p.buffer) {
		size := cap(p.buffer) * 2
		if size < p.pos+n {
			size = p.pos + n
		}
		tmp := make([]byte, size)
		if write {
			copy(tmp, p.buffer[:p.pos]) // for write buffer
		} else {
			copy(tmp, p.buffer[p.pos:p.limit])
		}
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
	c.rbuf.moreSpace(length+2, false)
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

	// if len(line) == 0 {
	// 	fmt.Println(c.rbuf.pos, c.rbuf.limit)
	// 	fmt.Println(string(c.rbuf.buffer[:c.rbuf.limit]))
	// 	fmt.Println("----------------------------------------")
	// }

	size, err := parseInt(line[1:])
	if err != nil {
		return 0, RedisError("request expected a number")
	}

	return size, err
}

func (c *redisClient) readLine() ([]byte, error) {
	start := c.rbuf.pos
	for {
		if c.rbuf.pos >= c.rbuf.limit {
			c.rbuf.moreSpace(128, false)
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

	// println(string(c.rbuf.buffer[start : c.rbuf.pos-2]))

	return c.rbuf.buffer[start : c.rbuf.pos-2], nil
}

func (c *redisClient) ReadRequest() (*Request, error) {
	if c.rbuf.pos == c.rbuf.limit {
		c.rbuf.pos = 0
		c.rbuf.limit = 0
		c.rbuf.moreSpace(128, false)
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

	// pos := c.rbuf.pos
	// c.rbuf.pos += 1
	// line, err := c.readLine()
	// if err != nil {
	// 	return nil, err
	// }

	// switch c.rbuf.buffer[pos] {
	// case '*':
	// 	// size, err := strconv.Atoi(string(line))
	// 	size, err := parseInt(line)
	// 	if err != nil || size < 1 {
	// 		return nil, RedisError("request expected a number")
	// 	}

	// 	req := c.req // reuse req, zero malloc
	// 	if size > len(req.Arguments) {
	// 		req.Arguments = make([][]byte, size-1)
	// 	}

	// 	line, err := c.readLine()
	// 	if err != nil {
	// 		return nil, err
	// 	}

	// 	length, err := parseInt(line[1:]) //  line[0] == $
	// 	if err != nil || length < 1 {
	// 		return nil, err
	// 	}

	// 	req.Command = string(line)
	// 	for i := 0; i < size-1; i++ {
	// 		if arg, err := c.readLine(); err != nil {
	// 			return nil, err
	// 		} else {
	// 			req.Arguments[i] = arg
	// 		}
	// 	}
	// 	return req, nil
	// }

	// return nil, nil
}

type Request struct {
	Command   string
	Size      int
	Arguments [][]byte
}

type Reply io.WriterTo
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

func (er ErrorReply) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write([]byte("-ERROR " + er.message + "\r\n"))
	return int64(n), err
}

func (r StatusReply) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write([]byte("+" + r.code + "\r\n"))
	return int64(n), err
}

func (r IntReply) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write([]byte(":" + strconv.Itoa(r.number) + "\r\n"))
	return int64(n), err
}

func (r BulkReply) WriteTo(w io.Writer) (int64, error) {
	n, err := writeBytes(w, r.value)
	return int64(n), err
}

func writeBytes(w io.Writer, data []byte) (int, error) {
	if data == nil {
		return w.Write([]byte("$-1\r\n"))
	}
	d := "$" + strconv.Itoa(len(data)) + "\r\n" + string(data) + "\r\n"
	return w.Write([]byte(d))
}

func (r MultiBulkReply) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write([]byte("*" + strconv.Itoa(len(r.values)) + "\r\n"))
	if err == nil {
		for _, value := range r.values {
			if n_, err := writeBytes(w, value); err != nil {
				return int64(n + n_), err
			} else {
				n += n_
			}
		}
	}
	return int64(n), err
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
