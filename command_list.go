package main

import (
	"fmt"
)

func (h *DbHandler) Rpush(c *redisClient, key []byte, values ...[]byte) (int, error) {
	if old, err := c.db.Get(c.arena, key); err != nil {
		return 0, err
	} else if old == nil { // new value
		ks, vs := NewLinkedList(c.arena, key, values)
		return len(values), c.db.Batch(ks, vs)
	} else {
		li := LinkedList(old)
		size, _ := li.listMeta()
		ks, vs := li.Rpush(c.arena, key, values)
		return size + len(values), c.db.Batch(ks, vs)
	}
}

func (h *DbHandler) Lpush(c *redisClient, key []byte, values ...[]byte) (int, error) {
	if old, err := c.db.Get(c.arena, key); err != nil {
		return 0, err
	} else if old == nil { // new value
		ks, vs := NewLinkedList(c.arena, key, values)
		return len(values), c.db.Batch(ks, vs)
	} else {
		li := LinkedList(old)
		size, _ := li.listMeta()
		ks, vs := li.Lpush(c.arena, key, values)
		return size + len(values), c.db.Batch(ks, vs)
	}
}

func normalsize(input, size int) int {
	for input < 0 {
		input += size
	}
	if input > size {
		input = size
	}
	return input
}

func (h *DbHandler) Lrange(c *redisClient, key []byte, start, end int) ([][]byte, error) {
	if old, err := c.db.Get(c.arena, key); err != nil || old == nil {
		return nil, err
	} else {
		size, seqstart := LinkedList(old).listMeta()
		start, end = normalsize(start, size), normalsize(end, size)
		if start > end || start > size {
			return nil, fmt.Errorf("Lrange: begin(%v) > end(%v)", start, end)
		}
		startKey := listKey(c.arena, key, start+seqstart)
		result := make([][]byte, 0, end-start)

		n := 0
		collector := func(k, v []byte) bool {
			result = append(result, v)
			n += 1
			if n >= end-start {
				return false
			} else {
				return true
			}
		}

		if err := c.db.Scan(c.arena, startKey, collector); err != nil {
			return nil, err
		}
		return result, nil
	}
}
