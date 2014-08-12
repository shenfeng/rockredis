package main

import (
	"encoding/binary"
	// "log"
	"time"
)

// list encoding:

// key => prefix(l):count(4 byte):min-seq(4-byte):inline-start-seq(4-byte):add-ts(4-byte):update-ts(4-byte):[size(varint):content] (up to ListMaxZiplistEntries)

// \0key$seq => prefix(1):content

var (
	bigEndian = binary.BigEndian
)

const (
	SeqStart = 1073741824
)

type listMeta []byte

func NewListMeta(size int) listMeta {
	d := make(listMeta, 17)
	now := uint32(time.Now().Unix())

	// prefix(l):count(4 byte):min-seq(4-byte):add-ts(4-byte):update-ts(4-byte)
	d[0] = 'l'
	bigEndian.PutUint32(d[1:], uint32(size))     // count
	bigEndian.PutUint32(d[5:], uint32(SeqStart)) // min-seq
	bigEndian.PutUint32(d[9:], now)              // add-ts
	bigEndian.PutUint32(d[13:], now)             // update-ts
	return d
}

func (lm listMeta) getMeta() (size, seqstart int) {
	return int(bigEndian.Uint32(lm[1:])), int(bigEndian.Uint32(lm[5:]))
}

func (lm listMeta) updateMeta(size, minSeq int) {
	bigEndian.PutUint32(lm[1:], uint32(size))   // count
	bigEndian.PutUint32(lm[5:], uint32(minSeq)) // min-seq
	now := uint32(time.Now().Unix())
	bigEndian.PutUint32(lm[13:], now) // update-ts
}

func firstSave(db Store, key []byte, values ...[]byte) (int, error) {
	// TODO, batch write
	lm := NewListMeta(len(values))
	if err := db.Set(key, []byte(lm)); err != nil {
		return 0, err
	}

	listKey := make([]byte, len(key)+6)
	listKey[0] = 'l'
	copy(listKey[1:], key)
	listKey[len(key)+1] = ':'
	for i := 0; i < len(values); i++ {
		bigEndian.PutUint32(listKey[len(key)+2:], uint32(SeqStart+i))
		if err := db.Set(listKey, values[i]); err != nil {
			return 0, err
		}
	}
	return len(values), nil
}

func (h *DbHandler) Llen(c *redisClient, key []byte) (int, error) {
	if value, err := c.db.Get(key); err != nil {
		return 0, err
	} else if value == nil {
		return 0, nil
	} else {
		size, _ := listMeta(value).getMeta()
		return size, nil
	}
}

func (h *DbHandler) Rpush(c *redisClient, key []byte, values ...[]byte) (int, error) {
	if old, err := c.db.Get(key); err != nil {
		return 0, err
	} else if old == nil { // new value
		return firstSave(c.db, key, values...)
	} else {
		// TODO batch write
		lm := listMeta(old)
		size, seqStart := lm.getMeta()

		listKey := make([]byte, len(key)+6)
		listKey[0] = 'l'
		copy(listKey[1:], key)
		listKey[len(key)+1] = ':'
		for i := 0; i < len(values); i++ {
			bigEndian.PutUint32(listKey[len(key)+2:], uint32(seqStart+size+i))
			if err = c.db.Set(listKey, values[i]); err != nil {
				return 0, err
			}
		}

		lm.updateMeta(size+len(values), seqStart)
		c.db.Set(key, []byte(lm))
		return size + len(values), nil
	}
}

func (h *DbHandler) Lpush(c *redisClient, key []byte, values ...[]byte) (int, error) {
	if old, err := c.db.Get(key); err != nil {
		return 0, err
	} else if old == nil { // new value
		// FIXME, values should be reverse order
		return firstSave(c.db, key, values...)
	} else {
		// TODO batch write
		lm := listMeta(old)
		size, seqStart := lm.getMeta()

		listKey := make([]byte, len(key)+6)
		listKey[0] = 'l'
		copy(listKey[1:], key)
		listKey[len(key)+1] = ':'
		for i := 0; i < len(values); i++ {
			bigEndian.PutUint32(listKey[len(key)+2:], uint32(seqStart-i-1))
			if err = c.db.Set(listKey, values[i]); err != nil {
				return 0, err
			}
		}

		lm.updateMeta(size+len(values), seqStart-len(values))
		c.db.Set(key, []byte(lm))
		return size + len(values), nil
	}
}

func encodeListKey(key []byte, seq int) []byte {
	listKey := make([]byte, len(key)+6)
	listKey[0] = 'l'
	copy(listKey[1:], key)
	listKey[len(key)+1] = ':'
	bigEndian.PutUint32(listKey[len(key)+2:], uint32(seq))
	return listKey
}

func (h *DbHandler) Lrange(c *redisClient, key []byte, start, end int) ([][]byte, error) {
	if old, err := c.db.Get(key); err != nil || old == nil {
		return nil, err
	} else {
		lm := listMeta(old)
		size, seqStart := lm.getMeta()

		// todo param check
		if start < 0 {
			start += size
		}
		if end < 0 {
			end += size
		}
		if end > size {
			end = size
		}

		startKey := encodeListKey(key, seqStart+start)
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

		if err := c.db.Scan(startKey, nil, collector); err != nil {
			return nil, err
		}
		return result, nil
	}
}
