package main

import (
	"encoding/binary"
	"time"
)

// list encoding:

// key => prefix(l):count(4 byte):min-seq(4-byte):inline-start-seq(4-byte):add-ts(4-byte):update-ts(4-byte):[size(varint):content] (up to ListMaxZiplistEntries)

// \0key$seq => prefix(1):content

const (
	SeqStart = 1073741824
)

type InlineList []byte

func (il InlineList) Count() int {
	return int(binary.BigEndian.Uint32(il[1:]))
}

func (il InlineList) MinSeq() int {
	return int(binary.BigEndian.Uint32(il[5:]))
}

func (il InlineList) IsFull() bool {
	return il.Count() == ListMaxZiplistEntries
}

func (il InlineList) SetCount(c int) {
	binary.BigEndian.PutUint32(il[1:], uint32(c)) // count
	// return il.Count() == ListMaxZiplistEntries
}

func (il InlineList) SetUpdateTs(now uint32) {
	binary.BigEndian.PutUint32(il[17:], now) // update-ts
	// binary.BigEndian.PutUint32(bytes[1:], uint32(count)) // count
	// return il.Count() == ListMaxZiplistEntries
}

// 0_key_:seq. binary order
func listKey(dest, key []byte, seq uint32) {
	dest[0] = 0
	copy(dest[1:], key)
	dest[1+len(key)] = ':'
	binary.BigEndian.PutUint32(dest[2+len(key):], seq)
}

func firstSave(db Store, key []byte, values ...[]byte) error {
	now := uint32(time.Now().Unix())
	length := 1 + 5*4

	count := len(values)
	if count > ListMaxZiplistEntries {
		count = ListMaxZiplistEntries
	}

	for i := 0; i < count; i++ {
		length += 5 + len(values[i])
	}

	bytes := make([]byte, length)
	bytes[0] = 'l'
	binary.BigEndian.PutUint32(bytes[1:], uint32(count)) // count
	binary.BigEndian.PutUint32(bytes[5:], SeqStart)      // min-seq
	binary.BigEndian.PutUint32(bytes[9:], SeqStart)      // inline-start-seq
	binary.BigEndian.PutUint32(bytes[13:], now)          // added-ts
	binary.BigEndian.PutUint32(bytes[17:], now)          // update-ts
	start := 21
	for i := 0; i < count; i++ { // list element save inline
		start += binary.PutUvarint(bytes[start:], uint64(len(values[i])))
		copy(bytes[start:], values[i])
		start += len(values[i])
	}

	if err := db.Set(key, bytes[:start]); err != nil {
		return err
	}

	if count < len(values) { //
		saveKey := make([]byte, len(key)+5)
		for i := count; i < len(values); i++ {
			listKey(saveKey, key, uint32(SeqStart+i))
			if err := db.Set(saveKey, values[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *DbHandler) Llen(c *redisClient, key []byte) (int, error) {
	if value, err := c.db.Get(key); err != nil {
		return 0, err
	} else if value == nil {
		return 0, nil
	} else {
		return InlineList(value).Count(), nil
	}
}

// func firstSave(db Store, )

// func (h *DbHandler) Lpush(c *redisClient, key []byte, values ...[]byte) (int, error) {
// 	if old, err := c.db.Get(key); err != nil {
// 		return 0, err
// 	} else if old == nil { // new value
// 		err = firstSave(c.db, key, values...)
// 		if err == nil {
// 			return len(values), nil
// 		} else {
// 			return 0, err
// 		}
// 	} else {
// 	}
// }

func (h *DbHandler) Rpush(c *redisClient, key []byte, values ...[]byte) (int, error) {
	if old, err := c.db.Get(key); err != nil {
		return 0, err
	} else if old == nil { // new value
		err = firstSave(c.db, key, values...)
		if err == nil {
			return len(values), nil
		} else {
			return 0, err
		}
	} else {
		now := uint32(time.Now().Unix())
		il := InlineList(old)
		exiting := il.Count()
		il.SetCount(exiting + len(values))
		il.SetUpdateTs(now)

		count := len(values) + exiting
		if count > ListMaxZiplistEntries {
			count = ListMaxZiplistEntries
		}

		if exiting < ListMaxZiplistEntries { // put more element inline
			length := len(old)
			for i := 0; i < count-exiting; i++ {
				length += 4 + len(values[i])
			}
			bytes := make([]byte, length)
			copy(bytes, old)
			start := len(old)
			for i := 0; i < count-exiting; i++ {
				start += binary.PutUvarint(bytes[start:], uint64(len(values[i])))
				copy(bytes[start:], values[i])
				start += len(values[i])
			}

			if err := c.db.Set(key, bytes[:start]); err != nil {
				return 0, err
			}
		}

		if count < len(values)+exiting { //
			saved := count - exiting
			if saved < 0 {
				saved = 0
			}
			seqstart := il.MinSeq()
			saveKey := make([]byte, len(key)+5)
			for i := saved; i < len(values); i++ {
				listKey(saveKey, key, uint32(seqstart+i+exiting))
				if err := c.db.Set(saveKey, values[i]); err != nil {
					return 0, err
				}
			}
		}
		return exiting + len(values), nil
	}
}

func (h *DbHandler) Lrange(c *redisClient, key []byte, start, end int) ([][]byte, error) {
	if old, err := c.db.Get(key); err != nil || old == nil {
		return 0, err
	} else {
		// il
	}
}
