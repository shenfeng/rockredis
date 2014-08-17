package main

func listMetaKey(a *Arena, key []byte) []byte {
	mKey := a.Allocate(len(key) + 1)
	mKey[0] = kListKeyPrefix
	copy(mKey[1:], key)
	return mKey
}


func (h *DbHandler) Rpush(c *redisClient, key []byte, values ...[]byte) (int, error) {
	metaKey := listMetaKey(c.arena, key)
	if old, err := c.db.Get(c.arena, metaKey); err != nil {
		return 0, err
	} else if old == nil { // new value
		ks, vs := NewLinkedList(c.arena, metaKey, values)
		return len(values), c.db.Batch(ks, vs)
	} else {
		li := LinkedList(old)
		llen, _ := li.listMeta()
		ks, vs := li.Rpush(c.arena, metaKey, values)
		return llen + len(values), c.db.Batch(ks, vs)
	}
}

func (h *DbHandler) Lpush(c *redisClient, key []byte, values ...[]byte) (int, error) {
	metaKey := listMetaKey(c.arena, key)
	if old, err := c.db.Get(c.arena, metaKey); err != nil {
		return 0, err
	} else if old == nil { // new value
		ks, vs := NewLinkedList(c.arena, metaKey, values)
		return len(values), c.db.Batch(ks, vs)
	} else {
		li := LinkedList(old)
		llen, _ := li.listMeta()
		ks, vs := li.Lpush(c.arena, metaKey, values)
		return llen + len(values), c.db.Batch(ks, vs)
	}
}

func (h *DbHandler) Lrange(c *redisClient, key []byte, start, end int) ([][]byte, error) {
	metaKey := listMetaKey(c.arena, key)
	if old, err := c.db.Get(c.arena, metaKey); err != nil || old == nil {
		return nil, err
	} else {
		llen, seqstart := LinkedList(old).listMeta()
		// [start, end]
		if start < 0 { start += llen}
		if end < 0 { end += llen}
		if start <0 { start = 0 }

		if start > end || start >= llen { 
			/* Invariant: start >= 0, so this test will be true when end < 0.
			* The range is empty when start > end or start >= length. */
			return nil, nil
		}

		if end >= llen { end = llen - 1 }
		count := end - start + 1

		startKey := listDataKey(c.arena, metaKey, start+seqstart)
		result := make([][]byte, 0, count)

		n := 0
		collector := func(k, v []byte) bool {
			result = append(result, v)
			n += 1
			if n >= count {
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
