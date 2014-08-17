package main

func listMetaKey(a *Arena, key []byte) []byte {
	mKey := a.Allocate(len(key) + 1)
	mKey[0] = kListKeyPrefix
	copy(mKey[1:], key)
	return mKey
}

func (h *DbHandler) Llen(c *redisClient, key []byte) (int, error) {
	metaKey := listMetaKey(c.arena, key)
	if old, err := c.db.Get(c.arena, metaKey); err != nil || old == nil {
		return 0, nil
	} else {
		li := LinkedList(old)
		llen, _ := li.listMeta()
		return llen, nil
	}
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

func (h *DbHandler) Lpop(c *redisClient, key []byte) ([]byte, error) {
	metaKey := listMetaKey(c.arena, key)
	if old, err := c.db.Get(c.arena, metaKey); err != nil || old == nil {
		return nil, err
	} else {
		li := LinkedList(old)
		_, minSeq := li.listMeta()
		if val, err := c.db.Get(c.arena, listDataKey(c.arena, metaKey, minSeq)); err != nil {
			return nil, err
		} else {
			ks, vs := li.Pop(c.arena, metaKey, 1, 0)
			return val, c.db.Batch(ks, vs)
		}
	}
}

func (h *DbHandler) Rpop(c *redisClient, key []byte) ([]byte, error) {
	metaKey := listMetaKey(c.arena, key)
	if old, err := c.db.Get(c.arena, metaKey); err != nil || old == nil {
		return nil, err
	} else {
		li := LinkedList(old)
		llen, minSeq := li.listMeta()
		if val, err := c.db.Get(c.arena, listDataKey(c.arena, metaKey, minSeq+llen-1)); err != nil {
			return nil, err
		} else {
			ks, vs := li.Pop(c.arena, metaKey, 0, 1)
			return val, c.db.Batch(ks, vs)
		}
	}
}

func (h *DbHandler) Lrange(c *redisClient, key []byte, start, end int) ([][]byte, error) {
	metaKey := listMetaKey(c.arena, key)
	if old, err := c.db.Get(c.arena, metaKey); err != nil || old == nil {
		return nil, err
	} else {
		llen, seqstart := LinkedList(old).listMeta()
		// [start, end]
		if start < 0 {
			start += llen
		}
		if end < 0 {
			end += llen
		}
		if start < 0 {
			start = 0
		}

		if start > end || start >= llen {
			/* Invariant: start >= 0, so this test will be true when end < 0.
			* The range is empty when start > end or start >= length. */
			return nil, nil
		}

		if end >= llen {
			end = llen - 1
		}
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

func (h *DbHandler) Ltrim(c *redisClient, key []byte, start, end int) error {
	metaKey := listMetaKey(c.arena, key)
	if old, err := c.db.Get(c.arena, metaKey); err != nil || old == nil {
		return err
	} else {
		li := LinkedList(old)
		llen, _ := li.listMeta()
		ltrim, rtrim := 0, 0

		// [start, end]
		if start < 0 {
			start += llen
		}
		if end < 0 {
			end += llen
		}
		if start < 0 {
			start = 0
		}

		if start > end || start >= llen {
			ltrim, rtrim = llen, 0 //  remove all
		} else {
			if end >= llen {
				end = llen - 1
			}
			ltrim, rtrim = start, llen-end-1
		}

		ks, vs := li.Pop(c.arena, metaKey, ltrim, rtrim)
		if len(ks) > 0 {
			return c.db.Batch(ks, vs)
		} else {
			return nil
		}
	}
}
