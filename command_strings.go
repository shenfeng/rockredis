package main

// import "fmt"

func (h *DbHandler) Get(c *redisClient, key []byte) ([]byte, error) {
	db := h.server.dbs[c.db]
	return db[string(key)], nil
}

func (h *DbHandler) Set(c *redisClient, key, value []byte) error {
	db := h.server.dbs[c.db]
	db[string(key)] = value

	// fmt.Println(db)

	return nil
}

func (h *DbHandler) Setex(c *redisClient, key, value []byte, expire int) error {
	return nil
}
