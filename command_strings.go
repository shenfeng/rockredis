package main

// import "fmt"

func (h *DbHandler) Get(c *redisClient, key []byte) ([]byte, error) {
	return c.db.Get(c.arena, key)
}

func (h *DbHandler) Set(c *redisClient, key, value []byte) error {
	return c.db.Set(key, value)
}

func (h *DbHandler) Del(c *redisClient, key []byte) error {
	return c.db.Delete(key)
}

// func (h *DbHandler) Setex(c *redisClient, key, value []byte, expire int) error {
// 	return nil
// }
