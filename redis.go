package redislock

import (
	"context"
	"github.com/gomodule/redigo/redis"
	"strings"
	"time"
)

type Client struct {
	pool *redis.Pool
	ClientOptions
}

type LockClient interface {
	SetNEX(ctx context.Context, key, value string, expireSeconds int64) (int64, error)
	Eval(ctx context.Context, src string, keyCount int, keysAndArgs []interface{}) (interface{}, error)
}

func NewClient(network, address, password string, opts ...ClientOption) *Client {
	c := Client{
		ClientOptions: ClientOptions{
			network:  network,
			address:  address,
			password: password,
		},
	}
	for _, opt := range opts {
		opt(&c.ClientOptions)
	}
	repairClient(&c.ClientOptions)
	pool := c.getRedisPool()
	return &Client{
		pool: pool,
	}

}

func (c *Client) getRedisPool() *redis.Pool {
	return &redis.Pool{
		MaxActive:   c.maxActive,
		MaxIdle:     c.maxIdle,
		Wait:        c.wait,
		IdleTimeout: time.Duration(c.idleTimeoutSeconds) * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := c.getRedisConn()
			if err != nil {
				return nil, err
			}
			return c, nil

		},
		TestOnBorrow: func(c redis.Conn, lastUsed time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}
func (c *Client) getRedisConn() (redis.Conn, error) {
	if c.address == "" {
		panic("Cannot get redis address from config")
	}
	var dialOpts []redis.DialOption
	if len(c.password) > 0 {
		dialOpts = append(dialOpts, redis.DialPassword(c.password))
	}
	return redis.DialContext(context.Background(), c.network, c.address, dialOpts...)
}

func (c *Client) SetNEX(ctx context.Context, key, value string, expireSeconds int64) (int64, error) {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer conn.Close()
	reply, err := conn.Do("SET", key, value, "EX", expireSeconds, "NX")
	if err != nil {
		return -1, err
	}
	if replyStr, ok := reply.(string); ok && strings.ToLower(replyStr) == "ok" {
		return 1, nil
	}
	//return redis.Int64(reply, err)
	r, _ := reply.(int64)
	return r, err
}

func (c *Client) SetNX(ctx context.Context, key, val string) (int64, error) {
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer conn.Close()
	reply, err := conn.Do("SET", key, val, "NX")
	if err != nil {
		return -1, err
	}
	if replyStr, ok := reply.(string); ok && strings.ToLower(replyStr) == "ok" {
		return 1, nil
	}
	//return redis.Int64(reply, err)
	r, _ := reply.(int64)
	return r, err
}

func (c *Client) Eval(ctx context.Context, src string, keyCount int, keysAndArgs []interface{}) (interface{}, error) {
	args := make([]interface{}, 2+len(keysAndArgs))
	args[0] = src
	args[1] = keyCount
	copy(args[2:], keysAndArgs)
	conn, err := c.pool.GetContext(ctx)
	if err != nil {
		return -1, err
	}
	defer conn.Close()
	return conn.Do("EVAL", args...)
}
