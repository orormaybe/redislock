package redislock

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type RedLock struct {
	locks        []*RedisLock
	successLocks []*RedisLock
	RedLockOptions
}

func NewRedLock(key string, confs []*SingleNodeConf, opts ...RedLockOption) (*RedLock, error) {
	// 3 个节点以上，红锁才有意义
	if len(confs) < 3 {
		return nil, errors.New("can not use redLock less than 3 nodes")
	}
	r := RedLock{}
	for _, opt := range opts {
		opt(&r.RedLockOptions)
	}
	repairRedLock(&r.RedLockOptions)
	// 要求所有节点累计的超时阈值要小于分布式锁过期时间的十分之一
	if r.expireDuration > 0 && time.Duration(len(confs))*r.singleNodesTimeout*10 > r.expireDuration {
		return nil, errors.New("expire thresholds of single node is too long")
	}
	r.locks = make([]*RedisLock, 0, len(confs))
	r.successLocks = make([]*RedisLock, 0, len(confs))
	for _, conf := range confs {
		client := NewClient(conf.Network, conf.Address, conf.Password)
		r.locks = append(r.locks, NewRedisLock(key, client, WithExpireSeconds(int64(r.expireDuration.Seconds()))))
	}
	return &r, nil
}

func (r *RedLock) Lock(ctx context.Context) error {
	successCnt := 0
	for _, lock := range r.locks {
		startTime := time.Now()
		err := lock.Lock(ctx)
		cost := time.Since(startTime)
		if err == nil {
			r.successLocks = append(r.successLocks, lock)
		}
		if err == nil && cost <= r.singleNodesTimeout {
			successCnt++
		}
	}
	if successCnt < len(r.locks)>>1+1 {
		for _, lock := range r.successLocks {
			err := lock.Unlock(ctx)
			if err != nil {
				return fmt.Errorf("unlock failed , lock %w", err)
			}
		}
		return errors.New("lock failed")
	}
	return nil
}

// 解锁时，对所有节点广播解锁
func (r *RedLock) Unlock(ctx context.Context) error {
	var err error
	for _, lock := range r.successLocks {
		if _err := lock.Unlock(ctx); _err != nil {
			if err == nil {
				err = _err
			}
		}

	}
	return err

}
