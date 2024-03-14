package redislock

import (
	"context"
	"errors"
	"fmt"
	"github.com/orormaybe/redislock/utils"
	"sync/atomic"
	"time"
)

const RedisLockKeyPrefix = "REDIS_LOCK_PREFIX_"

var ErrLockAcquiredByOthers = errors.New("lock is acquired by others")

type RedisLock struct {
	key, token string
	client     LockClient
	LockOptions
	// 看门狗运作标识
	runningDog int32
	// 停止看门狗
	stopDog context.CancelFunc
}

func NewRedisLock(key string, client LockClient, opts ...LockOption) *RedisLock {
	r := RedisLock{
		key:    key,
		token:  utils.GetProcessAndGoroutineIDStr(),
		client: client,
	}
	for _, opt := range opts {
		opt(&r.LockOptions)
	}
	repairLock(&r.LockOptions)
	return &r

}

// 非阻塞模式Lock 加锁.
func (r *RedisLock) Lock(ctx context.Context) error {
	// 不管是不是阻塞模式，都要先获取一次锁
	err := r.tryLock(ctx)
	if err == nil {
		return nil
	}
	// 非阻塞模式加锁失败直接返回错误
	if !r.isBlock {
		return err
	}
	// 判断错误是否可以允许重试，如果是锁被抢占的错误可以重试，不可允许的错误类型则直接返回错误
	if !IsRetryableErr(err) {
		return err
	}
	return r.blockingLock(ctx)

}

func (r *RedisLock) tryLock(ctx context.Context) error {
	//查询锁是否属于自己
	reply, err := r.client.SetNEX(ctx, r.key, r.token, r.expireSeconds)
	if err != nil {
		return err
	}
	if reply != 1 {
		return fmt.Errorf("reply: %d, err: %w", reply, ErrLockAcquiredByOthers)
	}
	return nil
}

func IsRetryableErr(err error) bool {
	return errors.Is(err, ErrLockAcquiredByOthers)
}

func (r *RedisLock) blockingLock(ctx context.Context) error {
	// 阻塞模式等锁时间上限
	timeoutCh := time.After(time.Duration(r.blockWaitingSeconds) * time.Second)
	// 轮询 ticker，每隔 50 ms 尝试取锁一次
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for range ticker.C {
		select {
		// ctx 终止了
		case <-ctx.Done():
			return fmt.Errorf("lock failed, context time out, err: %w", ctx.Err())
		// 阻塞等锁达到上限时间
		case <-timeoutCh:
			return fmt.Errorf("block waiting time out, err: %w", ErrLockAcquiredByOthers)
		default:
		}
		// 尝试取锁
		err := r.tryLock(ctx)
		if err == nil {
			// 加锁成功，返回结果
			return nil
		}
		if !IsRetryableErr(err) {
			return err
		}

	}
	return nil
}

func (r *RedisLock) getLockKey() string {
	return r.key
}

func (r *RedisLock) Unlock(ctx context.Context) error {
	defer func() {
		if r.stopDog != nil {
			//停止看门狗
			r.stopDog()
		}
	}()

	keysAndArgs := []interface{}{r.getLockKey(), r.token}
	reply, err := r.client.Eval(ctx, LuaCheckAndDeleteDistributionLock, 1, keysAndArgs)
	if err != nil {
		return err
	}
	if replyInt := reply.(int64); replyInt != 1 {
		return errors.New("can not unlock without ownership of lock")
	}
	return nil
}

// 启动看门狗
func (r *RedisLock) watchDog(ctx context.Context) {
	// 1. 非看门狗模式，不处理
	if !r.watchDogMode {
		return
	}
	// 2. 确保之前启动的看门狗已经正常回收
	for !atomic.CompareAndSwapInt32(&r.runningDog, 0, 1) {

	}
	ctx, r.stopDog = context.WithCancel(ctx)
	go func() {
		defer func() {
			atomic.StoreInt32(&r.runningDog, 0)
		}()
		r.runWatchDog(ctx)
	}()
}

func (r *RedisLock) runWatchDog(ctx context.Context) {
	ticker := time.NewTicker(WatchDogWorkStepSeconds * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// 看门狗负责在用户未显式解锁时，持续为分布式锁进行续期
		// 通过 lua 脚本，延期之前会确保保证锁仍然属于自己
		// 为避免因为网络延迟而导致锁被提前释放的问题，watch dog 续约时需要把锁的过期时长额外增加 5 s
		_ = r.DelayExpire(ctx, WatchDogWorkStepSeconds+5)
	}

}

func (r *RedisLock) DelayExpire(ctx context.Context, expireSeconds int64) error {
	keysAndArgs := []interface{}{r.getLockKey(), r.token, expireSeconds}
	reply, err := r.client.Eval(ctx, LuaCheckAndExpireDistributionLock, 1, keysAndArgs)
	if err != nil {
		return err
	}
	if ret, _ := reply.(int64); ret != 1 {
		return errors.New("can not expire lock without ownership of lock")
	}
	return nil

}
