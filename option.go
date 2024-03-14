package redislock

import "time"

// 红锁中每个节点默认的处理超时时间为 50 ms
const DefaultSingleLockTimeout = 50 * time.Millisecond

const (
	// 默认连接池超过 10 s 释放连接
	DefaultIdleTimeoutSeconds = 10
	// 默认最大激活连接数
	DefaultMaxActive = 100
	// 默认最大空闲连接数
	DefaultMaxIdle = 20

	// 默认的分布式锁过期时间
	DefaultLockExpireSeconds = 30
	// 看门狗工作时间间隙
	WatchDogWorkStepSeconds = 10
)

type ClientOptions struct {
	maxIdle            int
	idleTimeoutSeconds int
	maxActive          int
	wait               bool
	// 必填参数
	network  string
	address  string
	password string
}

type ClientOption func(opts *ClientOptions)

func WithMaxIdle(i int) ClientOption {
	return func(opts *ClientOptions) {
		opts.maxIdle = i
	}
}

func WithIdleTimeoutSeconds(i int) ClientOption {
	return func(opts *ClientOptions) {
		opts.idleTimeoutSeconds = i
	}
}

func WithMaxActive(i int) ClientOption {
	return func(opts *ClientOptions) {
		opts.maxActive = i
	}
}

func WithWait(w bool) ClientOption {
	return func(opts *ClientOptions) {
		opts.wait = w
	}
}

func repairClient(opts *ClientOptions) {
	if opts.maxActive < 0 {
		opts.maxActive = DefaultMaxActive
	}
	if opts.maxIdle < 0 {
		opts.maxIdle = DefaultMaxIdle
	}
	if opts.idleTimeoutSeconds < 0 {
		opts.idleTimeoutSeconds = DefaultIdleTimeoutSeconds
	}

}

type LockOptions struct {
	isBlock             bool
	blockWaitingSeconds int64
	expireSeconds       int64
	watchDogMode        bool
}

type LockOption func(opts *LockOptions)

func WithBlockWaitingSeconds(sec int64) LockOption {
	return func(opts *LockOptions) {
		opts.blockWaitingSeconds = sec
	}
}

func WithExpireSeconds(sec int64) LockOption {
	return func(opts *LockOptions) {
		opts.expireSeconds = sec
	}
}

func WithBlock(block bool) LockOption {
	return func(opts *LockOptions) {
		opts.isBlock = block
	}
}

func repairLock(opts *LockOptions) {
	if opts.isBlock && opts.blockWaitingSeconds <= 0 {
		// 默认阻塞等待时间上限为 5 秒
		opts.blockWaitingSeconds = 5
	}

	if opts.expireSeconds > 0 {
		return
	}
	//倘若用户未显式指定锁的过期时间或者指定了一个负值，则锁会自动激活看门狗模式.

	opts.expireSeconds = DefaultLockExpireSeconds
	opts.watchDogMode = true

	// 分布式锁默认超时时间为 30 秒
	if opts.expireSeconds <= 0 {
		opts.expireSeconds = 30
	}
}

type RedLockOptions struct {
	singleNodesTimeout time.Duration
	expireDuration     time.Duration
}

type RedLockOption func(opts *RedLockOptions)

func WithSingleNodesTimeout(d time.Duration) RedLockOption {
	return func(opts *RedLockOptions) {
		opts.singleNodesTimeout = d
	}
}

func WithExpireDuration(d time.Duration) RedLockOption {
	return func(opts *RedLockOptions) {
		opts.expireDuration = d
	}
}

type SingleNodeConf struct {
	Network  string
	Address  string
	Password string
	Opts     []ClientOption
}

func repairRedLock(o *RedLockOptions) {
	if o.singleNodesTimeout <= 0 {
		o.singleNodesTimeout = DefaultSingleLockTimeout
	}
}
