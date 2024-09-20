package godisson

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"net"
	"time"
)

var ErrLockNotObtained = errors.New("ErrLockNotObtained")

const UNLOCK_MESSAGE int64 = 0
const READ_UNLOCK_MESSAGE int64 = 1

type RLock struct {
	Key string
	g   *Godisson
}

var _ Locker = (*RLock)(nil)

func newRLock(key string, g *Godisson) *RLock {
	return &RLock{Key: key, g: g}
}

func (r *RLock) Lock() error {
	// 调用 TryLock，默认等待和租期为 -1 表示无限等待，并启用 Watchdog
	return r.TryLock(-1, -1)
}

// TryLock 尝试获取锁
// waitTime 等待时间，单位为毫秒
// leaseTime 锁的过期时间，单位为毫秒，-1 时启用 Watchdog 自动续期
func (r *RLock) TryLock(waitTime int64, leaseTime int64) error {
	wait := waitTime
	startTime := currentTimeMillis()

	// 尝试获取锁
	ttl, err := r.tryAcquire(waitTime, leaseTime)
	if err != nil {
		return err
	}

	// 锁成功获取
	if ttl == 0 {
		return nil
	}

	// 计算剩余等待时间
	wait -= currentTimeMillis() - startTime
	if wait <= 0 {
		return ErrLockNotObtained
	}

	// 订阅锁释放事件
	sub := r.g.c.Subscribe(context.TODO(), r.g.getChannelName(r.Key))
	defer sub.Close()

	timeoutCtx, timeoutCancel := context.WithTimeout(context.TODO(), time.Duration(wait)*time.Millisecond)
	defer timeoutCancel()

	// 等待锁释放的消息
	_, err = sub.ReceiveMessage(timeoutCtx)
	if err != nil {
		return ErrLockNotObtained
	}

	// 再次尝试获取锁
	wait -= currentTimeMillis() - startTime
	for wait > 0 {
		currentTime := currentTimeMillis()
		ttl, err = r.tryAcquire(waitTime, leaseTime)
		if err != nil {
			return err
		}
		if ttl == 0 {
			return nil
		}

		// 等待锁的释放通知
		wait -= currentTimeMillis() - currentTime
		if ttl > 0 && ttl < wait {
			tCtx, _ := context.WithTimeout(context.TODO(), time.Duration(ttl)*time.Millisecond)
			_, err := sub.ReceiveMessage(tCtx)
			if err != nil && !isTemporaryNetError(err) {
				return ErrLockNotObtained
			}
		} else {
			tCtx, _ := context.WithTimeout(context.TODO(), time.Duration(wait)*time.Millisecond)
			_, err := sub.ReceiveMessage(tCtx)
			if err != nil && !isTemporaryNetError(err) {
				return ErrLockNotObtained
			}
		}
		wait -= currentTimeMillis() - currentTime
	}

	return ErrLockNotObtained
}

// tryAcquire 尝试获取锁，并支持 Watchdog 续期机制
func (r *RLock) tryAcquire(waitTime int64, leaseTime int64) (int64, error) {
	goid, err := gid()
	if err != nil {
		return 0, err
	}

	// 使用指定的过期时间或 Watchdog 时间
	if leaseTime != -1 {
		return r.tryAcquireInner(waitTime, leaseTime)
	}

	// 启用 Watchdog 续期
	ttl, err := r.tryAcquireInner(waitTime, r.g.watchDogTimeout.Milliseconds())
	if err != nil {
		return 0, err
	}
	if ttl == 0 {
		r.renewExpirationScheduler(goid)
	}
	return ttl, nil
}

// renewExpirationScheduler 启动 Watchdog 续期任务
func (r *RLock) renewExpirationScheduler(goroutineId uint64) {
	newEntry := NewRenewEntry()
	entryName := r.g.getEntryName(r.Key)
	if oldEntry, ok := r.g.RenewMap.Get(entryName); ok {
		oldEntry.(*RenewEntry).addGoroutineId(goroutineId)
	} else {
		newEntry.addGoroutineId(goroutineId)
		cancel, cancelFunc := context.WithCancel(context.TODO())

		go r.renewExpirationSchedulerGoroutine(cancel, goroutineId)

		newEntry.cancelFunc = cancelFunc
		r.g.RenewMap.Set(entryName, newEntry)
	}
}

// renewExpirationSchedulerGoroutine Watchdog Goroutine 任务，定期续期锁
func (r *RLock) renewExpirationSchedulerGoroutine(cancel context.Context, goid uint64) {
	entryName := r.g.getEntryName(r.Key)
	ticker := time.NewTicker(r.g.watchDogTimeout / 3)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			renew, err := r.renewExpiration(goid)
			if err != nil {
				r.g.RenewMap.Remove(entryName)
				return
			}
			if renew == 0 {
				r.cancelExpirationRenewal(0)
				return
			}
		case <-cancel.Done():
			return
		}
	}
}

// cancelExpirationRenewal 取消锁的 Watchdog 续期
func (r *RLock) cancelExpirationRenewal(goid uint64) {
	entryName := r.g.getEntryName(r.Key)
	entry, ok := r.g.RenewMap.Get(entryName)
	if !ok {
		return
	}
	task := entry.(*RenewEntry)
	if goid != 0 {
		task.removeGoroutineId(goid)
	}
	if goid == 0 || task.hasNoThreads() {
		if task.cancelFunc != nil {
			task.cancelFunc()
			task.cancelFunc = nil
		}
		r.g.RenewMap.Remove(entryName)
	}
}

// tryAcquireInner 通过 Lua 脚本尝试获取锁
func (r *RLock) tryAcquireInner(waitTime int64, leaseTime int64) (int64, error) {
	gid, err := gid()
	if err != nil {
		return 0, err
	}
	lockName := r.getHashKey(gid)
	result, err := r.g.c.Eval(context.TODO(), `
if (redis.call('exists', KEYS[1]) == 0) then
    redis.call('hincrby', KEYS[1], ARGV[2], 1);
    redis.call('pexpire', KEYS[1], ARGV[1]);
    return 0;
end;
if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then
    redis.call('hincrby', KEYS[1], ARGV[2], 1);
    redis.call('pexpire', KEYS[1], ARGV[1]);
    return 0;
end;
return redis.call('pttl', KEYS[1]);
`, []string{r.Key}, leaseTime, lockName).Result()
	if err != nil {
		return 0, err
	}

	if ttl, ok := result.(int64); ok {
		return ttl, nil
	}
	return 0, errors.Errorf("tryAcquireInner result converter to int64 error, value is %v", result)
}

// Unlock 释放锁
func (r *RLock) Unlock() (int64, error) {
	goid, err := gid()
	if err != nil {
		return 0, err
	}

	defer func() {
		r.cancelExpirationRenewal(goid)
	}()

	result, err := r.g.c.Eval(context.Background(), `
if (redis.call('hexists', KEYS[1], ARGV[3]) == 0) then
    return nil;
end;
local counter = redis.call('hincrby', KEYS[1], ARGV[3], -1);
if (counter > 0) then
    redis.call('pexpire', KEYS[1], ARGV[2]);
    return 0;
else
    redis.call('del', KEYS[1]);
    redis.call('publish', KEYS[2], ARGV[1]);
    return 1;
end;
`, []string{r.Key, r.g.getChannelName(r.Key)}, UNLOCK_MESSAGE, DefaultWatchDogTimeout, r.getHashKey(goid)).Result()

	if err != nil {
		return 0, err
	}

	if unlocked, ok := result.(int64); ok {
		return unlocked, nil
	}
	return 0, errors.Errorf("Unlock result converter to int64 error, value is %v", result)
}

// renewExpiration 续期锁的过期时间
func (r *RLock) renewExpiration(gid uint64) (int64, error) {
	result, err := r.g.c.Eval(context.TODO(), `
if (redis.call('hexists', KEYS[1], ARGV[2]) == 1) then
    redis.call('pexpire', KEYS[1], ARGV[1]);
    return 1;
end;
return 0;
`, []string{r.Key}, r.g.watchDogTimeout.Milliseconds(), r.getHashKey(gid)).Result()
	if err != nil {
		return 0, err
	}
	if ttl, ok := result.(int64); ok {
		return ttl, nil
	}
	return 0, errors.Errorf("renewExpiration result converter to int64 error, value is %v", result)
}

// 工具函数 - 处理临时网络错误
func isTemporaryNetError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Temporary()
	}
	return false
}

func (r *RLock) getHashKey(goroutineId uint64) string {
	return fmt.Sprintf("%s:%d", r.g.uuid, goroutineId)
}
