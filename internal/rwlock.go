package internal

import (
	"context"
	"sync"
	"time"

	minio "github.com/minio/minio/cmd"
)

type StoreFLock struct {
	//inode     meta.Ino
	Owner uint64
	//meta      meta.Meta
	localLock sync.RWMutex
	Readonly  bool
}

// TODO:should get lock from a lock pool
func (j *StoreFLock) GetLock(ctx context.Context, timeout *minio.DynamicTimeout) (newCtx context.Context, timedOutErr error) {
	logger.Infof("%s: enter", GetCurrentFuncName())
	return j.getFlockWithTimeOut(ctx, F_WRLCK, timeout)
}

func (j *StoreFLock) getFlockWithTimeOut(ctx context.Context, ltype uint32, timeout *minio.DynamicTimeout) (context.Context, error) {
	logger.Infof("%s: enter", GetCurrentFuncName())
	if j.Readonly {
		return ctx, nil
	}
	start := time.Now()
	deadline := start.Add(timeout.Timeout())
	lockStr := "write"

	var getLockFunc func() bool
	//var unlockFunc func()
	var getLock bool
	if ltype == F_RDLCK {
		getLockFunc = j.localLock.TryRLock
		//unlockFunc = j.localLock.RUnlock
		lockStr = "read"
	} else {
		getLockFunc = j.localLock.TryLock
		//unlockFunc = j.localLock.Unlock
	}

	for {
		getLock = getLockFunc()
		if getLock {
			break
		}
		if time.Now().After(deadline) {
			timeout.LogFailure()
			logger.Errorf("get %s lock timed out", lockStr)
			return ctx, minio.OperationTimedOut{}
		}
		time.Sleep(5 * time.Millisecond)
	}

	/*for {
		if errno := j.meta.Flock(mctx, j.inode, j.owner, ltype, false); errno != 0 {
			if !errors.Is(errno, syscall.EAGAIN) {
				logger.Errorf("failed to get %s lock for inode %d by owner %d, error : %s", lockStr, j.inode, j.owner, errno)
			}
		} else {
			timeout.LogSuccess(time.Since(start))
			return ctx, nil
		}

		if time.Now().After(deadline) {
			unlockFunc()
			timeout.LogFailure()
			logger.Errorf("get %s lock timed out ino:%d", lockStr, j.inode)
			return ctx, minio.OperationTimedOut{}
		}
		time.Sleep(5 * time.Millisecond)
	}*/
	return ctx, nil
}

func (j *StoreFLock) Unlock() {
	logger.Infof("%s: enter", GetCurrentFuncName())
	if j.Readonly {
		return
	}
	/*if errno := j.meta.Flock(mctx, j.inode, j.owner, meta.F_UNLCK, true); errno != 0 {
		logger.Errorf("failed to release lock for inode %d by owner %d, error : %s", j.inode, j.owner, errno)
	}*/
	j.localLock.Unlock()
}

func (j *StoreFLock) GetRLock(ctx context.Context, timeout *minio.DynamicTimeout) (newCtx context.Context, timedOutErr error) {
	logger.Infof("%s: enter", GetCurrentFuncName())
	return j.getFlockWithTimeOut(ctx, F_RDLCK, timeout)
}

func (j *StoreFLock) RUnlock() {
	logger.Infof("%s: enter", GetCurrentFuncName())
	if j.Readonly {
		return
	}
	/*
		if errno := j.meta.Flock(mctx, j.inode, j.owner, meta.F_UNLCK, true); errno != 0 {
			logger.Errorf("failed to release lock for inode %d by owner %d, error : %s", j.inode, j.owner, errno)
		}*/

	j.localLock.RUnlock()
}

// ----------------------------
type DynamicTimeout struct {
	timeout time.Duration
}

func (dt *DynamicTimeout) Get() time.Duration {
	return dt.timeout
}

type StoreRWLocker struct {
	mu sync.RWMutex
}

func (l *StoreRWLocker) GetLock(ctx context.Context, timeout *DynamicTimeout) (context.Context, error) {
	logger.Infof("%s: enter", GetCurrentFuncName())
	var ctxWithTimeout context.Context
	var cancel context.CancelFunc

	if timeout != nil && timeout.Get() > 0 {
		ctxWithTimeout, cancel = context.WithTimeout(ctx, timeout.Get())
	} else {
		ctxWithTimeout, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	ch := make(chan struct{})
	go func() {
		l.mu.Lock()
		close(ch)
	}()

	select {
	case <-ch:
		return ctxWithTimeout, nil
	case <-ctxWithTimeout.Done():
		return ctxWithTimeout, ctxWithTimeout.Err()
	}
}

func (l *StoreRWLocker) Unlock() {
	logger.Infof("%s: enter", GetCurrentFuncName())
	l.mu.Unlock()
}

func (l *StoreRWLocker) GetRLock(ctx context.Context, timeout *DynamicTimeout) (context.Context, error) {
	logger.Infof("%s: enter", GetCurrentFuncName())

	var ctxWithTimeout context.Context
	var cancel context.CancelFunc

	if timeout != nil && timeout.Get() > 0 {
		ctxWithTimeout, cancel = context.WithTimeout(ctx, timeout.Get())
	} else {
		ctxWithTimeout, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	ch := make(chan struct{})
	go func() {
		l.mu.RLock()
		close(ch)
	}()

	select {
	case <-ch:
		return ctxWithTimeout, nil
	case <-ctxWithTimeout.Done():
		return ctxWithTimeout, ctxWithTimeout.Err()
	}
}

func (l *StoreRWLocker) RUnlock() {
	logger.Infof("%s: enter", GetCurrentFuncName())
	l.mu.RUnlock()
}
