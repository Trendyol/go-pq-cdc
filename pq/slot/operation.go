package slot

import (
	"context"
	"time"
)

func (s *Slot) initializeLifetime() {
	s.lifetimeOnce.Do(func() {
		s.lifetime, s.cancelLifetime = newSlotLifetime()
		s.operationGate = make(chan struct{}, 1)
	})
}

// newSlotLifetime transfers cancellation ownership to Slot.Close.
func newSlotLifetime() (context.Context, context.CancelFunc) {
	return context.WithCancel(context.Background())
}

// beginOperation bounds both admission and I/O. Shutdown cancels only the
// derived request, never the caller's context.
func (s *Slot) beginOperation(ctx context.Context, timeout time.Duration) (context.Context, func(), error) {
	s.initializeLifetime()
	if s.closed.Load() {
		return nil, nil, ErrorSlotClosed
	}
	var call context.Context
	var cancel context.CancelFunc
	if timeout != 0 {
		call, cancel = context.WithTimeout(ctx, timeout)
	} else {
		call, cancel = context.WithCancel(ctx)
	}
	stop := context.AfterFunc(s.lifetime, cancel)
	cleanup := func() {
		stop()
		cancel()
	}
	select {
	case <-call.Done():
		cleanup()
		return nil, nil, s.operationError(call.Err())
	case s.operationGate <- struct{}{}:
	}
	if call.Err() != nil || s.closed.Load() {
		<-s.operationGate
		cleanup()
		return nil, nil, s.operationError(call.Err())
	}
	return call, func() {
		<-s.operationGate
		cleanup()
	}, nil
}

func (s *Slot) operationError(err error) error {
	if s.closed.Load() {
		return ErrorSlotClosed
	}
	return err
}

func (s *Slot) infoTimeout() time.Duration {
	if s.cfg.InfoTimeout == 0 {
		return DefaultInfoTimeout
	}
	return s.cfg.InfoTimeout
}
