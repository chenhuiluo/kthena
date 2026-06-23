/*
Copyright The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package datastore

import (
	"context"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// This file extends RequestPriorityQueue with session-boost behavior. When a
// queue is constructed with FairnessQueueConfig.SessionBoostEnabled, the shared
// priority-queue framework in fairness_queue.go reuses the same heap, push/pop,
// cancellation and shutdown logic, while the ordering and dequeue strategy below
// replace per-user fairness with session-aware boosting for prefix-cache reuse.

// BackendWaitingChecker is a function that checks whether the backend pods
// have capacity to accept new requests. It returns true when at least one pod
// has an empty waiting queue (i.e. RequestWaitingNum == 0), meaning the backend
// can accept a new request without queuing.
type BackendWaitingChecker func() bool

// SessionTracker tracks recently completed sessions for priority boosting.
// It maps correlation IDs to their last completion time, allowing follow-up
// requests in the same session to be prioritized for prefix cache utilization.
type SessionTracker struct {
	mu       sync.RWMutex
	sessions map[string]time.Time // sessionID -> last completion time
	ttl      time.Duration
}

// NewSessionTracker creates a new session tracker with the given TTL.
func NewSessionTracker(ttl time.Duration) *SessionTracker {
	return &SessionTracker{
		sessions: make(map[string]time.Time),
		ttl:      ttl,
	}
}

// MarkCompleted records that a request from the given session has completed.
func (st *SessionTracker) MarkCompleted(sessionID string) {
	if sessionID == "" {
		return
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	st.sessions[sessionID] = time.Now()
}

// HasRecentCompletion checks if the given session ID has a completion within the TTL window.
func (st *SessionTracker) HasRecentCompletion(sessionID string) bool {
	if sessionID == "" {
		return false
	}
	st.mu.RLock()
	defer st.mu.RUnlock()
	completionTime, exists := st.sessions[sessionID]
	if !exists {
		return false
	}
	return time.Since(completionTime) <= st.ttl
}

// Cleanup removes expired sessions. Should be called periodically.
func (st *SessionTracker) Cleanup() {
	st.mu.Lock()
	defer st.mu.Unlock()
	now := time.Now()
	expired := 0
	for id, t := range st.sessions {
		if now.Sub(t) > st.ttl {
			delete(st.sessions, id)
			expired++
		}
	}
	if expired > 0 {
		klog.V(4).Infof("[SessionTracker] cleanup: removed %d expired sessions, remaining=%d", expired, len(st.sessions))
	}
}

// ActiveSessions returns the number of sessions currently tracked.
func (st *SessionTracker) ActiveSessions() int {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return len(st.sessions)
}

// MarkSessionCompleted records that a request from the given session has completed,
// enabling priority boosting for follow-up requests in the same session. No-op when
// the queue is not in session-boost mode.
func (pq *RequestPriorityQueue) MarkSessionCompleted(sessionID string) {
	if pq.sessionTracker != nil {
		pq.sessionTracker.MarkCompleted(sessionID)
	}
}

// GetSessionTracker returns the session tracker, or nil if session boost is disabled.
func (pq *RequestPriorityQueue) GetSessionTracker() *SessionTracker {
	return pq.sessionTracker
}

// GetInflightCount returns the current number of inflight requests in session-boost mode.
func (pq *RequestPriorityQueue) GetInflightCount() int64 {
	return pq.inflightCount.Load()
}

// runSessionBoostMode is the session-boost dequeue loop. It uses backend backpressure
// when a checker is configured, otherwise dequeues directly (no rate limiting).
func (pq *RequestPriorityQueue) runSessionBoostMode(ctx context.Context) {
	// Start session tracker cleanup goroutine
	go pq.runSessionCleanup(ctx)

	if pq.backendChecker != nil {
		pq.runBackpressureMode(ctx)
		return
	}
	pq.runDirectMode(ctx)
}

// admitSessionBoost marks a request as inflight, installs its release callback and
// unblocks the waiting caller by closing its NotifyChan.
func (pq *RequestPriorityQueue) admitSessionBoost(req *Request) {
	pq.inflightCount.Add(1)
	releaseOnce := sync.Once{}
	req.Release = func() {
		releaseOnce.Do(func() {
			pq.inflightCount.Add(-1)
			select {
			case pq.releaseCh <- struct{}{}:
			default:
			}
			pq.metricDecInflight(req.ModelName)
		})
	}
	pq.metricIncInflight(req.ModelName)
	if req.NotifyChan != nil {
		close(req.NotifyChan)
	}
}

// runDirectMode dequeues requests as fast as they arrive with no rate limiting.
func (pq *RequestPriorityQueue) runDirectMode(ctx context.Context) {
	for {
		req, err := pq.popWhenAvailable(ctx)
		if err != nil {
			return
		}
		if req == nil || req.NotifyChan == nil {
			continue
		}
		if req.isCancelled() {
			continue
		}
		pq.admitSessionBoost(req)
	}
}

// runSessionCleanup periodically cleans up expired sessions from the session tracker.
func (pq *RequestPriorityQueue) runSessionCleanup(ctx context.Context) {
	cleanupInterval := pq.config.SessionBoostTTL
	if cleanupInterval < 10*time.Second {
		cleanupInterval = 10 * time.Second
	}
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-pq.stopCh:
			return
		case <-ticker.C:
			if pq.sessionTracker != nil {
				pq.sessionTracker.Cleanup()
			}
		}
	}
}

// runBackpressureMode dequeues requests only when backend pods have capacity.
// Uses two-level admission control:
//  1. Inflight limit: at most MaxConcurrent requests in flight across all backends.
//  2. Backend metrics check: at least one pod reports capacity available.
//
// Session Grace Period: When SessionBoostGracePeriod > 0, a release event triggers
// a short wait before dequeuing to give the same session time to submit a follow-up
// request that can leverage prefix cache.
func (pq *RequestPriorityQueue) runBackpressureMode(ctx context.Context) {
	pollInterval := pq.config.BackpressurePollInterval
	if pollInterval <= 0 {
		pollInterval = 100 * time.Millisecond
	}
	klog.V(4).Infof("[SessionBoost] starting backpressure dequeue loop, poll_interval=%v, gracePeriod=%v",
		pollInterval, pq.config.SessionBoostGracePeriod)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	if pq.config.SessionBoostGracePeriod > 0 {
		pq.runBackpressureWithGrace(ctx, ticker)
	} else {
		pq.runBackpressureNoGrace(ctx, ticker)
	}
}

// runBackpressureNoGrace is the fast path when grace period is disabled (default).
// Listens on notifyCh for immediate dequeue of freshly enqueued requests.
func (pq *RequestPriorityQueue) runBackpressureNoGrace(ctx context.Context, ticker *time.Ticker) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-pq.stopCh:
			return
		case <-pq.releaseCh:
			pq.tryBackpressureDequeue(ctx)
		case <-pq.notifyCh:
			pq.tryBackpressureDequeue(ctx)
		case <-ticker.C:
			pq.tryBackpressureDequeue(ctx)
		}
	}
}

// runBackpressureWithGrace handles the case where grace period is configured.
// Does NOT listen on notifyCh in the main select to avoid racing with the grace
// period logic on releaseCh. The ticker serves as the backstop for new arrivals.
func (pq *RequestPriorityQueue) runBackpressureWithGrace(ctx context.Context, ticker *time.Ticker) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-pq.stopCh:
			return
		case <-pq.releaseCh:
			pq.waitGraceAndDequeue(ctx)
		case <-ticker.C:
			pq.tryBackpressureDequeue(ctx)
		}
	}
}

// isHeadSessionBoosted checks if the highest-priority request in the queue has a session boost.
func (pq *RequestPriorityQueue) isHeadSessionBoosted() bool {
	pq.mu.RLock()
	defer pq.mu.RUnlock()
	if len(pq.heap) == 0 {
		return false
	}
	return pq.heap[0].SessionBoost
}

// waitGraceAndDequeue waits up to SessionBoostGracePeriod for a session-boosted
// request to arrive at the head of the queue.
func (pq *RequestPriorityQueue) waitGraceAndDequeue(ctx context.Context) {
	// Fast path: head is already session-boosted.
	if pq.isHeadSessionBoosted() {
		klog.V(4).Info("[SessionBoost] grace: head already boosted, skipping wait")
		pq.tryBackpressureDequeue(ctx)
		return
	}

	klog.V(4).Infof("[SessionBoost] grace: starting grace period %v", pq.config.SessionBoostGracePeriod)
	timer := time.NewTimer(pq.config.SessionBoostGracePeriod)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-pq.stopCh:
			return
		case <-pq.notifyCh:
			if pq.isHeadSessionBoosted() {
				klog.V(4).Info("[SessionBoost] grace period: session-boosted request arrived, dequeuing immediately")
				pq.tryBackpressureDequeue(ctx)
				return
			}
		case <-timer.C:
			pq.tryBackpressureDequeue(ctx)
			return
		}
	}
}

// tryBackpressureDequeue admits as many queued requests as possible in one pass,
// stopping when the inflight limit is reached, backends report no capacity, or
// the queue is empty. This avoids the one-request-per-tick bottleneck during
// initial ramp-up and whenever spare capacity exists.
func (pq *RequestPriorityQueue) tryBackpressureDequeue(ctx context.Context) {
	// In session-boost mode, MaxConcurrent is the global (total) inflight limit.
	// Operators size it from the estimated per-pod concurrency and pod count.
	maxInflight := int64(pq.config.MaxConcurrent)
	if maxInflight <= 0 {
		maxInflight = int64(defaultSessionBoostMaxConcurrent)
	}

	for {
		currentInflight := pq.inflightCount.Load()

		if currentInflight >= maxInflight {
			klog.V(4).Infof("[SessionBoost] backpressure: inflight limit reached, inflight=%d maxInflight=%d",
				currentInflight, maxInflight)
			return
		}

		if !pq.backendChecker() {
			pq.mu.RLock()
			queueLen := len(pq.heap)
			pq.mu.RUnlock()
			klog.V(4).Infof("[SessionBoost] backpressure: backend pods busy, holding dequeue. queueLen=%d inflight=%d",
				queueLen, currentInflight)
			return
		}

		pq.mu.RLock()
		queueLen := len(pq.heap)
		pq.mu.RUnlock()
		if queueLen == 0 {
			return
		}

		req, err := pq.popWhenAvailable(ctx)
		if err != nil || req == nil {
			return
		}

		pq.admitSessionBoost(req)

		klog.V(4).Infof("[SessionBoost] backpressure dequeue: user=%s model=%s sessionBoost=%v inflight=%d/%d",
			req.UserID, req.ModelName, req.SessionBoost, pq.inflightCount.Load(), maxInflight)
	}
}
