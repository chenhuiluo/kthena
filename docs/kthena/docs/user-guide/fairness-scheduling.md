# Fairness Scheduling

Kthena Router fairness scheduling prevents a single user from dominating a model's serving capacity during periods of contention. Instead of serving requests strictly in arrival order, the router prioritizes users with lower recent usage.

Fairness scheduling is the **default priority strategy** of the router's per-model **priority queue**. This guide explains how the priority queue works, how the user-fairness strategy orders requests, how to enable it, which configuration knobs are available, and how to verify that it is behaving as expected.

## The Priority Queue and Its Strategies

The router schedules requests through a per-model **priority queue**. The queue itself is strategy-agnostic: it orders queued requests by a numeric priority value and admits them to the backends subject to capacity. Everything else the queue provides — admission control, request timeouts, client-disconnect handling, dequeue-time priority refresh, and heap rebuild — is shared regardless of how priority is computed.

How the priority value is computed is decided by a pluggable **priority strategy**:

- **User Fairness** (default, this guide): priority is derived from each user's recent token usage, so users with lower recent usage are served first.
- **Session Boost** (alternative, see [Session Boost Queue](./session-boost)): priority is derived from recent session completions, so follow-up requests in a recently active conversation are promoted to maximize prefix cache reuse.

The two strategies are **mutually exclusive**. Enabling the priority queue (`ENABLE_PRIORITY_QUEUE=true`) activates the default user-fairness strategy described here. Setting `ENABLE_SESSION_BOOST=true` switches the same queue to the session-boost strategy instead.

Because both strategies share the same queue, the queue-level knobs documented below (prefixed `PRIORITY_QUEUE_*`) apply to either strategy, while the `FAIRNESS_*` knobs are specific to the user-fairness strategy.

## Overview

When the priority queue runs with the default user-fairness strategy, the router does the following for each request:

1. Extracts the user identity for the request.
2. Calculates a priority from the user's recent usage for that model.
3. Enqueues the request into a per-model priority queue.
4. Dequeues requests when the queue policy allows them to proceed.
5. Records token usage after the request completes so future requests reflect recent consumption.

The queue uses these rules:

- **Different users**: lower recent usage gets higher priority.
- **Same user**: requests remain FIFO by arrival time.
- **Same priority**: earlier requests win.

Fairness is enforced **per model**. Heavy usage on one model does not currently reduce a user's priority on a different model.

## How Priority Is Calculated

The fairness scheduler tracks recent usage in a sliding window per `(user, model)` pair.

First, the token tracker builds weighted historical usage:

$$
weighted\_tokens = input\_tokens \times inputTokenWeight + output\_tokens \times outputTokenWeight
$$

Then the queue priority is calculated as:

$$
priority = tokenWeight \times weighted\_tokens + requestNumWeight \times requestCount
$$

Lower priority values are served first.

In practice this means:

- Users with lower recent token usage move ahead of heavier users.
- You can optionally include request count in the score for workloads with many small requests.
- Input/output token cost and queue priority weighting are configured separately.

## Queue Behavior

The priority queue currently supports two dequeue modes:

- **QPS mode**: when `PRIORITY_QUEUE_MAX_CONCURRENT=0`, the queue releases requests at a fixed maximum dequeue rate controlled by `PRIORITY_QUEUE_MAX_QPS`.
- **Concurrency-gated mode**: when `PRIORITY_QUEUE_MAX_CONCURRENT>0`, the queue allows only that many in-flight requests through the gate for a model at a time.

The queue also supports the following runtime protections:

- **Request-scoped timeout**: requests waiting too long in the queue time out.
- **Client disconnect handling**: cancelled requests are skipped instead of being sent downstream later.
- **Dequeue-time priority refresh**: when `PRIORITY_QUEUE_REFRESH_RETRIES > 0`, the priority of the candidate request (the heap root) is recalculated against current usage before it is released. If a fresher priority would put it behind another waiting request, it is reinserted and the next candidate is tried instead.
- **Heap rebuild fallback**: when dequeue-time refresh retries are exhausted *and* the current queue depth is at or below `PRIORITY_QUEUE_REBUILD_THRESHOLD`, all queued item priorities are recalculated from current usage and the heap is fully rebuilt. This bounds the staleness of the entire queue while protecting against expensive rebuilds on large queues.

## Prerequisites

- A Kubernetes cluster with Kthena installed.
- A deployed `ModelRoute` and backend `ModelServer` for the model you want to protect.
- A user identity available to the router for each request. Fairness scheduling depends on a resolved `userId`; requests without it cannot be fairly scheduled.

## Enable Fairness Scheduling

The simplest way to enable the priority queue with its default user-fairness strategy is through the Helm values used by the Kthena Router chart.

```yaml
networking:
  kthenaRouter:
    priorityQueue:
      enabled: true
      fairness:
        windowSize: "1h"
        inputTokenWeight: 1.0
        outputTokenWeight: 2.0
```

Apply the change with Helm:

```bash
helm upgrade --install kthena charts/kthena \
  --namespace kthena-system \
  --create-namespace \
  -f your-values.yaml
```

This config enables the priority queue (which defaults to the user-fairness strategy) and sets the token tracking window and token weighting used to accumulate recent usage. You do not need to select a strategy explicitly: user fairness is used unless `sessionBoost.enabled` is set.

## Advanced Configuration

The router supports additional environment variables beyond the basic Helm values above. Queue-level knobs use the `PRIORITY_QUEUE_*` prefix because they apply to whichever strategy is active; the user-fairness strategy weights use the `FAIRNESS_*` prefix. These can be set directly on the `kthena-router` Deployment when you need finer control over dequeue policy or queue scoring.

```yaml
env:
# Priority queue (queue-level, strategy-agnostic)
- name: ENABLE_PRIORITY_QUEUE
  value: "true"
- name: PRIORITY_QUEUE_TIMEOUT
  value: "45s"
- name: PRIORITY_QUEUE_MAX_CONCURRENT
  value: "32"
- name: PRIORITY_QUEUE_MAX_QPS
  value: "100"
- name: PRIORITY_QUEUE_REFRESH_RETRIES
  value: "2"
- name: PRIORITY_QUEUE_REBUILD_THRESHOLD
  value: "64"
# User-fairness strategy (default strategy scoring)
- name: FAIRNESS_PRIORITY_TOKEN_WEIGHT
  value: "1.0"
- name: FAIRNESS_PRIORITY_REQUEST_NUM_WEIGHT
  value: "0.2"
```

## Configuration Reference

### Priority Queue Settings (queue-level)

These settings apply to the priority queue itself and are shared by all strategies.

| Environment Variable               | Purpose                                                              | Default | Notes                                                                      |
| ---------------------------------- | -------------------------------------------------------------------- | ------- | -------------------------------------------------------------------------- |
| `ENABLE_PRIORITY_QUEUE`            | Enables the request priority queue in the router                     | `false` | Global feature switch. When enabled, the default strategy is user fairness |
| `PRIORITY_QUEUE_TIMEOUT`           | Maximum time a request may wait in the priority queue                | `60s`   | Waiting longer returns a timeout to the client                             |
| `PRIORITY_QUEUE_MAX_CONCURRENT`    | Maximum in-flight requests admitted per model through the queue gate | `0`     | `0` disables semaphore mode and falls back to QPS mode                     |
| `PRIORITY_QUEUE_MAX_QPS`           | Maximum dequeue rate in QPS mode                                     | `100`   | Used only when `PRIORITY_QUEUE_MAX_CONCURRENT=0`                           |
| `PRIORITY_QUEUE_REFRESH_RETRIES`   | Max dequeue-time refresh/reinsert attempts before heap rebuild       | `0`     | `0` disables dequeue-time refresh                                          |
| `PRIORITY_QUEUE_REBUILD_THRESHOLD` | Queue size threshold controlling when heap rebuild is allowed        | `64`    | Helps bound rebuild cost                                                   |

### User-Fairness Strategy Settings

These settings are specific to the default user-fairness strategy and control how each user's recent usage is tracked and scored.

| Environment Variable                   | Purpose                                                | Default              | Notes                                                                       |
| -------------------------------------- | ------------------------------------------------------ | -------------------- | --------------------------------------------------------------------------- |
| `FAIRNESS_WINDOW_SIZE`                 | Sliding window used to track recent usage              | runtime default `5m` | The Helm chart default sets this to `1h` when the priority queue is enabled |
| `FAIRNESS_INPUT_TOKEN_WEIGHT`          | Weight applied to input tokens when recording usage    | `1.0`                | Used by the token tracker                                                   |
| `FAIRNESS_OUTPUT_TOKEN_WEIGHT`         | Weight applied to output tokens when recording usage   | `2.0`                | Used by the token tracker                                                   |
| `FAIRNESS_PRIORITY_TOKEN_WEIGHT`       | Weight of tracked token usage in the final queue score | `1.0`                | Multiplies the tracked weighted token total                                 |
| `FAIRNESS_PRIORITY_REQUEST_NUM_WEIGHT` | Weight of request count in the final queue score       | `0.0`                | Enables composite token + request-count priority                            |

## Choosing Good Settings

Start with the defaults unless you have a clear throughput or fairness issue to solve.

Recommended tuning guidance:

- **Latency-sensitive online serving**: use `PRIORITY_QUEUE_MAX_CONCURRENT` so dequeue is tied to actual in-flight capacity instead of a fixed release rate.
- **Stable, simple rollout**: keep `FAIRNESS_PRIORITY_REQUEST_NUM_WEIGHT=0.0` and tune only token weights first.
- **Small-request-heavy workloads**: add a small non-zero `FAIRNESS_PRIORITY_REQUEST_NUM_WEIGHT` so users sending many tiny requests do not dominate the queue.
- **Rapidly changing usage patterns**: enable bounded refresh with `PRIORITY_QUEUE_REFRESH_RETRIES=1` or `2`.
- **Long prompts and expensive generations**: increase `FAIRNESS_OUTPUT_TOKEN_WEIGHT` if generated tokens are materially more expensive than prompt ingestion in your environment.

## Example Scenarios

### 1. Protect a Shared Model From Heavy Users

If user A has consumed far more recent tokens than user B on the same model, user B's next request is likely to run first. This reduces the chance that a single tenant monopolizes the model under load.

### 2. Preserve Order Within One User Session

If the same user sends several requests in sequence, Kthena preserves FIFO order for that user. Fairness applies across users, not by reordering a single user's own requests.

### 3. Match Admission to Backend Capacity

If the backend safely handles only 16 concurrent requests, set `PRIORITY_QUEUE_MAX_CONCURRENT=16`. The queue then becomes capacity-aware instead of pushing requests at a fixed rate that may be too low or too high for the backend.

## Verify Fairness Scheduling

### 1. Check Router Environment

```bash
kubectl -n kthena-system get deployment kthena-router -o yaml | grep -E 'PRIORITY_QUEUE|FAIRNESS'
```

Confirm that the router is running with the priority queue and fairness variables you expect.

### 2. Inspect Prometheus Metrics

Port-forward the router metrics endpoint:

```bash
kubectl -n kthena-system port-forward deploy/kthena-router 8080:8080
```

Then inspect fairness metrics:

```bash
curl -s http://localhost:8080/metrics | grep kthena_router_fairness_queue
```

Key metrics to watch:

- `kthena_router_fairness_queue_size`
- `kthena_router_fairness_queue_duration_seconds`
- `kthena_router_fairness_queue_cancelled_total`
- `kthena_router_fairness_queue_dequeue_total`
- `kthena_router_fairness_queue_inflight`
- `kthena_router_fairness_queue_priority_refresh_total`
- `kthena_router_fairness_queue_heap_rebuild_total`

### 3. Compare Competing Users

Generate concurrent traffic from at least two users against the same model. Under contention, the user with lower recent usage should observe shorter queue wait times than the heavier user.

## Operational Notes

- Fairness is **per model**, not cross-model.
- Token history is **in memory** on each router instance. In a multi-replica router deployment, fairness state is not shared across replicas.
- Requests without a resolved user identity cannot be scheduled fairly.
- Queue timeout, cancellation handling, and priority refresh are router-side behaviors; they do not require changes to `ModelRoute` resources.

## Troubleshooting

### Requests return `missing userId in request body`

The router could not resolve a user identity for the request. Verify your auth and request-processing path so `userId` is consistently available before fairness scheduling runs.

### Fairness is enabled but throughput is lower than expected

If `PRIORITY_QUEUE_MAX_CONCURRENT=0`, the queue runs in fixed-QPS mode. Increase `PRIORITY_QUEUE_MAX_QPS` or switch to concurrency-gated mode with `PRIORITY_QUEUE_MAX_CONCURRENT`.

### Queue wait times remain high even after enabling fairness

Fairness improves distribution during contention, but it does not create capacity. If all backends are saturated, queue wait time will still grow. Combine fairness with autoscaling and observability to identify true capacity limits.

### Priority does not seem to react quickly enough to recent traffic

Reduce `FAIRNESS_WINDOW_SIZE` for more responsiveness, or enable dequeue-time refresh with `PRIORITY_QUEUE_REFRESH_RETRIES`.

## Related Guides

- [Session Boost Queue](./session-boost) — the alternative priority-queue strategy
- [Router Routing](./router-routing)
- [Router Rate Limiting](./rate-limit)
- [Router Observability](./router-observability)