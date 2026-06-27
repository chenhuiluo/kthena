# Kthena Networking Chart

This chart deploys the Kthena networking components, including the kthena-router and webhook.

## Configuration

### Kthena Router

The kthena-router is the main component that handles serving requests and provides priority-queue request scheduling.

#### Basic Configuration

```yaml
kthenaRouter:
  enabled: true
  replicas: 1
  image:
    repository: ghcr.io/volcano-sh/kthena-router
    tag: latest
    pullPolicy: IfNotPresent
```

#### Priority Queue Configuration

The router schedules requests through a per-model **priority queue**. The queue is
strategy-agnostic: it orders requests by a priority value and admits them subject to
capacity. The priority value is produced by a pluggable **priority strategy**:

- **User Fairness** (default): orders requests by each user's recent token usage so
  that no single user dominates a model under contention.
- **Session Boost**: promotes follow-up requests from recently-completed conversation
  sessions to maximize prefix cache hits for multi-turn workloads.

The two strategies are mutually exclusive. Enabling the priority queue
(`priorityQueue.enabled: true`) activates the default user-fairness strategy; setting
`priorityQueue.sessionBoost.enabled: true` switches the queue to the session-boost
strategy.

```yaml
kthenaRouter:
  priorityQueue:
    # Enable the priority queue (default strategy: user fairness)
    enabled: true

    # Queue-level global total inflight limit (0 uses the router default of 16)
    maxConcurrent: 0

    # User-fairness strategy settings (default strategy)
    fairness:
      # Sliding window duration for token tracking (default: "1h")
      # Valid formats: 1m, 5m, 10m, 30m, 1h
      windowSize: "10m"
      # Token weights for priority calculation
      inputTokenWeight: 1.0
      outputTokenWeight: 2.0
```

#### Configuration Parameters

| Parameter                                               | Type    | Default          | Description                                                                 |
| ------------------------------------------------------- | ------- | ---------------- | --------------------------------------------------------------------------- |
| `kthenaRouter.priorityQueue.enabled`                    | boolean | `false`          | Enable the priority queue (default strategy: user fairness)                 |
| `kthenaRouter.priorityQueue.maxConcurrent`              | int     | `0`              | Queue-level global total inflight limit (`0` uses the router default of 16) |
| `kthenaRouter.priorityQueue.fairness.windowSize`        | string  | `"1h"`           | Fairness strategy: sliding window duration (1m-1h)                          |
| `kthenaRouter.priorityQueue.fairness.inputTokenWeight`  | float   | `1.0`            | Fairness strategy: weight for input tokens (≥0)                             |
| `kthenaRouter.priorityQueue.fairness.outputTokenWeight` | float   | `2.0`            | Fairness strategy: weight for output tokens (≥0)                            |
| `kthenaRouter.priorityQueue.sessionBoost.enabled`       | boolean | `false`          | Switch the priority queue to the session-boost strategy                     |
| `kthenaRouter.priorityQueue.sessionBoost.header`        | string  | `"X-Session-ID"` | HTTP header used to identify conversation sessions                          |
| `kthenaRouter.priorityQueue.sessionBoost.maxSessions`   | int     | `4096`           | Max recently-completed sessions kept warm (LRU-evicted)                     |
| `kthenaRouter.priorityQueue.sessionBoost.gracePeriod`   | string  | `"0s"`           | Wait time for a same-session follow-up (disabled by default)                |
| `kthenaRouter.priorityQueue.sessionBoost.pollInterval`  | string  | `"100ms"`        | Interval for polling backend pod metrics                                    |

#### Session Boost Configuration

Session boost is the priority queue's alternative **priority strategy** that optimizes
multi-turn conversation latency by prioritizing follow-up requests from the same session
(maximizing prefix cache hits). It requires `priorityQueue.enabled: true` and switches the
queue from the default user-fairness strategy to session-aware boosting (the two strategies
are mutually exclusive).

```yaml
kthenaRouter:
  priorityQueue:
    enabled: true               # Required: session boost is a priority-queue strategy
    sessionBoost:
      enabled: true
      header: "X-Session-ID"
      maxSessions: 4096         # LRU cache of recently-completed sessions kept warm
```

#### Configuration Scenarios

##### Development Environment
```yaml
kthenaRouter:
  priorityQueue:
    enabled: true
    fairness:
      windowSize: "2m"          # Short window for quick feedback
      inputTokenWeight: 1.0     # Equal weights for simplicity
      outputTokenWeight: 1.0
```

##### Production Environment
```yaml
kthenaRouter:
  priorityQueue:
    enabled: true
    fairness:
      windowSize: "10m"         # Balanced window size
      inputTokenWeight: 1.0     # Realistic cost ratios
      outputTokenWeight: 2.5
```

##### Cost-Sensitive Environment
```yaml
kthenaRouter:
  priorityQueue:
    enabled: true
    fairness:
      windowSize: "30m"         # Longer window for stability
      inputTokenWeight: 1.0     # High output weight for cost control
      outputTokenWeight: 4.0
```

### TLS Configuration

```yaml
kthenaRouter:
  tls:
    enabled: true
    dnsName: "your-domain.com"
    secretName: "kthena-router-tls"
```

### Resource Configuration

```yaml
kthenaRouter:
  resource:
    limits:
      cpu: 500m
      memory: 512Mi
    requests:
      cpu: 100m
      memory: 128Mi
```

### Drain Timeout

```yaml
kthenaRouter:
  terminationGracePeriodSeconds: 330
  drainTimeout: 5m
```

| Parameter                                    | Type   | Default | Description                                                             |
| -------------------------------------------- | ------ | ------- | ----------------------------------------------------------------------- |
| `kthenaRouter.terminationGracePeriodSeconds` | int    | `330`   | Pod termination grace period for the router                             |
| `kthenaRouter.drainTimeout`                  | string | `"5m"`  | Time allowed for the router to drain in-flight requests before shutdown |

## Installation

### Basic Installation
```bash
helm install kthena ./charts/kthena
```

### With the Priority Queue (User-Fairness Strategy)
```bash
helm install kthena ./charts/kthena \
  --set networking.kthenaRouter.priorityQueue.enabled=true \
  --set networking.kthenaRouter.priorityQueue.fairness.windowSize=10m \
  --set networking.kthenaRouter.priorityQueue.fairness.outputTokenWeight=3.0
```
