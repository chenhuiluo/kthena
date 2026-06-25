# Kthena Networking Chart

This chart deploys the Kthena networking components, including the kthena-router and webhook.

## Configuration

### Kthena Router

The kthena-router is the main component that handles serving requests and provides fairness scheduling.

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

#### Fairness Scheduling Configuration

The fairness scheduling system ensures equitable resource allocation among users based on their token usage history.

```yaml
kthenaRouter:
  fairness:
    # Enable fairness scheduling (default: false)
    enabled: true
    
    # Sliding window duration for token tracking (default: "1h")
    # Valid formats: 1m, 5m, 10m, 30m, 1h
    windowSize: "10m"
    
    # Token weights for priority calculation
    # Input token weight (default: 1.0)
    inputTokenWeight: 1.0
    
    # Output token weight (default: 2.0)
    # Typically higher than input weight due to generation cost
    outputTokenWeight: 2.0
```

#### Configuration Parameters

| Parameter                                         | Type    | Default          | Description                                                     |
| ------------------------------------------------- | ------- | ---------------- | --------------------------------------------------------------- |
| `kthenaRouter.fairness.enabled`                   | boolean | `false`          | Enable fairness scheduling                                      |
| `kthenaRouter.fairness.windowSize`                | string  | `"5m"`           | Sliding window duration (1m-1h)                                 |
| `kthenaRouter.fairness.inputTokenWeight`          | float   | `1.0`            | Weight for input tokens (≥0)                                    |
| `kthenaRouter.fairness.outputTokenWeight`         | float   | `2.0`            | Weight for output tokens (≥0)                                   |
| `kthenaRouter.fairness.maxConcurrent`             | int     | `0`              | Global total inflight limit (`0` uses the router default of 16) |
| `kthenaRouter.fairness.sessionBoost.enabled`      | boolean | `false`          | Enable session-boost mode on the fairness queue                 |
| `kthenaRouter.fairness.sessionBoost.header`       | string  | `"X-Session-ID"` | HTTP header used to identify conversation sessions              |
| `kthenaRouter.fairness.sessionBoost.maxSessions`  | int     | `4096`           | Max recently-completed sessions kept warm (LRU-evicted)         |
| `kthenaRouter.fairness.sessionBoost.gracePeriod`  | string  | `"0s"`           | Wait time for a same-session follow-up (disabled by default)    |
| `kthenaRouter.fairness.sessionBoost.pollInterval` | string  | `"100ms"`        | Interval for polling backend pod metrics                        |

#### Session Boost Configuration

Session boost is a **mode of the fairness queue** that optimizes multi-turn conversation
latency by prioritizing follow-up requests from the same session (maximizing prefix cache
hits). It requires `fairness.enabled: true` and switches the queue from per-user fair
queuing to session-aware boosting (the two modes are mutually exclusive).

```yaml
kthenaRouter:
  fairness:
    enabled: true               # Required: session boost is a mode of the fairness queue
    sessionBoost:
      enabled: true
      header: "X-Session-ID"
      maxSessions: 4096         # LRU cache of recently-completed sessions kept warm
```

#### Configuration Scenarios

##### Development Environment
```yaml
kthenaRouter:
  fairness:
    enabled: true
    windowSize: "2m"          # Short window for quick feedback
    inputTokenWeight: 1.0     # Equal weights for simplicity
    outputTokenWeight: 1.0
```

##### Production Environment
```yaml
kthenaRouter:
  fairness:
    enabled: true
    windowSize: "10m"         # Balanced window size
    inputTokenWeight: 1.0     # Realistic cost ratios
    outputTokenWeight: 2.5
```

##### Cost-Sensitive Environment
```yaml
kthenaRouter:
  fairness:
    enabled: true
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

### With Fairness Scheduling
```bash
helm install kthena ./charts/kthena \
  --set networking.kthenaRouter.fairness.enabled=true \
  --set networking.kthenaRouter.fairness.windowSize=10m \
  --set networking.kthenaRouter.fairness.outputTokenWeight=3.0
```
