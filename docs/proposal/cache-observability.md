---
title: Observability for prefix-cache and kvcache-aware Score Plugins
authors:
  - "@kube-gopher"
reviewers:
  - TBD
approvers:
  - TBD
creation-date: 2026-06-13
---

## Observability for prefix-cache and kvcache-aware Score Plugins

### Summary

The kthena-router scheduler relies on two cache-oriented score plugins — `prefix-cache` (`pkg/kthena-router/scheduler/plugins/prefix.go`) and `kvcache-aware` (`pkg/kthena-router/scheduler/plugins/kvcache_aware.go`) — to steer requests toward pods that are likely to have a warm cache. Both plugins make decisions whose quality is invisible at runtime: the only signal today is `klog.V(4)` output, which is impractical during load testing and offers no aggregated or historical view.

This proposal adds Prometheus instrumentation to both plugins, exported through the router's existing `/metrics` endpoint, plus a sample Grafana dashboard. The metrics cover cache hit/miss behaviour, match-length distribution, internal latency breakdown (Redis, tokenization), error rates, and cache occupancy/eviction. They are registered through the existing central `metrics.Metrics` struct (`pkg/kthena-router/metrics/metrics.go`) so that naming, labelling, and registration stay consistent with the rest of the router.

### Motivation

Both plugins perform caching/matching logic that critically affects scheduling quality, yet expose no runtime telemetry:

- **`prefix-cache`** is fundamentally a cache. Its effectiveness is defined by hit rate, match length, occupancy, and eviction pressure — none of which are observable.
- **`kvcache-aware`** depends on a tokenizer round-trip and batched Redis lookups (`kvcache_aware.go:204-230`) for block matching. Tokenizer and Redis latency directly bound router throughput and scoring accuracy, but are only ever logged.

Without telemetry it is difficult to (1) evaluate plugin effectiveness under load, (2) locate performance bottlenecks (Redis latency, tokenizer latency), and (3) tune configuration parameters (`blockSizeToHash`, `maxBlocksToMatch`, cache capacity).

#### Goals

- Export Prometheus metrics for both plugins via the router's existing `/metrics` endpoint.
- Make cache hit rate, match length, internal latency, and error rate queryable and aggregatable, labelled by `model`.
- Reuse existing router metric infrastructure (central `Metrics` struct + `promauto`) and naming conventions (`kthena_router_*` prefix).
- Ship a sample Grafana dashboard for load-test analysis.
- Introduce no measurable regression in `Score()` latency.

#### Non-Goals

- Per-request distributed tracing (OpenTelemetry spans). Listed as future work.
- Restructuring the prefix store or the Redis key schema.
- A `pod`-level label on any metric (rejected on cardinality grounds; see Risks).
- Instrumenting other score plugins (`gpu`, `least-latency`, `least-request`, `lora-affinity`); they are already covered adequately by the generic per-plugin duration metric and are out of scope here.

### Proposal

Add two groups of plugin-scoped metrics, all prefixed `kthena_router_` and labelled with `model` where applicable, recorded from within each plugin. Hit/miss/match/latency metrics are recorded on the request path through the `MetricsRecorder` already available on `framework.Context`; occupancy/eviction metrics are maintained out-of-band (pod deletion and LRU eviction run outside any request) against the global `metrics.DefaultMetrics`.

A key design constraint discovered during review: total `Score()` duration is **already exported** generically as `kthena_router_scheduler_plugin_duration_seconds{plugin,type="score"}`, recorded for every score plugin at `scheduler_impl.go:217-224`. This proposal therefore does **not** add a per-plugin total-Score histogram (that would duplicate it) and instead instruments only the sub-phases the generic metric cannot break down.

#### Metrics: `prefix-cache`

| Metric                                          | Type     | Labels  | Description                                                                                  |
|------------------------------------------------|----------|---------|----------------------------------------------------------------------------------------------|
| `kthena_router_prefix_cache_hits_total`        | Counter  | `model` | Number of `Score()` calls in which at least one candidate pod had a matching prefix. Incremented by exactly 1 per hit event — independent of how many pods matched or how many blocks matched. |
| `kthena_router_prefix_cache_misses_total`      | Counter  | `model` | Number of `Score()` calls that hashed a non-empty prompt but found no matching pod. `hits + misses` is the number of real lookups; `hits / (hits + misses)` is the hit rate. |
| `kthena_router_prefix_cache_blocks_matched`    | Histogram| `model` | Per `Score()` call, the longest prefix match length (in blocks) among candidate pods — one observation per call, `0` on a miss. Measures how much prefix was reused, complementing the binary hit/miss counters. |
| `kthena_router_prefix_cache_evictions_total`   | Counter  | `model` | Number of cached hash entries evicted from a per-pod LRU due to capacity pressure. Excludes removals caused by pod deletion (see Notes). |
| `kthena_router_prefix_cache_entries`           | Gauge    | —       | Current total cached hash entries, summed across all per-pod LRUs at scrape time. Bounded by `(#pods with entries) × MaxHashCacheSize`; once every pod LRU is full the value plateaus (1-for-1 eviction) and changes only as pods are added or deleted. |

#### Metrics: `kvcache-aware`

| Metric                                                 | Type     | Labels                | Description                                                                                    |
|--------------------------------------------------------|----------|-----------------------|------------------------------------------------------------------------------------------------|
| `kthena_router_kvcache_aware_hits_total`               | Counter  | `model`               | Number of `Score()` calls in which at least one candidate pod matched ≥1 KV block. Incremented by exactly 1 per hit event — independent of how many pods or blocks matched. |
| `kthena_router_kvcache_aware_misses_total`             | Counter  | `model`               | Number of `Score()` calls that produced block hashes but matched zero blocks on any pod. `hits / (hits + misses)` is the hit rate. |
| `kthena_router_kvcache_aware_blocks_matched`           | Histogram| `model`               | Per `Score()` call, the longest contiguous prefix-block match length (`lastMatchedBlock + 1`); one observation per call, `0` on a miss. |
| `kthena_router_kvcache_aware_redis_duration_seconds`   | Histogram| `model`               | Latency of the batched Redis block-lookup performed during a `Score()` call. |
| `kthena_router_kvcache_aware_tokenize_duration_seconds`| Histogram| `model`               | Latency of tokenizing the prompt during a `Score()` call. |
| `kthena_router_kvcache_aware_errors_total`             | Counter  | `model`, `stage`      | Number of `Score()` calls aborted by an error, labelled by failing stage (`tokenize` or `redis`). Counted separately from misses so transient failures do not distort the hit rate. |

> **Note on `errors_total`:** Several `Score()` paths return `nil` that are *not* cache misses — tokenization failure (`kvcache_aware.go:209-212`) and Redis failure (`kvcache_aware.go:225-227`). Folding these into `misses_total` would corrupt the hit-rate signal, so they are counted separately and labelled by stage. This directly serves the bottleneck-diagnosis goal.

#### Grafana Dashboard

Ship a sample dashboard (JSON) under `docs/proposal/images/` or `examples/observability/` visualising, per model: hit rate (`rate(hits)/rate(hits+misses)`), match-length distribution, Redis and tokenizer latency quantiles, error rate by stage, and prefix-cache occupancy/eviction trend.

### Notes/Constraints

- **Miss definition excludes non-attempts.** Empty/nil prompt and "no hashes generated" early returns (`prefix.go:164`, `kvcache_aware.go:198-220`) count as neither hit nor miss. Only a genuine lookup that produced zero matches is a miss.
- **Two eviction paths exist and only one is an eviction.** Capacity eviction fires the LRU `onEvict` callback (`prefix_store.go:201-204`); pod deletion removes entries directly via `onHashEvicted` (`prefix_store.go:97-124`), bypassing that callback. `evictions_total` counts **only** capacity evictions. Both paths shrink the pod LRUs, so both are reflected in the `entries` gauge (see below).
- **`entries` is computed by a scrape-time scan, not a maintained counter.** The gauge is backed by a `GaugeFunc` whose provider is `ModelPrefixStore.EntryCount` (registered via `SetPrefixCacheEntriesProvider` in `NewPrefixCache`). At scrape time it takes `podHashesMu.RLock()` and sums each pod LRU's `Len()`. This trades a small amount of read-lock contention on the (infrequent) scrape path for keeping insert/eviction/removal paths free of any extra bookkeeping; `Len()` is guarded by the underlying LRU's own lock. A maintained `atomic.Int64` was considered but rejected as added complexity on every hot-path mutation for a value only read on scrape.
- **Occupancy/eviction metrics use the global `DefaultMetrics`,** because eviction and pod deletion run outside request context and have no `MetricsRecorder`.