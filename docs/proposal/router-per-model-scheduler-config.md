---
title: Per-ModelServer Scheduler Configuration
authors:
- "@chenhuiluo"
reviewers:
- TBD
approvers:
- TBD

creation-date: 2026-10-28

---

## Per-ModelServer Scheduler Configuration

### Summary

This proposal adds an optional `Scheduler` field to `ModelServer.Spec` that overrides the global router ConfigMap for scheduler plugin selection, plugin weights, and plugin arguments on a per-ModelServer basis. The router maintains a `SchedulerImpl` instance per ModelServer that declares an override, plus one global default built from the ConfigMap; `Schedule` selects the instance using the `ModelServerName` already present in `framework.Context`. ModelServers that omit the field behave exactly as today (global default), so the change is backward compatible.

The motivation is that a single kthena-router instance commonly serves multiple models with very different characteristics — context length, token size, KV-cache behavior, inference engine, and SLA — so one global scheduler configuration cannot be optimal for all of them.

### Motivation

Today all scheduler configuration lives in the single `routerConfiguration` ConfigMap: `scheduler.plugins.Filter/Score` (which plugins are enabled and their weights) and `scheduler.pluginConfig[].args` (per-plugin parameters). The router parses this once at startup (`pkg/kthena-router/router/router.go:168` `ParseRouterConfig`) and constructs a single `SchedulerImpl` instance (`pkg/kthena-router/scheduler/scheduler_impl.go:63-118`) shared by every model. The per-request `framework.Context` carries the model and ModelServer names (`pkg/kthena-router/scheduler/framework/interface.go:29-57`), but the scheduler never branches on them. The ConfigMap is mounted via `subPath` and has no fsnotify/watcher, so updates require a pod restart.

Different models warrant different scheduling strategies:

- A long-context model (e.g. deepseek-v4.1) benefits most from `prefix-cache` with a high weight and a larger `maxBlocksToMatch`; for it, cache hit savings dominate.
- A short conversational model (e.g. glm-5.2) is better served by `least-request` / `least-latency` as the dominant score plugins.
- A code + lora model (e.g. qwen3.8) only benefits from `lora-affinity` when it is enabled at all; for non-lora models the plugin adds overhead with no benefit, yet today it cannot be scoped off for those models.
- `least-latency.TTFTTPOTWeightFactor` should vary with the model's TTFT/TPOT SLA; `prefix-cache.blockSizeToHash` should vary with context length; `kvcache-aware.vllmTokenizerPort` / `sglangTokenizerPort` should vary with the engine. A single global value cannot fit all.

This aligns with established practice: vLLM configures each instance per model (`--max-num-seqs`, `--max-model-len`, `--enable-prefix-caching`); Seldon Core 2 declares per-model requirements; llm-d routes by per-model cache match; and the Kubernetes Gateway API Inference Extension (Beta, 2026-03) exposes per-model scheduling via `InferencePool.endpointPickerConfig` and `InferenceModel.criticality`. The common layering is: **per-model routing/scheduling config lives in CRDs (runtime-mutable); process-level config lives in ConfigMap.** kthena already follows this for `ModelRoute.Spec.RateLimit` and `ModelRoute.Rule.TargetModels[].Weight` (both per-model, hot-reloaded via CRD informer); the gap is specifically the scheduler plugin layer, which is stuck in the ConfigMap.

#### Goals

- Add an optional `Scheduler` field to `ModelServer.Spec` that overrides the global ConfigMap's plugin enablement, weights, and args for that ModelServer.
- Keep behavior unchanged for ModelServers that omit the field (global default).
- Hot-reload per-ModelServer overrides via the existing ModelServer informer (create/update/delete).
- Reuse the `ModelServerName` already in `framework.Context` to select the scheduler; no new request-time lookup primitive.
- Reuse the existing `SchedulerConfiguration` struct for the override so the CRD field mirrors the ConfigMap shape.

#### Non-Goals

- Removing or replacing the global ConfigMap default; it stays as the fallback for ModelServers without an override.
- Introducing per-model token budgets or scheduling priority/criticality. These are valuable (see Gateway API `criticality`, vLLM `max_num_scheduled_tokens`) but are reserved for a follow-up.
- Making fairness / session-boost environment variables per-model. They are process-level today and acceptable as-is for most deployments.
- Changing PD-disaggregation scheduling logic.
- Changing rate limiting or ModelServer-traffic split weights (already per-model).
- Splitting the router into separate data-plane and control-plane processes.

### Proposal

Add an optional `Scheduler *SchedulerConfiguration` field to `ModelServer.Spec`. When non-nil, the router builds a dedicated `SchedulerImpl` for that ModelServer from the override (falling back to the global ConfigMap values for any sub-field the override leaves unset); when nil, the ModelServer uses the single global default, exactly as today.

The router keeps `map[types.NamespacedName]Scheduler` (keyed by ModelServer namespaced name) holding only ModelServers with an override, plus one global default. The single `Schedule` call site (`pkg/kthena-router/router/router.go:668` `r.scheduler.Schedule(ctx, pods)`) resolves the scheduler from `ctx.ModelServerName`: if an override entry exists, use it; otherwise use the global default.

The existing `ModelServerController` already handles ModelServer add/update/delete events (`pkg/kthena-router/controller/modelserver_controller.go`, `syncModelServerHandler`). On update, if the `Scheduler` field changed, the router rebuilds that ModelServer's `SchedulerImpl`; on delete, it drops the entry. This reuses the existing CRD-informer hot-reload path — the same mechanism already used for `TrafficPolicy.ConnectionPool` (transport registry rebuild) and `ModelRoute.Spec.RateLimit`.

Configuration precedence: `ModelServer.Spec.Scheduler` (non-nil) overrides the global ConfigMap default. Within the override, any sub-field left empty falls back to the global value, so an override can tune a single dimension (e.g. only weights) without redeclaring the whole config.

#### User Stories

##### Story 1: Long-context model

An operator runs deepseek-v4.1 with long system prompts. They set `ModelServer.Spec.Scheduler` to enable `prefix-cache` with weight 5 and `prefix-cache.maxBlocksToMatch: 512`, leaving other plugins at the global default. Requests to this ModelServer prefer pods with matching KV-cache prefixes; other models are unaffected.

##### Story 2: Lora vs non-lora

An operator runs qwen3.8 (lora) and glm-5.2 (non-lora) in the same cluster. They set qwen3.8's `Scheduler` to enable `lora-affinity`; glm-5.2 omits the field and never pays the `lora-affinity` overhead. Today both must share one global enable/disable decision.

##### Story 3: Conversational latency

An operator runs glm-5.2 for chat where TTFT matters more than throughput. They set `least-latency.TTFTTPOTWeightFactor: 0.8` on that ModelServer's `Scheduler` override, while a batch ModelServer keeps the global 0.5.

#### Notes/Constraints/Caveats

- **Memory.** Each override builds a `SchedulerImpl` with its own plugin instances and arg cache. Mitigation: only ModelServers with a non-nil `Scheduler` get a dedicated instance; the rest share the single global default. Plugin instances are lightweight (args + score closures); the cost is proportional to the number of overrides, not the number of models.
- **Process-level resources in plugin args.** Some plugin args reference process-level resources (e.g. `kvcache-aware` Redis client, tokenizer ports). A per-ModelServer override may change tuning args (block sizes, weights) but must NOT rebuild process-level clients/connections — those stay shared at the process level. The override applies to scheduling parameters, not to connection lifecycle.
- **ConfigMap no-hot-reload is unaffected.** The global default remains ConfigMap-sourced and restart-only; per-ModelServer overrides are CRD-sourced and informer-hot-reloaded. The two sources are independent.
- **`random` plugin exclusivity.** The existing rule that `random` is mutually exclusive with other score plugins (`conf.go:151-169`) applies per scheduler instance, so an override enabling `random` must not also enable other score plugins — enforced by the same startup validation, adapted to run on override build.

#### Risks and Mitigations

- **Configuration complexity.** Operators now have two config sources (ConfigMap default + per-ModelServer override). Mitigation: the default is unchanged, overrides are opt-in, and precedence is simple (override wins, empty sub-fields fall back). Webhook validation rejects inconsistent overrides (e.g. `random` + other score plugins).
- **Instance growth.** Many ModelServers setting overrides could increase memory. Mitigation: only overrides build instances; a future cap or LRU can be added if needed, but is not required initially.
- **Misuse of plugin args.** A wrong tokenizer port or block size can degrade scheduling silently. Mitigation: admission webhook validates ranges and required combinations; defaults are documented.
- **Security review.** The field is operator-facing config, not user-facing request data; no new attack surface. Existing CRD RBAC for ModelServer edits applies.

### Design Details

- **API.** Add `Scheduler *SchedulerConfiguration` to `ModelServerSpec` (`pkg/apis/networking/v1alpha1/modelserver_types.go`). Reuse the existing `SchedulerConfiguration` struct from `pkg/kthena-router/scheduler/plugins/conf/conf.go` (or a CRD-friendly mirror if the struct lives in the router package and cannot be referenced from APIs — in that case define an identical API-side struct and convert at the router boundary). DeepCopy follows the existing codegen for ModelServerSpec.
- **Scheduler selection.** At `router.go:668`, replace the direct `r.scheduler.Schedule(ctx, pods)` with a lookup: `sched := r.schedulerFor(ctx.ModelServerName); sched.Schedule(ctx, pods)`. `schedulerFor` returns the per-ModelServer instance if present, else the global default.
- **Instance management.** In `ModelServerController.syncModelServerHandler` (`modelserver_controller.go`), on ModelServer add/update with a non-nil `Scheduler`, build and store a `SchedulerImpl` keyed by namespaced name; on update with the field cleared, drop the entry (falls back to global); on delete, drop the entry. Building reuses `scheduler.NewScheduler(store, overrideConfig)` with the same plugin factory registry (`scheduler/factory.go`).
- **Precedence and partial override.** When building an override instance, start from a copy of the global config and apply the override's non-empty sub-fields, so a partial override (e.g. weights only) inherits the rest from the global default. This keeps overrides short and behavior predictable.
- **Validation.** Extend the ModelServer admission webhook (`pkg/kthena-router/webhook/validator.go`) to validate `Scheduler`: plugin names must be registered; score weights > 0; `random` exclusivity; arg ranges (e.g. `TTFTTPOTWeightFactor` in [0,1]). This mirrors the startup validation in `conf.go:180-200`.

#### Test Plan

- **Unit.** `schedulerFor` returns the override instance when present and the global default otherwise; building from a partial override inherits unset sub-fields from the global default; a ModelServer update changing `Scheduler` rebuilds the instance; clearing the field drops the entry (falls back to global); ModelServer delete cleans up the entry.
- **Integration.** Multiple ModelServers with different `Scheduler` weights route requests through their respective instances; a ModelServer without the field routes through the global default and matches today's behavior.
- **Compatibility.** An existing cluster with no `Scheduler` fields set produces identical scheduling decisions to today (global default only), verified by re-running the current scheduler e2e suite unchanged.
- **Webhook.** Invalid overrides (unknown plugin, weight ≤ 0, `random` + other score, out-of-range args) are rejected; valid overrides are accepted.

### Alternatives

- **ConfigMap per-model sections** (`routerConfiguration.yaml` with a `perModel` map). Rejected: the ConfigMap is `subPath`-mounted with no hot-reload, so adding/removing a model requires a pod restart; it also contradicts the "model-level config belongs in CRDs" layering that kthena already follows for RateLimit and TrafficPolicy.
- **Override on `ModelRoute.Spec` instead of `ModelServer.Spec`.** Rejected: scheduling selects among the pods of a ModelServer (backend), so the tuning follows backend characteristics (engine, context, KV cache) which are ModelServer-level, not route-entry-level. One ModelRoute can target multiple ModelServers with different engines; putting the override on ModelRoute would force one strategy onto heterogeneous backends.
- **One ConfigMap per model (multiple ConfigMaps).** Rejected: multiplies ConfigMap management, still has no hot-reload, and still mixes model-level config into a deployment artifact.
- **No override, only better global defaults.** Rejected: a single global value cannot fit models with opposing optimal strategies (prefix-cache-dominant vs least-request-dominant); no default choice is correct for all models.
