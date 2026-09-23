---
title: Lossless Upgrade for ModelServing
authors:
- "@chenhuiluo" # Authors' GitHub accounts here.
reviewers:
- TBD
approvers:
- TBD

creation-date: 2026-09-23

---

## Lossless Upgrade for ModelServing

### Summary

This proposal adds a drain step to `ModelServing` rolling updates so that an old
Pod is deleted only after its in-flight requests finish (or a timeout fallback
fires), instead of being killed immediately. The goal is zero-downtime,
zero-interruption upgrades: the service keeps serving throughout the update and
in-flight requests are never SIGKILLed.

### Motivation

Today, updating a `ModelServing` (new image, engine config change) drives
`model-serving-controller` to delete old-revision ServingGroups/Roles via
`DeleteCollection` with empty `DeleteOptions`. Old Pods are removed immediately,
with no wait for in-flight requests. LLM inference requests — especially
streaming — often run for tens of seconds to minutes, so an immediate delete
SIGKILLs in-flight work and clients see 502 / connection resets.

A drain step before the delete solves this: stop routing new traffic to the old
Pod, wait for in-flight requests to finish, then delete. The existing rolling
strategy (`maxUnavailable`/`maxSurge`) already keeps enough Pods serving during
the update; the gap is only that the "delete old" step is not graceful.

#### Goals

- Add a drain step before deleting an old Pod during a `ModelServing` rolling
  update, so in-flight requests on the old Pod finish before it is removed.
- Keep serving throughout the update (already guaranteed by `maxUnavailable: 0`
  + `maxSurge: 1`; this proposal does not change the rolling strategy).
- Cover both rollout strategies (`ServingGroupRollingUpdate` and
  `RoleRollingUpdate`), plus scale-down and failure-rebuild, since they all
  converge on the same deletion path.
- Work without Redis or any new CRD field.
- Work for engines the router already adapts (vLLM including ascend, SGLang).

#### Non-Goals

- Drain for engines the router does not support (MindIE): the router does not
  route MindIE traffic, so there is nothing to drain; the timeout fallback still
  ensures the update completes.
- Changing the rolling strategy itself (`maxUnavailable`/`maxSurge`/`partition`).
- Drain as a separate API toggle: the drain timeout flag doubles as the
  enable/disable switch (0 disables).

### Proposal

#### User Stories

##### Story 1

A user updates the model image of a `ModelServing`. During the update, clients
keep calling the model. Without drain, a Pod serving a long streaming request is
killed mid-response; the client sees a broken connection. With drain, the old
Pod stops receiving new requests, finishes its in-flight streaming response,
and is then removed — the client never notices the update.

##### Story 2

A user scales down a `ModelServing` replica. The same drain applies: the Pod about
to be removed finishes its in-flight requests first, so no request is dropped
during scale-down.

#### Notes/Constraints/Caveats

True zero-downtime requires `maxUnavailable: 0` (+ `maxSurge: 1`): a new Pod must
be ready before the old one is removed. With `maxUnavailable: 0` the delete
budget is zero until the surge Pod is ready, so drain does not start until then
— this is correct. `maxSurge: 1` needs headroom for one extra Pod (GPU/NPU) during
the update; clusters must reserve it.

#### Risks and Mitigations

- **Drain gets stuck**: if the router cannot confirm in-flight is zero (engine
  unreachable, metrics scrape failing), drain could hang. Mitigation: a
  `drainTimeout` fallback forces deletion after the timeout, so the update
  always completes; residual in-flight is then bounded by the Pod's
  `terminationGracePeriodSeconds` and preStop hook.
- **Router restart during drain**: the router holds drain state in memory. A
  restart could lose the in-progress drain and let a draining Pod receive new
  traffic. Mitigation: the drain signal is also a Pod annotation, so after a
  restart the router re-applies the draining flag from the annotation.

### Design Details

The controller and router cooperate over Pod annotations, without Redis and
without a new CRD field.

Two annotations carry the drain handshake:

- `modelserving.volcano.sh/traffic-draining` (written by the controller, value
  is an RFC3339 timestamp used to bound the drain wait)
- `modelserving.volcano.sh/traffic-drained` (written by the router, value `true`,
  signals the Pod is safe to delete)

#### Controller

A drain gate is added to the two deletion convergence points,
`deleteServingGroup` and `DeleteRole`. Before deleting, the gate annotates the
Pods with `traffic-draining`, then waits. Drain completes when the router
writes `traffic-drained` onto the Pods, or when the `drainTimeout` elapses as a
fallback. Sitting on the two convergence points means rolling update, scale-down,
and failure-rebuild all go through the same drain path.

`patchTrafficDraining` retries transient errors with the client-go retry backoff
(terminal errors like NotFound/Forbidden/Invalid are not retried); Pods that
still cannot be annotated are force-deleted, so the K8s `DeletionTimestamp`
still removes them from scheduling.

#### Router

The router observes the `traffic-draining` annotation via the Pod informer and
sets an in-memory `draining` flag on the `PodInfo`. Draining Pods stay in the
store (in-flight tracking preserved) but are excluded from all
scheduling-candidate queries. `store.Run` watches engine metrics and, once
in-flight reaches zero, patches `traffic-drained=true` back onto the Pod. The
patch is retried with client-go backoff and is idempotent.

#### Drain progress: engine metrics, not onFlight

Drain completion is judged from engine-reported metrics (`RequestRunningNum` +
`RequestWaitingNum`), not the router's `onFlight` counter. Engine metrics
include direct-to-engine traffic (not just traffic through the router), do not
depend on Redis, and are monotonic after drain (no new traffic reaches the Pod),
so "reaches zero" reliably means in-flight is truly empty. The metrics are
already engine-agnostic through the vllm/sglang adapter layer.

#### Configuration

A `--drain-timeout` flag on `kthena-controller-manager` (default `5m`; `0`
disables drain and reverts to immediate deletion). It is intentionally a flag,
not a CRD field: drain is the correct behavior for rolling update, and the
timeout expresses both "how long to wait" and "whether to wait", avoiding a
separate boolean toggle. Helm exposes it as `controllerManager.drainTimeout`.

Drain writes Pod annotations, which the original code did not do (it only
deleted/watched Pods). The `patch` verb on `pods` is therefore added to both the
controller-manager and router ClusterRoles.

#### Engine Coverage

Drain works for engines the router already adapts: vLLM (including ascend, which
runs the vLLM engine with `vllm:` metrics) and SGLang. MindIE is not supported —
the router does not support MindIE (`BuildModelServer` rejects non-vLLM backends),
so no MindIE traffic flows through the router and there is nothing to drain. The
`drainTimeout` fallback still ensures an update involving MindIE eventually
completes.

#### Test Plan

- Unit tests for the drain gate (both group and role), `patchTrafficDraining`
  retry/force-delete, and the router's `markPodDrainedIfZero`.
- Unit tests for the ServingGroup-outdated check after a controller restart
  rebuilds `SG.Revision` from the first pod seen.
- E2E test for a `ModelServing` rolling update under `RoleRollingUpdate` with
  `maxUnavailable: 0` + `maxSurge: 1`, asserting the old Pod is drained and
  deleted after in-flight finishes and ready replicas converge to
  `spec.replicas`.

### Alternatives

- **`onFlight` counter as the drain signal**: the router already maintains an
  in-flight counter. It was rejected because it only counts traffic through the
  router (not direct-to-engine traffic), and in a multi-replica router without
  Redis each replica only sees its own count and could falsely report zero.
  Engine metrics avoid both problems.
- **preStop hook only**: a preStop hook can delay Pod termination, but it cannot
  stop the router from sending new traffic to the Pod. Drain needs the router to
  actively stop new traffic, which a preStop hook cannot do. preStop remains as a
  secondary safety net.
- **A separate CRD field to enable drain**: rejected because a timeout of 0
  already disables drain, so a separate boolean is redundant.
