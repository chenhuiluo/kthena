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

package autoscaler

import (
	"context"
	"sort"
	"strings"

	workload "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/autoscaler/algorithm"
	corev1 "k8s.io/api/core/v1"
	listerv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
)

// Optimizer 管理 HeterogeneousTarget 的联合伸缩。
// 与 Autoscaler 的区别：Autoscaler 管理单个角色的伸缩，Optimizer 管理多个后端的联合伸缩。
// 核心思路：先算所有后端的"总需求副本数"，再按成本优先级分配到各后端。
type Optimizer struct {
	Meta       *OptimizerMeta
	Collectors map[string]*MetricCollector
	Status     *Status
	Generations
}

// OptimizerMeta 包含异构伸缩的元数据。
// ScalingOrder 是成本排序后的 ReplicaBlock 列表——分配副本时按此顺序逐个填充，
// 成本低的先填满，成本高的后填。这实现了"贪心成本最小化"的分配策略。
// MinReplicas/MaxReplicas 是所有后端的 min/max 之和（全局上下限）。
type OptimizerMeta struct {
	Config        *workload.HeterogeneousTarget
	MetricTargets map[string]float64
	ScalingOrder  []*ReplicaBlock
	MinReplicas   int32
	MaxReplicas   int32
	Scope         Scope
}

// ReplicaBlock 代表一个可分配的副本块。
// 异构伸缩中，每个后端的可伸缩范围（MaxReplicas - MinReplicas）被拆分成多个 Block。
// 例如后端 A 的可伸缩范围为 8，CostExpansionRatePercent=200（指数分块），
// 则拆成 Block 大小为 1, 2, 4, 1 的四个块。
// 按 cost 排序后，分配时先填满便宜的 Block，再填贵的 Block。
type ReplicaBlock struct {
	targetKey string
	index     int32
	replicas  int32
	cost      int64
}

// HeterogeneousTargetKey returns a namespace-qualified key for a
// heterogeneous autoscaling target. An empty target namespace refers to the
// namespace of the AutoscalingPolicy.
func HeterogeneousTargetKey(targetRef corev1.ObjectReference, policyNamespace string) string {
	namespace := targetRef.Namespace
	if namespace == "" {
		namespace = policyNamespace
	}
	return namespace + "/" + targetRef.Name
}

// RestoreReplicasOfEachBackend 将聚合的修正副本数分配回各个后端。
// 分配算法（贪心成本最小化）：
//  1. 初始化每个后端为 MinReplicas
//  2. 从总修正值中减去全局 MinReplicas，得到"可分配余量"
//  3. 按 ScalingOrder（成本升序）逐个填充 Block：
//     每个 Block 分配 slot = min(余量, block.replicas)
//     余量 -= slot
//  4. 余量耗尽则停止
//
// 例子：两个后端 T4(cost=1,可伸缩=8) 和 A100(cost=3,可伸缩=4)
//
//	Total=6, Min=3, 余量=3
//	Block 排序：T4(1个,成本1), T4(2个,成本2), T4(4个,成本4), A100(1个,成本3), A100(2个,成本6), A100(1个,成本3)
//	按成本排序：1,2,3,3,4,6 → 先填 T4 的 1+2=3，余量=0，A100 不分配额外副本
func (meta *OptimizerMeta) RestoreReplicasOfEachBackend(replicas int32) map[string]int32 {
	replicasMap := make(map[string]int32, len(meta.Config.Params))
	for _, param := range meta.Config.Params {
		replicasMap[HeterogeneousTargetKey(param.Target.TargetRef, meta.Scope.Namespace)] = param.MinReplicas
	}
	replicas = min(max(replicas, meta.MinReplicas), meta.MaxReplicas)
	replicas -= meta.MinReplicas
	for _, block := range meta.ScalingOrder {
		slot := min(replicas, block.replicas)
		replicasMap[block.targetKey] += slot
		replicas -= slot
		if replicas <= 0 {
			break
		}
	}
	return replicasMap
}

// NewOptimizerMeta 构建 OptimizerMeta，核心是生成 ScalingOrder（ReplicaBlock 排序列表）。
// 指数分块算法（CostExpansionRatePercent ≠ 100 时）：
//   初始 packageLen=1.0
//   循环：currentLen = min(剩余可伸缩, max(int32(packageLen), 1))
//         创建 Block{name, index, currentLen, cost*currentLen}
//         剩余 -= currentLen
//         packageLen *= CostExpansionRatePercent / 100
//   这产生了 1,2,4,8... 的指数分块（rate=200时）
//
// 为什么要指数分块而不是均匀分块？
//   因为成本是按 Block 累积的。一个大小为 8 的 Block 成本=8*cost，而两个大小为 4 的 Block
//   总成本也是 8*cost，但排序后可以更灵活地与其他后端的 Block 交叉排列。
//   指数分块让小的 Block（1-2个副本）排在前面，可以更精细地按成本递增分配。
func NewOptimizerMeta(policy *workload.AutoscalingPolicy) *OptimizerMeta {
	if policy.Spec.HeterogeneousTarget == nil {
		klog.Warningf("OptimizerConfig not configured in policy: %s", policy.Name)
		return nil
	}
	costExpansionRatePercent := policy.Spec.HeterogeneousTarget.CostExpansionRatePercent
	minReplicas := int32(0)
	maxReplicas := int32(0)
	var scalingOrder []*ReplicaBlock
	for index, param := range policy.Spec.HeterogeneousTarget.Params {
		targetKey := HeterogeneousTargetKey(param.Target.TargetRef, policy.Namespace)
		minReplicas += param.MinReplicas
		maxReplicas += param.MaxReplicas
		replicas := param.MaxReplicas - param.MinReplicas
		if replicas <= 0 {
			continue
		}
		if costExpansionRatePercent == 100 {
			scalingOrder = append(scalingOrder, &ReplicaBlock{
				index:     int32(index),
				targetKey: targetKey,
				replicas:  replicas,
				cost:      int64(param.Cost),
			})
			continue
		}
		packageLen := 1.0
		for replicas > 0 {
			currentLen := min(replicas, max(int32(packageLen), 1))
			scalingOrder = append(scalingOrder, &ReplicaBlock{
				targetKey: targetKey,
				index:     int32(index),
				replicas:  currentLen,
				cost:      int64(param.Cost) * int64(currentLen),
			})
			replicas -= currentLen
			packageLen = packageLen * float64(costExpansionRatePercent) / 100
		}
	}
	sort.Slice(scalingOrder, func(i, j int) bool {
		if scalingOrder[i].cost != scalingOrder[j].cost {
			return scalingOrder[i].cost < scalingOrder[j].cost
		}
		return scalingOrder[i].index < scalingOrder[j].index
	})
	return &OptimizerMeta{
		Config:       policy.Spec.HeterogeneousTarget,
		MinReplicas:  minReplicas,
		MaxReplicas:  maxReplicas,
		ScalingOrder: scalingOrder,
		Scope: Scope{
			OwnedPolicyId: policy.UID,
			Namespace:     policy.Namespace,
		},
	}
}

func NewOptimizer(autoscalePolicy *workload.AutoscalingPolicy) *Optimizer {
	metricTargets := GetMetricTargets(autoscalePolicy)
	collectors := make(map[string]*MetricCollector)
	for _, param := range autoscalePolicy.Spec.HeterogeneousTarget.Params {
		targetKey := HeterogeneousTargetKey(param.Target.TargetRef, autoscalePolicy.Namespace)
		collectors[targetKey] = NewMetricCollector(&param.Target, autoscalePolicy, metricTargets)
	}

	meta := NewOptimizerMeta(autoscalePolicy)
	meta.MetricTargets = metricTargets
	return &Optimizer{
		Meta:       meta,
		Collectors: collectors,
		Status:     NewStatus(&autoscalePolicy.Spec.Behavior),
		Generations: Generations{
			AutoscalePolicyGeneration: autoscalePolicy.Generation,
		},
	}
}

func (optimizer *Optimizer) NeedUpdate(policy *workload.AutoscalingPolicy) bool {
	return optimizer.Generations.AutoscalePolicyGeneration != policy.Generation
}

// Optimize 是 Heterogeneous 路径的核心方法。与 Scale() 类似的两阶段流程，但操作的是所有后端的聚合值：
//
// 步骤1：遍历所有后端（param），分别采集指标
//   - 每个后端有自己的 MetricCollector，独立采集该后端 Pod 的指标
//   - 累加所有后端的当前副本数 → instancesCountSum
//   - 外部指标直接求和（TODO: 对 GPU 利用率等比率型指标语义错误，见缺陷 #7）
//
// 步骤2：推荐算法——在聚合总量上计算推荐值
//   MinInstances/MaxInstances 是所有后端的 min/max 之和
//   推荐值代表"所有后端加起来总共需要多少副本"
//
// 步骤3：恐慌检查——与 Scale() 相同的阈值逻辑，但 current 是聚合总量
//
// 步骤4：修正算法——与 Scale() 相同的 5 窗口逻辑
//
// 步骤5：RestoreReplicasOfEachBackend——将聚合修正值按成本优先级分配到各后端
func (optimizer *Optimizer) Optimize(ctx context.Context, podLister listerv1.PodLister, autoscalePolicy *workload.AutoscalingPolicy, currentInstancesCounts map[string]int32) (map[string]int32, error) {
	size := len(optimizer.Meta.Config.Params)
	unreadyInstancesCount := int32(0)
	readyInstancesMetrics := make([]algorithm.Metrics, 0, size)
	// externalSamples accumulates per-backend (value, replicas) pairs for each
	// external metric so that the correct aggregation can be applied afterwards.
	externalSamples := make(map[string][]backendExternalSample)
	instancesCountSum := int32(0)
	// Update all model serving instances' metrics
	for _, param := range optimizer.Meta.Config.Params {
		targetKey := HeterogeneousTargetKey(param.Target.TargetRef, autoscalePolicy.Namespace)
		collector, exists := optimizer.Collectors[targetKey]
		if !exists {
			klog.Warningf("collector for target %s not exists", targetKey)
			continue
		}

		backendReplicas := currentInstancesCounts[targetKey]
		instancesCountSum += backendReplicas
		currentUnreadyInstancesCount, currentReadyInstancesMetrics, currentExternalMetrics, err := collector.UpdateMetrics(ctx, podLister, param.Target.MetricSources)
		if err != nil {
			klog.Warningf("update metrics error: %v", err)
			continue
		}
		unreadyInstancesCount += currentUnreadyInstancesCount
		readyInstancesMetrics = append(readyInstancesMetrics, currentReadyInstancesMetrics)
		for metricName, metricValue := range currentExternalMetrics {
			externalSamples[metricName] = append(externalSamples[metricName], backendExternalSample{
				value:    metricValue,
				replicas: backendReplicas,
			})
		}
	}
	// Aggregate external metrics using the semantics appropriate for each metric
	// type: additive metrics are summed; ratio metrics use a replica-weighted
	// average so that heterogeneous backends (different replica counts) do not
	// distort the result.
	externalMetrics := make(algorithm.Metrics, len(externalSamples))
	for name, samples := range externalSamples {
		externalMetrics[name] = aggregateExternalSamples(name, samples)
	}
	// Get recommended replicas of all model serving instances
	instancesAlgorithm := algorithm.RecommendedInstancesAlgorithm{
		MinInstances:          optimizer.Meta.MinReplicas,
		MaxInstances:          optimizer.Meta.MaxReplicas,
		CurrentInstancesCount: instancesCountSum,
		Tolerance:             float64(autoscalePolicy.Spec.TolerancePercent) * 0.01,
		MetricTargets:         optimizer.Meta.MetricTargets,
		UnreadyInstancesCount: unreadyInstancesCount,
		ReadyInstancesMetrics: readyInstancesMetrics,
		ExternalMetrics:       externalMetrics,
	}
	recommendedInstances, skip := instancesAlgorithm.GetRecommendedInstances()
	if skip {
		klog.Warning("skip recommended instances")
		return nil, nil
	}
	if recommendedInstances*100 >= instancesCountSum*(*autoscalePolicy.Spec.Behavior.ScaleUp.PanicPolicy.PanicThresholdPercent) {
		optimizer.Status.RefreshPanicMode()
	}
	CorrectedInstancesAlgorithm := algorithm.CorrectedInstancesAlgorithm{
		IsPanic:              optimizer.Status.IsPanicMode(),
		History:              optimizer.Status.History,
		Behavior:             &autoscalePolicy.Spec.Behavior,
		MinInstances:         optimizer.Meta.MinReplicas,
		MaxInstances:         optimizer.Meta.MaxReplicas,
		CurrentInstances:     instancesCountSum,
		RecommendedInstances: recommendedInstances}
	correctedInstances := CorrectedInstancesAlgorithm.GetCorrectedInstances()

	klog.InfoS("autoscale controller", "recommendedInstances", recommendedInstances, "correctedInstances", correctedInstances)
	optimizer.Status.AppendRecommendation(recommendedInstances)
	optimizer.Status.AppendCorrected(correctedInstances)

	replicasMap := optimizer.Meta.RestoreReplicasOfEachBackend(correctedInstances)
	return replicasMap, nil
}

// backendExternalSample pairs an external metric value with the replica count
// of the backend that reported it.
type backendExternalSample struct {
	value    float64
	replicas int32
}

// isRatioMetric reports whether name represents a ratio (bounded) metric that
// must be aggregated as a weighted average across backends rather than summed.
// Recognised suffixes: _utilization, _usage, _ratio, _percent, _saturation.
// The "rate" suffix is intentionally excluded: throughput and request-rate
// metrics are additive across backends.
func isRatioMetric(name string) bool {
	lowerName := strings.ToLower(name)
	for _, sfx := range []string{"_utilization", "_usage", "_ratio", "_percent", "_saturation"} {
		if strings.HasSuffix(lowerName, sfx) {
			return true
		}
	}
	return false
}

// aggregateExternalSamples combines per-backend metric samples into a single
// value using the semantics appropriate for the metric:
//
//   - Additive metrics (queue length, request count, …): sum across backends.
//   - Ratio metrics (utilization, usage, …): replica-count weighted average,
//     Σ(value_i × replicas_i) / Σ(replicas_i), matching Kubernetes HPA
//     semantics for cross-instance metric aggregation.
//
// When all backend replica counts are zero the function falls back to a plain
// unweighted average so that ratio metrics are never silently dropped.
func aggregateExternalSamples(name string, samples []backendExternalSample) float64 {
	if !isRatioMetric(name) {
		total := 0.0
		for _, s := range samples {
			total += s.value
		}
		return total
	}

	// Weighted average: Σ(value_i × replicas_i) / Σ(replicas_i)
	weightedSum := 0.0
	totalReplicas := int32(0)
	for _, s := range samples {
		weightedSum += s.value * float64(s.replicas)
		totalReplicas += s.replicas
	}
	if totalReplicas == 0 {
		// Replica counts are unavailable; fall back to unweighted average so
		// the metric still influences scaling decisions.
		sum := 0.0
		for _, s := range samples {
			sum += s.value
		}
		return sum / float64(len(samples))
	}
	return weightedSum / float64(totalReplicas)
}
