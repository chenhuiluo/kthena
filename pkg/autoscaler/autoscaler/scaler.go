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

	workload "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/autoscaler/algorithm"
	listerv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
)

// Autoscaler 管理单个 HomogeneousTarget 的伸缩状态。
// 包含三个核心组件：
//   - Collector: 从 Pod 直采或 Prometheus 获取指标
//   - Status:   恐慌模式状态机 + 5 个滑动窗口（记录推荐/修正历史）
//   - Meta:     伸缩配置（min/max replicas）+ generation 追踪
type Autoscaler struct {
	Collector *MetricCollector
	Status    *Status
	Meta      *ScalingMeta
}

type ScalingMeta struct {
	Config    *workload.HomogeneousTarget
	Namespace string
	Generations
}

func NewAutoscaler(autoscalePolicy *workload.AutoscalingPolicy) *Autoscaler {
	return &Autoscaler{
		Status:    NewStatus(&autoscalePolicy.Spec.Behavior),
		Collector: NewMetricCollector(&autoscalePolicy.Spec.HomogeneousTarget.Target, autoscalePolicy, GetMetricTargets(autoscalePolicy)),
		Meta: &ScalingMeta{
			Config:    autoscalePolicy.Spec.HomogeneousTarget,
			Namespace: autoscalePolicy.Namespace,
			Generations: Generations{
				AutoscalePolicyGeneration: autoscalePolicy.Generation,
			},
		},
	}
}

func (autoscaler *Autoscaler) NeedUpdate(autoscalePolicy *workload.AutoscalingPolicy) bool {
	return autoscaler.Meta.Generations.AutoscalePolicyGeneration != autoscalePolicy.Generation
}

func (autoscaler *Autoscaler) UpdateAutoscalePolicy(autoscalePolicy *workload.AutoscalingPolicy) {
	if autoscaler.Meta.Generations.AutoscalePolicyGeneration == autoscalePolicy.Generation {
		return
	}
	autoscaler.Meta.Generations.AutoscalePolicyGeneration = autoscalePolicy.Generation
}

// Scale 是 Homogeneous 路径的核心方法，执行两阶段伸缩决策：
//
// 第一阶段 — 推荐算法（GetRecommendedInstances）：
//   输入：当前所有就绪 Pod 的指标值、外部指标值、目标值
//   计算：对每个指标，ratio = 平均指标值 / 目标值
//         如果 ratio 在容忍带 [0.9, 1.1] 内，跳过
//         否则 desired = ceil(ratio × metricsCount)
//         多个指标取 max（满足最差的那个）
//   输出：recommendedInstances（数学最优副本数）
//
// 第二阶段 — 修正算法（GetCorrectedInstances）：
//   输入：recommendedInstances, isPanic, 5 个滑动窗口历史, Behavior 配置
//   计算：根据 isPanic 选择恐慌/稳定路径
//         稳定缩容：MaxRecommendation 防抖 → MaxCorrected 速率限制
//         稳定扩容：MinRecommendation 防抖 → MinCorrectedForStable 速率限制
//         恐慌扩容：MinCorrectedForPanic 速率限制（10x），永不缩容
//   输出：correctedInstances（工程安全副本数）
//
// 返回 -1 表示本周期不需要伸缩（推荐算法 skip=true）
func (autoscaler *Autoscaler) Scale(ctx context.Context, podLister listerv1.PodLister, autoscalePolicy *workload.AutoscalingPolicy, currentInstancesCount int32) (int32, error) {
	// 步骤1：采集指标。HTTP GET 各 Pod 的 /metrics 端点，或查询 Prometheus。
	// 返回：未就绪 Pod 数、就绪 Pod 的指标汇总、外部指标值
	unreadyInstancesCount, readyInstancesMetrics, externalMetrics, err := autoscaler.Collector.UpdateMetrics(ctx, podLister, autoscaler.Meta.Config.Target.MetricSources)
	if err != nil {
		klog.Errorf("update metrics error: %v", err)
		return -1, err
	}
	// 步骤2：推荐算法。纯粹基于指标快照，算出"如果一切理想，需要多少副本"
	instancesAlgorithm := algorithm.RecommendedInstancesAlgorithm{
		MinInstances:          autoscaler.Meta.Config.MinReplicas,
		MaxInstances:          autoscaler.Meta.Config.MaxReplicas,
		CurrentInstancesCount: currentInstancesCount,
		Tolerance:             float64(autoscalePolicy.Spec.TolerancePercent) * 0.01,
		MetricTargets:         autoscaler.Collector.MetricTargets,
		UnreadyInstancesCount: unreadyInstancesCount,
		ReadyInstancesMetrics: []algorithm.Metrics{readyInstancesMetrics},
		ExternalMetrics:       externalMetrics,
	}
	recommendedInstances, skip := instancesAlgorithm.GetRecommendedInstances()
	if skip {
		klog.InfoS("skip recommended instances")
		return -1, nil
	}
	// 步骤3：恐慌检查。当推荐值 ≥ 当前值的 2 倍（默认 PanicThresholdPercent=200），
	// 说明"按当前容量有一半以上需求无法满足"→ 触发恐慌模式。
	// 恐慌模式会刷新 PanicModeEndsAt = now + PanicModeHold（默认60s），
	// 意味着即使下一周期负载下降，也至少保持恐慌60秒（防虚假恢复）。
	if autoscalePolicy.Spec.Behavior.ScaleUp.PanicPolicy.PanicThresholdPercent != nil && recommendedInstances*100 >= currentInstancesCount*(*autoscalePolicy.Spec.Behavior.ScaleUp.PanicPolicy.PanicThresholdPercent) {
		autoscaler.Status.RefreshPanicMode()
	}

	// 步骤4：修正算法。在推荐值基础上叠加工程安全约束：
	//   - 防抖：不让指标毛刺导致频繁伸缩
	//   - 速率限制：不让一次扩/缩太多
	//   - 恐慌通道：紧急情况下允许10x速扩容
	CorrectedInstancesAlgorithm := algorithm.CorrectedInstancesAlgorithm{
		IsPanic:              autoscaler.Status.IsPanicMode(),
		History:              autoscaler.Status.History,
		Behavior:             &autoscalePolicy.Spec.Behavior,
		MinInstances:         autoscaler.Meta.Config.MinReplicas,
		MaxInstances:         autoscaler.Meta.Config.MaxReplicas,
		CurrentInstances:     currentInstancesCount,
		RecommendedInstances: recommendedInstances,
	}
	correctedInstances := CorrectedInstancesAlgorithm.GetCorrectedInstances()

	// 步骤5：将推荐值和修正值记录到 5 个滑动窗口，供下个周期做防抖和速率限制
	klog.InfoS("autoscale controller", "currentInstancesCount", currentInstancesCount, "recommendedInstances", recommendedInstances, "correctedInstances", correctedInstances)
	autoscaler.Status.AppendRecommendation(recommendedInstances)
	autoscaler.Status.AppendCorrected(correctedInstances)
	return correctedInstances, nil
}
