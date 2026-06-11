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

package algorithm

import (
	"math"

	"k8s.io/klog/v2"
)

type Metrics = map[string]float64

// RecommendedInstancesAlgorithm 是伸缩的第一阶段（推荐阶段）。
// 回答的问题："在当前指标值下，需要多少个副本才能让每个副本的平均负载恰好等于目标值？"
//
// 两条计算路径：
//   Instance 指标（每 Pod 维度，如 GPU Cache 利用率）：
//     recommended = ceil( Σ(metric_value_i) / target )
//     直觉：总负载 ÷ 单副本目标容量 = 需要的副本数
//
//   External 指标（全局维度，如 Prometheus 查询的系统级 QPS）：
//     recommended = ceil( metric_value / target )
//     直觉：全局需求量 ÷ 系统级目标 = 需要的副本数
//
// 关键设计点：
//   - 容忍带 Tolerance：|ratio - 1.0| <= 0.10 内不伸缩（防抖静区）
//   - 缺失 Pod 不对称处理：扩容方向悲观（按0贡献），缩容方向乐观（按目标值贡献）
//   - 方向反转保护：修正后如果方向反了，直接保持当前值
//   - 多指标取 max：确保任一 SLO 违规都触发扩容
type RecommendedInstancesAlgorithm struct {
	MinInstances          int32
	MaxInstances          int32
	CurrentInstancesCount int32
	Tolerance             float64
	MetricTargets         Metrics
	UnreadyInstancesCount int32
	ReadyInstancesMetrics []Metrics
	ExternalMetrics       Metrics
}

// GetRecommendedInstances 执行推荐算法。
// 返回 (recommendedInstances, skip)：
//   - recommendedInstances: 推荐的副本数
//   - skip=true: 当前状态在容忍带内或无有效指标，不需要伸缩（调用方应返回 -1）
func (alg *RecommendedInstancesAlgorithm) GetRecommendedInstances() (recommendedInstances int32, skip bool) {
	klog.InfoS("start to getRecommendedInstances", "args", alg)
	if alg.CurrentInstancesCount < alg.MinInstances {
		return alg.MinInstances, false
	}
	if alg.CurrentInstancesCount > alg.MaxInstances {
		return alg.MaxInstances, false
	}
	recommendedInstances = 0
	skip = true
	for name, target := range alg.MetricTargets {
		externalMetric, ok := alg.ExternalMetrics[name]
		if ok {
			updateRecommendation(&recommendedInstances, &skip,
				getDesiredInstancesForSingleExternalMetric(
					alg.CurrentInstancesCount,
					alg.Tolerance,
					target,
					externalMetric,
				))
		} else {
			if desired, ok := getDesiredInstancesForSingleInstanceMetric(
				alg.CurrentInstancesCount,
				alg.Tolerance,
				name,
				target,
				alg.UnreadyInstancesCount,
				alg.ReadyInstancesMetrics,
			); ok {
				updateRecommendation(&recommendedInstances, &skip, desired)
			}
		}
	}
	if !skip {
		recommendedInstances = min(max(recommendedInstances, alg.MinInstances), alg.MaxInstances)
	}
	return recommendedInstances, skip
}

func updateRecommendation(recommendedInstances *int32, skip *bool, desired int32) {
	if *skip {
		*recommendedInstances = desired
		*skip = false
	} else {
		*recommendedInstances = max(*recommendedInstances, desired)
	}
}

func getDesiredInstancesForSingleExternalMetric(
	currentCount int32,
	tolerance float64,
	target float64,
	metric float64,
) int32 {
	desired := metric / target
	ratio := desired / float64(currentCount)
	if math.Abs(ratio-1.0) <= tolerance {
		return currentCount
	}
	return getCeilDesiredInstances(desired)
}

func getDesiredInstancesForSingleInstanceMetric(
	currentCount int32,
	tolerance float64,
	name string,
	target float64,
	unreadyCount int32,
	readyMetrics []Metrics,
) (desired int32, ok bool) {
	currentMetricSum := 0.0
	missingCount := int32(0)
	metricsCount := int32(0)
	for _, readyInstance := range readyMetrics {
		metric, ok := readyInstance[name]
		if ok {
			metricsCount++
			currentMetricSum += metric
		} else {
			missingCount++
		}
	}
	if metricsCount == 0 {
		return 0, false
	}
	ratio := currentMetricSum / float64(metricsCount) / target
	shouldAddUnready := unreadyCount > 0 && getDirection(ratio) > 0
	klog.InfoS("recommendation", "metricsCount", metricsCount, "currentMetricSum", currentMetricSum, "ratio", ratio,
		"unreadyCount", unreadyCount, "shouldAddUnready", shouldAddUnready, "missingCount", missingCount, "tolerance", tolerance)
	if !shouldAddUnready && missingCount == 0 {
		if math.Abs(ratio-1.0) <= tolerance {
			return currentCount, true
		}
		return getCeilDesiredInstances(ratio * float64(metricsCount)), true
	}
	metricsCount += missingCount
	if getDirection(ratio) < 0 {
		currentMetricSum += float64(missingCount) * target
	}
	if shouldAddUnready {
		metricsCount += unreadyCount
	}
	newRatio := currentMetricSum / float64(metricsCount) / target
	if math.Abs(newRatio-1.0) <= tolerance || getDirection(ratio) != getDirection(newRatio) {
		return currentCount, true
	}
	desired = getCeilDesiredInstances(newRatio * float64(metricsCount))
	if (getDirection(newRatio) < 0 && desired > currentCount) ||
		(getDirection(newRatio) > 0 && desired < currentCount) {
		return currentCount, true
	}
	return desired, true
}

func getDirection(ratio float64) int32 {
	if ratio >= 1.0 {
		return 1
	} else {
		return -1
	}
}

func getCeilDesiredInstances(value float64) int32 {
	if math.IsNaN(value) {
		return 0
	}
	value = math.Ceil(value)
	const bound = int32(1000000000)
	if value < float64(bound) {
		return max(0, int32(value))
	}
	return bound
}
