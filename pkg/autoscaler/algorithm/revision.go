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

	"github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/autoscaler/datastructure"
)

// CorrectedInstancesAlgorithm 是伸缩的第二阶段（修正阶段）。
// 在推荐值基础上叠加工程安全约束，防止现实世界的风险：
//
// 5 个滑动窗口各自防范一种风险：
//   MaxRecommendation     — 缩容防抖：记住近期最大推荐值，缩容时不低于此
//   MinRecommendation     — 扩容防抖：记住近期最小推荐值，扩容时不高于此
//   MaxCorrected          — 缩容速率限制：每周期缩量不超过 Instances 或 Percent 限制
//   MinCorrectedForStable — 稳定扩容速率限制：每周期扩量不超过 Instances 或 Percent 限制
//   MinCorrectedForPanic  — 恐慌扩容速率限制：允许每周期 10x（Percent=1000%）
//
// 三条修正路径：
//   恐慌模式：MinCorrectedForPanic 限制速率，永不缩容（corrected >= current）
//   稳定缩容：MaxRecommendation 防抖 → MaxCorrected 速率限制
//   稳定扩容：MinRecommendation 防抖 → MinCorrectedForStable 速率限制
type CorrectedInstancesAlgorithm struct {
	IsPanic              bool
	History              *History
	Behavior             *v1alpha1.AutoscalingPolicyBehavior
	MinInstances         int32
	MaxInstances         int32
	CurrentInstances     int32
	RecommendedInstances int32
}

type History struct {
	MaxRecommendation     *datastructure.RmqRecordSlidingWindow[int32]
	MinRecommendation     *datastructure.RmqRecordSlidingWindow[int32]
	MaxCorrected          *datastructure.RmqLineChartSlidingWindow[int32]
	MinCorrectedForStable *datastructure.RmqLineChartSlidingWindow[int32]
	MinCorrectedForPanic  *datastructure.RmqLineChartSlidingWindow[int32]
}

// GetCorrectedInstances 执行修正算法，最终输出工程安全的副本数。
// 始终 clamp 到 [MinInstances, MaxInstances] 范围内。
func (alg CorrectedInstancesAlgorithm) GetCorrectedInstances() int32 {
	var corrected int32
	if alg.IsPanic {
		corrected = alg.getCorrectedInstancesForPanic()
	} else {
		corrected = alg.getCorrectedInstancesForStable()
	}
	return min(max(corrected, alg.MinInstances), alg.MaxInstances)
}

// 恐慌修正：允许 10x 速扩容，永不缩容
// 1. corrected = RecommendedInstances
// 2. 如果 MinCorrectedForPanic 窗口有历史样本，计算 relativeConstraint = pastSample × (1 + PanicPercent/100)
//    默认 PanicPercent=1000，即 relativeConstraint = pastSample × 11
// 3. corrected = min(corrected, relativeConstraint) — 速率限制
// 4. corrected = max(corrected, CurrentInstances) — 永不缩容
func (alg CorrectedInstancesAlgorithm) getCorrectedInstancesForPanic() int32 {
	corrected := alg.RecommendedInstances
	if pastSample, ok := alg.History.MinCorrectedForPanic.GetBest(alg.CurrentInstances); ok && pastSample > 0 {
		relativeConstraint := pastSample + int32(float64(pastSample)*float64(*alg.Behavior.ScaleUp.PanicPolicy.Percent)/100.0)
		corrected = min(corrected, relativeConstraint)
	}
	corrected = max(corrected, alg.CurrentInstances)
	return corrected
}

// 稳定修正：根据推荐值与当前值的关系选择缩容/扩容路径
func (alg CorrectedInstancesAlgorithm) getCorrectedInstancesForStable() int32 {
	var corrected int32
	switch {
	case alg.RecommendedInstances < alg.CurrentInstances:
		corrected = alg.getCorrectedInstancesForStableScaleDown()
	case alg.RecommendedInstances > alg.CurrentInstances:
		corrected = alg.getCorrectedInstancesForStableScaleUp()
	default:
		corrected = alg.RecommendedInstances
	}
	return corrected
}

// 稳定缩容路径：推荐值 < 当前值
// 1. corrected = RecommendedInstances
// 2. MaxRecommendation 防抖：corrected = max(corrected, MaxRecommendation.GetBest())
//    即"近期推荐值曾达到 X，不允许缩到 X 以下"
// 3. MaxCorrected 速率限制：
//    absoluteConstraint = pastSample - ScaleDown.Instances（默认-1）
//    relativeConstraint = pastSample × (1 - ScaleDown.Percent/100)（默认 ×0=0）
//    SelectPolicy=Or: constraint = min(abs, rel) 更宽松
//    SelectPolicy=And: constraint = max(abs, rel) 更严格
//    corrected = max(corrected, constraint)
// 4. corrected = min(corrected, CurrentInstances) — 缩容路径不允许扩容
func (alg CorrectedInstancesAlgorithm) getCorrectedInstancesForStableScaleDown() int32 {
	corrected := alg.RecommendedInstances
	if betterRecommendation, ok := alg.History.MaxRecommendation.GetBest(); ok {
		corrected = max(corrected, betterRecommendation)
	}
	if pastSample, ok := alg.History.MaxCorrected.GetBest(alg.CurrentInstances); ok {
		absoluteConstraint := pastSample - *alg.Behavior.ScaleDown.Instances
		relativeConstraint := pastSample - pastSample*(*alg.Behavior.ScaleDown.Percent)/100
		var constraint int32
		switch alg.Behavior.ScaleDown.SelectPolicy {
		case v1alpha1.SelectPolicyOr:
			constraint = min(absoluteConstraint, relativeConstraint)
		case v1alpha1.SelectPolicyAnd:
			constraint = max(absoluteConstraint, relativeConstraint)
		default:
			constraint = math.MinInt32
		}
		corrected = max(corrected, constraint)
	}
	corrected = min(corrected, alg.CurrentInstances)
	return corrected
}

// 稳定扩容路径：推荐值 > 当前值
// 1. corrected = RecommendedInstances
// 2. MinRecommendation 防抖：corrected = min(corrected, MinRecommendation.GetBest())
//    即"近期推荐值最低到 Y，不允许扩到 Y 以上"
// 3. MinCorrectedForStable 速率限制：
//    absoluteConstraint = pastSample + ScaleUp.StablePolicy.Instances（默认+1）
//    relativeConstraint = pastSample × (1 + ScaleUp.StablePolicy.Percent/100)（默认 ×2）
//    SelectPolicy=Or: constraint = max(abs, rel) 更宽松
//    SelectPolicy=And: constraint = min(abs, rel) 更严格
//    corrected = min(corrected, constraint)
// 4. corrected = max(corrected, CurrentInstances) — 扩容路径不允许缩容
func (alg CorrectedInstancesAlgorithm) getCorrectedInstancesForStableScaleUp() int32 {
	corrected := alg.RecommendedInstances
	if betterRecommendation, ok := alg.History.MinRecommendation.GetBest(); ok {
		corrected = min(corrected, betterRecommendation)
	}
	if pastSample, ok := alg.History.MinCorrectedForStable.GetBest(alg.CurrentInstances); ok {
		absoluteConstraint := pastSample + *alg.Behavior.ScaleUp.StablePolicy.Instances
		relativeConstraint := pastSample + pastSample*(*alg.Behavior.ScaleUp.StablePolicy.Percent)/100
		var constraint int32
		switch alg.Behavior.ScaleUp.StablePolicy.SelectPolicy {
		case v1alpha1.SelectPolicyOr:
			constraint = max(absoluteConstraint, relativeConstraint)
		case v1alpha1.SelectPolicyAnd:
			constraint = min(absoluteConstraint, relativeConstraint)
		default:
			constraint = math.MaxInt32
		}
		corrected = min(corrected, constraint)
	}
	corrected = max(corrected, alg.CurrentInstances)
	return corrected
}
