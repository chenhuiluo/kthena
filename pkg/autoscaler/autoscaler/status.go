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
	"github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/autoscaler/algorithm"
	"github.com/volcano-sh/kthena/pkg/autoscaler/datastructure"
	"github.com/volcano-sh/kthena/pkg/autoscaler/util"
)

// Status 维护伸缩的运行时状态，包含：
//   - 恐慌模式状态机：PanicModeEndsAt 记录恐慌到期时间戳
//   - History：5 个滑动窗口，记录推荐/修正历史，供修正算法做防抖和速率限制
//
// 恐慌模式状态机转换：
//
//	Stable → Panic:    推荐*100 >= 当前*PanicThresholdPercent（默认200，即推荐>=2x当前）
//	Panic → PanicHold: RefreshPanicMode() 将 PanicModeEndsAt 推迟到 now + PanicModeHold
//	PanicHold → Stable: 当前时间 > PanicModeEndsAt 且未被 Refresh
//
// 每次恐慌条件满足都调用 RefreshPanicMode()，PanicModeEndsAt 会被不断推迟。
// 只有连续 60 秒（默认 PanicModeHold）不满足恐慌条件，才自动退出恐慌。
// 这种"滞后退出"机制防止负载短暂下降后立即退出恐慌（虚假恢复）。
type Status struct {
	PanicModeEndsAt           int64
	PanicModeHoldMilliseconds int64
	History                   *algorithm.History
}

// NewStatus 根据 Behavior 配置创建 Status，包含恐慌模式状态机和 5 个滑动窗口。
// 各窗口的周期（freshMilliseconds）来自 Behavior 的不同字段：
//   MaxRecommendation     ← ScaleDown.StabilizationWindow
//   MinRecommendation     ← ScaleUp.StablePolicy.StabilizationWindow
//   MaxCorrected          ← ScaleDown.Period
//   MinCorrectedForStable ← ScaleUp.StablePolicy.Period
//   MinCorrectedForPanic  ← ScaleUp.PanicPolicy.Period
//
// 周期为 0 意味着窗口不启用，GetBest() 返回 (0, false)，即无约束。
func NewStatus(behavior *v1alpha1.AutoscalingPolicyBehavior) *Status {
	panicModeHoldMilliseconds := int64(0)
	if behavior.ScaleUp.PanicPolicy.PanicModeHold != nil {
		panicModeHoldMilliseconds = behavior.ScaleUp.PanicPolicy.PanicModeHold.Milliseconds()
	}
	scaleDownStabilizationWindowMilliseconds := int64(0)
	if behavior.ScaleDown.StabilizationWindow != nil {
		scaleDownStabilizationWindowMilliseconds = behavior.ScaleDown.StabilizationWindow.Milliseconds()
	}
	scaleUpStabilizationWindowMilliseconds := int64(0)
	if behavior.ScaleUp.StablePolicy.StabilizationWindow != nil {
		scaleUpStabilizationWindowMilliseconds = behavior.ScaleUp.StablePolicy.StabilizationWindow.Milliseconds()
	}
	scaleUpStablePolicyPeriodMilliseconds := int64(0)
	if behavior.ScaleUp.StablePolicy.Period != nil {
		scaleUpStablePolicyPeriodMilliseconds = behavior.ScaleUp.StablePolicy.Period.Milliseconds()
	}
	scaleDownPeriodMilliseconds := int64(0)
	if behavior.ScaleDown.Period != nil {
		scaleDownPeriodMilliseconds = behavior.ScaleDown.Period.Milliseconds()
	}
	return &Status{
		PanicModeEndsAt:           0,
		PanicModeHoldMilliseconds: panicModeHoldMilliseconds,
		History: &algorithm.History{
			MaxRecommendation:     datastructure.NewMaximumRecordSlidingWindow[int32](scaleDownStabilizationWindowMilliseconds),
			MinRecommendation:     datastructure.NewMinimumRecordSlidingWindow[int32](scaleUpStabilizationWindowMilliseconds),
			MaxCorrected:          datastructure.NewMaximumLineChartSlidingWindow[int32](scaleDownPeriodMilliseconds),
			MinCorrectedForStable: datastructure.NewMinimumLineChartSlidingWindow[int32](scaleUpStablePolicyPeriodMilliseconds),
			MinCorrectedForPanic:  datastructure.NewMinimumLineChartSlidingWindow[int32](behavior.ScaleUp.PanicPolicy.Period.Milliseconds()),
		},
	}
}

func (s *Status) AppendRecommendation(recommendedInstances int32) {
	s.History.MaxRecommendation.Append(recommendedInstances)
	s.History.MinRecommendation.Append(recommendedInstances)
}

func (s *Status) AppendCorrected(correctedInstances int32) {
	s.History.MaxCorrected.Append(correctedInstances)
	s.History.MinCorrectedForStable.Append(correctedInstances)
	s.History.MinCorrectedForPanic.Append(correctedInstances)
}

// RefreshPanicMode 刷新恐慌到期时间。
// 如果 PanicModeHoldMilliseconds=0，表示未配置恐慌模式，PanicModeEndsAt=0，永远不进入恐慌。
// 否则，将 PanicModeEndsAt 推迟到 now + PanicModeHoldMilliseconds。
// 每次恐慌条件满足都调用此方法，实现"持续续期"的滞后退出。
func (s *Status) RefreshPanicMode() {
	if s.PanicModeHoldMilliseconds == 0 {
		s.PanicModeEndsAt = 0
	} else {
		s.PanicModeEndsAt = util.GetCurrentTimestamp() + s.PanicModeHoldMilliseconds
	}
}

// IsPanicMode 判断当前是否处于恐慌模式。
// 两个条件必须同时满足：
//  1. PanicModeHoldMilliseconds > 0（配置了恐慌模式）
//  2. 当前时间 <= PanicModeEndsAt（恐慌尚未到期）
func (s *Status) IsPanicMode() bool {
	return s.PanicModeHoldMilliseconds > 0 && util.GetCurrentTimestamp() <= s.PanicModeEndsAt
}
