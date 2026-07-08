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

package framework

import (
	"k8s.io/apimachinery/pkg/types"

	aiv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/networking/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/kthena-router/common"
	"github.com/volcano-sh/kthena/pkg/kthena-router/datastore"
	"github.com/volcano-sh/kthena/pkg/kthena-router/metrics"
)

// =============================================================================
// 调度框架接口定义 — 所有插件必须实现这些接口
// =============================================================================
//
// 【三种插件类型】
//   FilterPlugin:   过滤不合格的 Pod (硬条件, 返回 true/false)
//   ScorePlugin:    对通过过滤的 Pod 打分 (软偏好, 返回 [0,100] 分)
//   PostScheduleHook: 调度完成后执行后处理 (如写缓存)
//
// 【Context】
//   在插件之间传递的上下文, 携带:
//     Model, Prompt, SessionID — 请求信息
//     BestPods — 同构场景调度结果 (TopN)
//     DecodePods, PrefillPods — PD 分离场景调度结果
//     PDGroup — PD 分组信息
//     MetricsRecorder — 指标记录器
//
// =============================================================================

// Context 存储在 Filter/Score 插件之间传递的信息
type Context struct {
	Model  string
	Prompt *common.ChatMessage

	// SessionID is the session identifier extracted from the HTTP header configured
	// via the SESSION_BOOST_HEADER environment variable.
	SessionID string

	Hashes []uint64

	// ModelServer information for efficient PDGroup scheduling
	ModelServerName types.NamespacedName
	PDGroup         *aiv1alpha1.PDGroup
	// 1. In PD Disaggregated mode, both DecodePods and PrefillPods are set.
	DecodePods  []*datastore.PodInfo
	PrefillPods []*datastore.PodInfo

	// 2. PD aggregated mode, BestPods is selected for inference.
	BestPods []*datastore.PodInfo

	// MetricsRecorder for recording scheduler plugin metrics
	MetricsRecorder *metrics.RequestMetricsRecorder
}

// ScorePlugin 对通过过滤的 Pod 打分, 分数范围 [0, 100], 100=最优
type ScorePlugin interface {
	Name() string
	// Score is a method that is used to rank pods that have passed the filter plugins.
	// Note each plugin should generate score for a pod within [0, 100]
	Score(ctx *Context, pods []*datastore.PodInfo) map[*datastore.PodInfo]int
}

// FilterPlugin 过滤不合格的 Pod, 返回 true=保留, false=剔除
type FilterPlugin interface {
	Name() string
	// Filter is a method that is used to filter valid pods that can be sent request to.
	Filter(ctx *Context, pods []*datastore.PodInfo) []*datastore.PodInfo
}

// PostHook is an interface that is executed after the scheduling is complete.
type PostScheduleHook interface {
	Name() string
	PostSchedule(ctx *Context, index int)
}
