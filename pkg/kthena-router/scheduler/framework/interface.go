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

// Context 是调度上下文, 在一次请求的 Filter/Score/PostHook 插件之间传递。
// 它是贯穿整个调度管线的唯一数据载体 — router 在进入 Schedule() 前构建,
// 插件从中读取所需信息(模型名/prompt/会话), 调度结果再写回给上层 proxy 使用。
type Context struct {
	// Model: 请求的模型名 (来自请求体 "model" 字段), 用于匹配 ModelRoute/ModelServer
	Model string
	// Prompt: 请求的 prompt 内容, 供 prefix-cache/kvcache-aware 计算 hash 用
	Prompt *common.ChatMessage

	// SessionID: 从 HTTP header (SESSION_BOOST_HEADER 环境变量配置) 提取的会话 ID。
	// 用于会话亲和: 同 session 的请求尽量路由到同一 Pod, 复用其 KV cache
	SessionID string

	// Hashes: prompt 的滚动哈希链 (prefix-cache 插件计算并写回),
	// 供 Score/PostSchedule 阶段做前缀匹配。见 prefix_cache.go 的 hashPrompt()
	Hashes []uint64

	// ModelServerName: 当前请求命中的 ModelServer, 用于查询该 ModelServer 下的 Pod
	ModelServerName types.NamespacedName
	// PDGroup: PD 分组配置, !=nil 表示走 PD 分离分支 (decode/prefill 分开调度)
	PDGroup *aiv1alpha1.PDGroup

	// 1. PD 分离模式: Schedule() 同时填充 DecodePods(Top5) 和 PrefillPods(每个 Decode 配 1 个 Prefill)
	DecodePods  []*datastore.PodInfo
	PrefillPods []*datastore.PodInfo

	// 2. 同构/PD 聚合模式: Schedule() 填充 BestPods(Top5), 上层 proxy() 逐个尝试代理
	BestPods []*datastore.PodInfo

	// MetricsRecorder: 指标记录器, 插件可记录延迟/命中率等指标
	MetricsRecorder *metrics.RequestMetricsRecorder
}

// ScorePlugin 对通过过滤的 Pod 打分 (软偏好)。
// 分数范围 [0, 100]: 0=最差, 100=最优。各插件的分数 × 权重累加成加权总分用于 TopN 排序。
type ScorePlugin interface {
	Name() string
	// Score 对 pods 中的每个 Pod 打分, 返回 map[*PodInfo]int。
	// 每个 Pod 的分数须落在 [0, 100], 由各插件自行归一化 (无独立 NormalizeScore 阶段)
	Score(ctx *Context, pods []*datastore.PodInfo) map[*datastore.PodInfo]int
}

// FilterPlugin 过滤不合格的 Pod (硬条件)。
// 返回保留的 Pod 列表 — 任一 Filter 把某 Pod 排除, 该 Pod 即不进入 Score 阶段。
type FilterPlugin interface {
	Name() string
	// Filter 接收完整 Pod 列表, 返回过滤后保留的子集 (批量过滤, 比逐 Pod 调用高效)
	Filter(ctx *Context, pods []*datastore.PodInfo) []*datastore.PodInfo
}

// PostScheduleHook 在请求代理成功后执行后处理 (对应 kube-scheduler 的 PostBind)。
// 如 prefix-cache 插件在此把 prompt hash 写入 LRU 缓存, 供后续同前缀请求命中。
// 参数 index 是本次代理在 TopN 列表中的索引, 用于定位实际使用的 Pod。
type PostScheduleHook interface {
	Name() string
	PostSchedule(ctx *Context, index int)
}
