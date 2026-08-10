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

package plugins

import (
	"math"

	"istio.io/istio/pkg/slices"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"sigs.k8s.io/yaml"

	"github.com/volcano-sh/kthena/pkg/kthena-router/datastore"
	"github.com/volcano-sh/kthena/pkg/kthena-router/scheduler/framework"
)

// =============================================================================
// least-request 插件 — 唯一同时实现 Score + Filter 的插件, 核心是"按在途负载分发请求"
// =============================================================================
//
// 【为什么用 onFlight 而非 running】
//   running:  引擎上报的正在执行请求数, 来自 /metrics 采集 — 有 ~1s 轮询延迟。
//   onFlight: router 自己记录的已分发但未收到响应的请求数, 零延迟更新。
//   高负载下 1s 延迟足以让队列堆积, 故打分以 onFlight 为主, running 仅用于估算"排队量"。
//
// 【跨 router 的 onFlight 同步】
//   router 通常多副本, 各副本只知道自己分发的请求。least-request 启用时,
//   scheduler.Schedule() 开头会 syncOnFlight 从 Redis 同步全局在途计数,
//   保证各副本看到一致的负载视图。见 scheduler_impl.go 的 syncOnFlight 字段。
//
// 【Filter vs Score】
//   Filter: 队列深度(waiting) >= maxWaitingRequests 的 Pod 直接剔除 (硬门槛, 引擎已积压)
//   Score:  onFlight 越少分越高, 且对"已分发但引擎未执行"(onFlight-running) 的排队强烈惩罚
// =============================================================================

const LeastRequestPluginName = "least-request"

var _ framework.ScorePlugin = &LeastRequest{}
var _ framework.FilterPlugin = &LeastRequest{}

type LeastRequest struct {
	name               string
	maxWaitingRequests int
}

type LeastRequestArgs struct {
	// MaxWaitingRequests filters out pods whose engine-reported waiting-queue
	// depth exceeds this value. It captures backpressure that the router cannot
	// observe directly (e.g. requests queued inside the engine before execution).
	MaxWaitingRequests int `yaml:"maxWaitingRequests,omitempty"`
}

func NewLeastRequest(pluginArg runtime.RawExtension) *LeastRequest {
	var leastRequestArgs LeastRequestArgs
	if pluginArg.Raw == nil || yaml.Unmarshal(pluginArg.Raw, &leastRequestArgs) != nil {
		klog.Errorf("Unmarshal LeastRequestArgs error, setting default value")
		leastRequestArgs = LeastRequestArgs{
			MaxWaitingRequests: 10,
		}
	}
	if leastRequestArgs.MaxWaitingRequests == 0 {
		leastRequestArgs.MaxWaitingRequests = 10
	}

	return &LeastRequest{
		name:               LeastRequestPluginName,
		maxWaitingRequests: leastRequestArgs.MaxWaitingRequests,
	}
}

func (l *LeastRequest) Name() string {
	return l.name
}

func (l *LeastRequest) Filter(ctx *framework.Context, pods []*datastore.PodInfo) []*datastore.PodInfo {
	return slices.FilterInPlace(pods, func(info *datastore.PodInfo) bool {
		// Filter on engine-reported waiting queue: catches backlog the router
		// cannot observe (requests already inside the engine but not yet running).
		return info.GetRequestWaitingNum() < float64(l.maxWaitingRequests)
	})
}

// Score 公式: base = onFlight + 100 × max(onFlight - running, 0), 越小分越高(归一化到[0,100])
//
//   - onFlight: router 自记录的在途请求(已分发未响应), 零延迟更新,
//     充当当前 Pod 负载的实时代理, 避开引擎 /metrics ~1s 轮询延迟。
//   - max(onFlight - running, 0): 估算"已分发但引擎还没开始执行"的排队请求数。
//     ×100 强烈惩罚队列正在堆积的 Pod — 这种 Pod 已经是瓶颈, 不应再往它塞请求。
//   - 归一化: (maxScore - base)/maxScore × 100, 负载最低的 Pod 得 100 分。
//   - running 的 ~1s 延迟可能短暂高估排队量, 但作为"队列堆积的前瞻信号"可接受。
func (l *LeastRequest) Score(ctx *framework.Context, pods []*datastore.PodInfo) map[*datastore.PodInfo]int {
	scoreResults := make(map[*datastore.PodInfo]int)
	if len(pods) == 0 {
		return scoreResults
	}

	// Score formula: base = onFlight + 100 * max(onFlight - running, 0)
	//
	//   - onFlight: router-tracked in-flight count, updated with zero delay.
	//     Acts as a proxy for current pod load and avoids the ~1 s engine-metrics
	//     poll lag.
	//   - max(onFlight - running, 0): estimates the number of requests that the
	//     router has dispatched but the engine has not yet started executing
	//     (i.e. likely queued inside the engine). Weighted ×100 to strongly
	//     penalise pods whose queue is building up.
	baseScores := make(map[*datastore.PodInfo]float64)
	maxScore := 0.0
	for _, info := range pods {
		// Estimate queued requests as max(onFlight - running, 0). The engine-reported
		// running count has a poll lag (~1 s), so this may briefly over-count, but
		// it provides a leading indicator of queue build-up at the pod.
		base := float64(info.GetOnFlightRequestNum()) + 100*math.Max(float64(info.GetOnFlightRequestNum())-float64(info.GetRequestRunningNum()), 0)
		baseScores[info] = base
		if base > maxScore {
			maxScore = base
		}
	}

	// Normalise to [0, 100]: the least-loaded pod gets 100.
	for _, info := range pods {
		score := 100.0
		if maxScore > 0 {
			score = ((maxScore - baseScores[info]) / maxScore) * 100
		}
		scoreResults[info] = int(score)
	}

	return scoreResults
}
