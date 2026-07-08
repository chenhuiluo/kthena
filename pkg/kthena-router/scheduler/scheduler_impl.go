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

package scheduler

import (
	"fmt"
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"

	"github.com/volcano-sh/kthena/pkg/kthena-router/datastore"
	"github.com/volcano-sh/kthena/pkg/kthena-router/metrics"
	"github.com/volcano-sh/kthena/pkg/kthena-router/scheduler/framework"
	"github.com/volcano-sh/kthena/pkg/kthena-router/scheduler/plugins"
	"github.com/volcano-sh/kthena/pkg/kthena-router/scheduler/plugins/conf"
)

// =============================================================================
// 调度器实现 — 仿照 kube-scheduler 的 Filter → Score → TopN → PostHook 管线
// =============================================================================
//
// 【与 kube-scheduler 的对应关系】
//
//   kube-scheduler              kthena-router scheduler         说明
//   ─────────────────           ──────────────────────         ────
//   PreFilter                   (无)                           无需预过滤
//   Filter                      RunFilterPlugins()             硬约束: Pod 能不能处理此请求
//   PreScore                    (无)                           无需预打分
//   Score                       RunScorePlugins()              软偏好: Pod 好不好 (加权打分)
//   NormalizeScore              (内嵌于 Score)                 各插件自行归一化到 [0,100]
//   Select                      TopNPodInfos()                 取 Top5 而非只选1个 (允许重试)
//   PostFilter                  (无)                           Filter 全部剔除时直接返回错误
//   Permit / Reserve / Bind     (无)                           无需 K8s 调度周期
//   PostBind                    RunPostHooks()                 请求成功后更新缓存状态
//
// 【Schedule() 主流程】
//   1. [syncOnFlight] 从 Redis 同步跨 router 在途计数 (仅 least-request 启用时)
//   2. RunFilterPlugins():  逐一运行 Filter 插件, 不合格的 Pod 被剔除
//   3. PD 分离检测: 若 ctx.PDGroup != nil, 分 Pod 为 decodePods + prefillPods
//   4a.[同构] RunScorePlugins() → TopNPodInfos(topN) → ctx.BestPods
//   4b.[PD分离] 对 decodePods + prefillPods 分别 RunScorePlugins() → TopN
//   5. RunPostHooks(): 请求成功后执行后处理 (如 prefix-cache 写缓存)
//
// 【同构场景】
//   Schedule() 填充 ctx.BestPods → 上层调 proxyModelEndpoint()
//
// 【PD 分离场景】
//   Schedule() 分别填充 ctx.DecodePods(Top5) 和 ctx.PrefillPods(每个 Decode 配 1 个 Prefill)
//   → 上层调 proxyToPDDisaggregated()
//
// 【打分公式】
//   总分 = Σ (plugin_score × plugin_weight)
//   plugin_score 范围 [0, 100], 权重默认 1
//   多个 Score 插件的加权总分用于 TopN 排序
// =============================================================================

const (
	// topN = 5: 从所有候选 Pod 中取加权总分最高的前 5 个 Pod
	// 选择 5 个而非 1 个是为了让上层 proxy() 函数有重试空间:
	// 如果第 1 个 Pod 代理失败,可以换第 2 个 Pod 重试,以此类推
	topN = 5
)

// SchedulerImpl 是调度器的具体实现,持有 Filter/Score/PostHook 插件列表
type SchedulerImpl struct {
	// store: DataStore,提供 Pod 列表/ModelServer 信息/PD 分组查询等
	store datastore.Store

	// filterPlugins: Filter 插件列表 — 过滤不合格的 Pod
	// 任一 Filter 返回剔除 → 该 Pod 不进入 Score 阶段
	// 已有插件: least-request (队列深度过滤), lora-affinity (适配器匹配)
	filterPlugins []framework.FilterPlugin

	// scorePlugins: Score 插件列表 (含权重) — 对通过过滤的 Pod 打分
	// 各自打分 [0,100] × 权重 → 加权总分用于 TopN 排序
	// 已有插件: least-request, least-latency, prefix-cache, kvcache-aware, gpu-usage, random
	scorePlugins []*scorePlugin

	// postScheduleHooks: PostHook 插件列表 — 调度完成后执行后处理
	// 如 prefix-cache 插件在代理成功后将 prompt hash 写入缓存
	postScheduleHooks []framework.PostScheduleHook

	// syncOnFlight: 是否在 Schedule() 开始前同步 Redis 中的在途请求计数
	// 仅当 least-request 插件启用时为 true (需要跨 router 共享计数)
	syncOnFlight bool
}

// scorePlugin 封装 Score 插件及其权重
// 最终权重 = plugin.Score() × weight
// 默认权重为 1,可通过 ConfigMap 调整 (如 prefix-cache 设为 2 以优先 prefix 命中)
type scorePlugin struct {
	plugin framework.ScorePlugin
	weight int
}

// podInfoWithValue 是 TopN 排序用的中间结构,关联 Pod 和其加权总分
type podInfoWithValue struct {
	pod   *datastore.PodInfo
	score int
}

// NewScheduler 创建并初始化调度器
// 参数:
//   - store: DataStore,提供集群状态查询
//   - routerConfig: 路由配置 (从 ConfigMap 解析),包含启用的插件列表+权重+参数
//
// 流程:
//   1. 创建插件注册表 + 注册默认插件 (least-request, prefix-cache 等)
//   2. 如果 routerConfig 为 nil,使用硬编码默认配置 (3 score + 1 filter)
//   3. 从 routerConfig 加载插件配置 (启用列表+权重+参数)
//   4. 检测 least-request 是否启用 (决定是否需要 syncOnFlight)
//   5. 实例化各插件并组装 SchedulerImpl
func NewScheduler(store datastore.Store, routerConfig *conf.RouterConfiguration) Scheduler {
	// 创建插件注册表 (name → 工厂函数)
	registry := NewPluginRegistry()
	// 注册所有内置插件到注册表
	registerDefaultPlugins(registry)

	// 默认插件配置 — 当 ConfigMap 不存在时的 fallback
	scorePluginMap := map[string]int{
		"least-request": 1, // 权重 1: 按 Pod 在途请求数打分,越少分越高
		"least-latency": 1, // 权重 1: 按 TTFT/TPOT 延迟打分,越低分越高
		"prefix-cache":  1, // 权重 1: 按 prefix cache 命中率打分,命中越多分越高
	}
	filterPluginMap := []string{
		"least-request", // 过滤掉队列深度超限的 Pod
	}
	pluginsArgMap := map[string]runtime.RawExtension{
		"least-request": {Raw: []byte(`{"maxWaitingRequests": 10}`)}, // 队列深度阈值 10
		"least-latency": {Raw: []byte(`{"TTFTTPOTWeightFactor": 0.5}`)}, // TTFT/TPOT 权重因子
		"prefix-cache":  {Raw: []byte(`{"blockSizeToHash": 64, "maxBlocksToMatch": 128, "maxHashCacheSize": 50000, "topKMatches": 5}`)},
	}

	var err error
	if routerConfig == nil {
		// 无 ConfigMap → 使用硬编码默认配置
		klog.Warning("No scheduler configuration found, using default configuration")
	} else {
		// 有 ConfigMap → 解析配置覆盖默认值
		scorePluginMap, filterPluginMap, pluginsArgMap, err = conf.LoadSchedulerConfig(&routerConfig.Scheduler)
		if err != nil {
			klog.Fatalf("failed to Load Scheduler: %v", err)
		}
	}

	// 检测 least-request 是否在 filter 或 score 中启用
	// 如果启用,需要 syncOnFlight=true,在 Schedule() 开始前从 Redis 同步跨 router 在途计数
	leastRequestEnabled := false
	for _, name := range filterPluginMap {
		if name == plugins.LeastRequestPluginName {
			leastRequestEnabled = true
			break
		}
	}
	if !leastRequestEnabled {
		for name := range scorePluginMap {
			if name == plugins.LeastRequestPluginName {
				leastRequestEnabled = true
				break
			}
		}
	}

	// 从注册表实例化 Score 插件 (传入 DataStore + 插件参数)
	scorePlugins := getScorePlugins(registry, store, scorePluginMap, pluginsArgMap)
	return &SchedulerImpl{
		store:             store,
		filterPlugins:     getFilterPlugins(registry, filterPluginMap, pluginsArgMap),
		scorePlugins:      scorePlugins,
		postScheduleHooks: getPostScheduleHooks(scorePlugins), // 从 Score 插件中提取实现了 PostScheduleHook 的
		syncOnFlight:      leastRequestEnabled,
	}
}

// Schedule 是调度器的主入口 — 对应 kube-scheduler 的 Filter → Score → Select 阶段
//
//   kube-scheduler 流程:  Pod → PreFilter → Filter → PreScore → Score → NormalizeScore → Select → Bind
//   kthena-router 流程:  Pod → [syncOnFlight] → RunFilter → RunScore → TopN → (proxy成功后) RunPostHooks
// 同构场景: 填充 ctx.BestPods (Top5 Pod)
// PD 分离: 填充 ctx.DecodePods (Top5) + ctx.PrefillPods (每个 Decode 配 1 个 Prefill)
//
// 完整流程:
//   1. [syncOnFlight] 从 Redis 同步跨 router 在途请求计数 (仅 least-request 启用时)
//   2. RunFilterPlugins(): 逐一运行 Filter 插件,不合格 Pod 被剔除
//   3. PD 分离检测: ctx.PDGroup != nil 时走 PD 分支,否则走同构分支
//   4a. [同构] RunScorePlugins() → TopNPodInfos(topN) → ctx.BestPods
//   4b. [PD 分离]
//       - store.GetDecodePods(): 获取该 ModelServer 的所有 decode Pod
//       - RunScorePlugins(decodePods): 对 decode Pod 打分
//       - TopNPodInfos(topN): 取 Top5 decode Pod → ctx.DecodePods
//       - 对每个 Top5 decode Pod:
//         - store.GetPrefillPodsForDecodeGroup(): 查找同 PDGroup 的 prefill Pod
//         - RunScorePlugins(prefillPods): 对 prefill Pod 打分
//         - TopNPodInfos(1): 取最优 1 个 prefill Pod
//       - 校验至少有 1 对有效 (decode, prefill)
func (s *SchedulerImpl) Schedule(ctx *framework.Context, pods []*datastore.PodInfo) error {
	// 如果 least-request 插件启用,先从 Redis 同步在途请求计数
	// 这确保了跨 router 副本的全局负载视图一致
	// Rate-limited internally: 不会每次 Schedule 都调用 Redis,有内部限频
	if s.syncOnFlight {
		s.store.SyncOnFlightCounts()
	}

	// ===== 第 1 阶段: Filter — 过滤不合格 Pod =====
	pods, err := s.RunFilterPlugins(pods, ctx)
	if err != nil {
		return err
	}

	// ===== PD 分离分支 =====
	if ctx.PDGroup != nil {
		klog.V(4).Info("Using optimized PD disaggregated scheduling")

		// 从 DataStore 获取该 ModelServer 的所有 decode Pod (O(1) 查找)
		// DataStore 内部已按 PDGroup.GroupKey 分类存储
		decodePods, err := s.store.GetDecodePods(ctx.ModelServerName)
		if err != nil {
			return fmt.Errorf("failed to get decode pods: %v", err)
		}

		if len(decodePods) == 0 {
			return fmt.Errorf("no decode pod found")
		}

		// 对 decode Pod 运行 Score 插件打分
		klog.V(4).Info("Running score plugins for decode pod")
		scores := s.RunScorePlugins(decodePods, ctx)

		// 取 Top5 decode Pod
		topNDecodePods := TopNPodInfos(scores, topN)
		ctx.DecodePods = topNDecodePods
		prefillPods := make([]*datastore.PodInfo, len(topNDecodePods))
		validPairs := 0

		// 为每个 Top5 decode Pod 配对一个最优 prefill Pod
		for i, decodePod := range ctx.DecodePods {
			decodePodName := decodePod.GetPodNamespacedName()
			if decodePodName.Name == "" {
				continue
			}
			// 查找与 decode Pod 同 PDGroup 的 prefill Pod (O(1) 查找)
			// PDGroup.GroupKey 相同的 Pod 属于同一分组
			selectedPods, err := s.store.GetPrefillPodsForDecodeGroup(ctx.ModelServerName, decodePodName)
			if err != nil || len(selectedPods) == 0 {
				klog.V(4).InfoS("prefill pods for decode group not found", "decode instance", decodePodName, "error", err)
				continue
			}

			// 对 prefill Pod 运行 Score 插件打分
			klog.V(4).Info("Running score plugins for prefill pod")
			scores = s.RunScorePlugins(selectedPods, ctx)
			bestPrefillPod := TopNPodInfos(scores, 1)
			if len(bestPrefillPod) == 0 {
				klog.V(4).InfoS("no valid prefill pods after scoring, skipping",
					"decode instance", decodePodName)
				continue
			}
			prefillPods[i] = bestPrefillPod[0]
			validPairs++
		}
		ctx.PrefillPods = prefillPods
		// 至少需要 1 对有效的 (decode, prefill) 才能进行 PD 分离推理
		if validPairs == 0 {
			return fmt.Errorf("no valid prefill-decode pod pairs found")
		}

		return nil
	}

	// ===== 同构/PD 聚合分支 =====
	klog.V(4).Info("Running score plugins for PD aggregated pod")
	scores := s.RunScorePlugins(pods, ctx)
	// 取 Top5 Pod 作为候选,供上层 proxy() 函数逐个尝试代理
	ctx.BestPods = TopNPodInfos(scores, topN)

	return nil
}

// RunFilterPlugins 逐一运行所有 Filter 插件 — 对应 kube-scheduler 的 Filter 阶段
//
//   kube-scheduler:   每个 Pod 对每个 Node 逐一调用 FilterPlugin.Filter()，返回成功/失败
//   kthena-router:    每个 Filter 插件接收完整 Pod 列表，批量过滤后返回新列表 (更高效)
//
// 过滤规则: 任一 Filter 返回的 Pod 列表不包含某 Pod → 该 Pod 被剔除
// 过滤规则: 任一 Filter 返回的 Pod 列表不包含某 Pod → 该 Pod 被剔除
// 如果某个 Filter 把所有 Pod 都过滤掉了,直接返回错误 (避免后续无 Pod 可调度)
//
// 执行顺序: 按 filterPlugins 数组顺序 (即 ConfigMap 中的配置顺序)
// 插件间是串联关系: 前一个 Filter 的输出 = 后一个 Filter 的输入
//
// 已有 Filter 插件:
//   - least-request: 过滤掉在途等待请求 ≥ maxWaitingRequests 的 Pod (默认 10)
//   - lora-affinity: 过滤掉不支持请求中 LoRA 适配器的 Pod
func (s *SchedulerImpl) RunFilterPlugins(pods []*datastore.PodInfo, ctx *framework.Context) ([]*datastore.PodInfo, error) {
	for _, filterPlugin := range s.filterPlugins {
		// 记录插件执行耗时 (用于 Prometheus 指标)
		startTime := time.Now()
		pods = filterPlugin.Filter(ctx, pods)
		duration := time.Since(startTime)

		// 写入调度器插件延迟指标
		if ctx.MetricsRecorder != nil {
			ctx.MetricsRecorder.RecordSchedulerPluginDuration(filterPlugin.Name(), metrics.PluginTypeFilter, duration)
		}

		// 全部被过滤掉 → 返回错误,避免无 Pod 可用
		if len(pods) == 0 {
			return nil, fmt.Errorf("pods have all been filtered out by %q", filterPlugin.Name())
		}
	}

	return pods, nil
}

// RunScorePlugins 逐一运行所有 Score 插件 — 对应 kube-scheduler 的 Score + NormalizeScore 阶段
//
//   kube-scheduler:   ScorePlugin.Score() → NormalizeScore() → × weight → 加权总分
//   kthena-router:    ScorePlugin.Score() (已含归一化到 [0,100]) → × weight → 加权总分
//
// 打分公式:
//   finalScore(pod) = Σ plugin_i.Score(ctx, pods)[pod] × weight_i
//
//
// 打分公式:
//   总分[pod] = Σ (plugin.Score(ctx, pods)[pod] × plugin.Weight)
//
// 每个插件的 Score() 方法返回 map[*PodInfo]int,值范围 [0, 100]:
//   0 = 最差 (如 Pod 队列已满)
//   100 = 最优 (如 prefix cache 100% 命中)
//
// 权重 (Weight) 默认为 1,可通过 ConfigMap 调整:
//   如 "prefix-cache": 2 表示 prefix-cache 插件的分数权重翻倍
//
// 已有 Score 插件:
//   - least-request: 按 Pod 在途请求数打分,越少分越高
//   - least-latency: 按 TTFT/TPOT 加权延迟打分,越低分越高
//   - prefix-cache: 按 prefix hash 匹配 KV Cache 块数打分,命中越多分越高
//   - kvcache-aware: 按 KV Cache 利用率打分
//   - gpu-usage: 按 GPU 利用率打分
//   - random: 随机打分 (用于测试)
func (s *SchedulerImpl) RunScorePlugins(pods []*datastore.PodInfo, ctx *framework.Context) map[*datastore.PodInfo]int {
	res := make(map[*datastore.PodInfo]int)
	for _, scorePlugin := range s.scorePlugins {
		// 记录插件执行耗时
		startTime := time.Now()
		scores := scorePlugin.plugin.Score(ctx, pods)
		duration := time.Since(startTime)

		// 写入调度器插件延迟指标
		if ctx.MetricsRecorder != nil {
			ctx.MetricsRecorder.RecordSchedulerPluginDuration(scorePlugin.plugin.Name(), metrics.PluginTypeScore, duration)
		}

		klog.V(4).Infof("ScorePlugin: %s", scorePlugin.plugin.Name())
		// 将各插件打分 × 权重 累加到总分
		for k, v := range scores {
			if podName := k.GetPodNamespacedName(); podName.Name != "" {
				klog.V(4).Infof("Pod: %s/%s, Score: %d", podName.Namespace, podName.Name, v)
			}
			if _, ok := res[k]; !ok {
				res[k] = v * scorePlugin.weight // 首次出现: 直接赋值加权分
			} else {
				res[k] += v * scorePlugin.weight // 已有: 累加加权分
			}
		}
	}

	// 日志输出最终加权总分 (用于调试)
	if klog.V(4).Enabled() {
		klog.Info("Final Pod Scores:")
		for k, v := range res {
			if podName := k.GetPodNamespacedName(); podName.Name != "" {
				klog.Infof("  Pod: %s/%s, Final Score: %d", podName.Namespace, podName.Name, v)
			}
		}
	}

	return res
}

// RunPostHooks 在调度完成后执行所有 PostScheduleHook — 对应 kube-scheduler 的 PostBind 阶段
//
//   kube-scheduler:   Pod 绑定到 Node 后 → PostBind() (如更新缓存)
//   kthena-router:    请求代理成功后 → PostSchedule() (如 prefix-cache 写入 LRU 缓存)
//
// 参数 index 是本次代理在 TopN 列表中的索引,用于定位当前使用的 Pod
// 已有 PostHook: prefix-cache (将 prompt hash 写入缓存,加速同 session 后续请求)
func (s *SchedulerImpl) RunPostHooks(ctx *framework.Context, index int) {
	for _, hook := range s.postScheduleHooks {
		hook.PostSchedule(ctx, index)
	}
}

// TopNPodInfos 从 Pod→分数 映射中取加权总分最高的前 N 个 Pod — 对应 kube-scheduler 的 Select 阶段
//
//   kube-scheduler:   只选 1 个最优 Node (通过 PrioritySort 排序)
//   kthena-router:    取 Top5 Pod (允许重试，第1个失败可换第2个)
//
// 用于 Schedule() 的最后一步: 将所有 Score 插件的加权总分排序后取 Top5
// 排序规则: 分数降序 (score 越高 = 越优)
func TopNPodInfos(m map[*datastore.PodInfo]int, n int) []*datastore.PodInfo {
	var list []podInfoWithValue
	for k, v := range m {
		list = append(list, podInfoWithValue{pod: k, score: v})
	}

	sort.Slice(list, func(i, j int) bool {
		return list[i].score > list[j].score
	})

	res := []*datastore.PodInfo{}
	for i := range list {
		if i >= n {
			break
		}
		res = append(res, list[i].pod)
	}

	return res
}
