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
	"github.com/volcano-sh/kthena/pkg/kthena-router/datastore"
	"github.com/volcano-sh/kthena/pkg/kthena-router/scheduler/framework"
	"github.com/volcano-sh/kthena/pkg/kthena-router/scheduler/plugins"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
)

// =============================================================================
// 插件工厂与注册表 — 把"配置里的插件名"变成"运行时插件实例"
// =============================================================================
//
// 【为什么需要工厂】
//   ConfigMap 里只有插件名 + 权重 + 参数(JSON), 运行时要根据名字 new 出实例。
//   PluginRegistry 就是 name→工厂函数 的映射表, NewScheduler 启动时:
//     registerDefaultPlugins() 注册所有内置插件 →
//     getScorePlugins()/getFilterPlugins() 按配置名查表实例化
//
// 【两种工厂签名差异】
//   ScorePluginBuilder 需要 store: 因为 prefix-cache/kvcache-aware 要写缓存或查 Redis
//   FilterPluginBuilder 不需要 store: 纯逻辑过滤 (lora-affinity 只看 Pod 元数据)
//
// 【内置插件清单】(见 registerDefaultPlugins)
//   Score:   gpu-usage / least-latency / least-request / random / prefix-cache / kvcache-aware
//   Filter:  least-request / lora-affinity
// =============================================================================

// ScorePluginBuilder: Score 插件工厂函数签名。store 供需要数据访问的插件用 (如 prefix-cache)
type ScorePluginBuilder = func(store datastore.Store, arg runtime.RawExtension) framework.ScorePlugin
// FilterPluginBuilder: Filter 插件工厂函数签名。无需 store, 纯逻辑过滤
type FilterPluginBuilder = func(arg runtime.RawExtension) framework.FilterPlugin

// PluginRegistry 插件注册表 — 持有 score/filter 两类插件的工厂函数映射
type PluginRegistry struct {
	scorePluginBuilders  map[string]ScorePluginBuilder
	filterPluginBuilders map[string]FilterPluginBuilder
}

// NewPluginRegistry 创建空注册表
func NewPluginRegistry() *PluginRegistry {
	return &PluginRegistry{
		scorePluginBuilders:  make(map[string]ScorePluginBuilder),
		filterPluginBuilders: make(map[string]FilterPluginBuilder),
	}
}

// registerScorePlugin 注册一个 Score 插件工厂到注册表 (name → builder)
func (r *PluginRegistry) registerScorePlugin(name string, sp ScorePluginBuilder) {
	r.scorePluginBuilders[name] = sp
}

// getScorePlugin 按名称获取 Score 插件工厂
func (r *PluginRegistry) getScorePlugin(name string) (ScorePluginBuilder, bool) {
	sp, exist := r.scorePluginBuilders[name]
	return sp, exist
}

// registerFilterPlugin 注册一个 Filter 插件工厂到注册表 (name → builder)
func (r *PluginRegistry) registerFilterPlugin(name string, fp FilterPluginBuilder) {
	r.filterPluginBuilders[name] = fp
}

// getFilterPlugin 按名称获取 Filter 插件工厂
func (r *PluginRegistry) getFilterPlugin(name string) (FilterPluginBuilder, bool) {
	fp, exist := r.filterPluginBuilders[name]
	return fp, exist
}

// registerDefaultPlugins 注册所有内置插件到注册表。
// 这张表是"可用插件全集", 实际启用哪些由 ConfigMap 决定 (见 NewScheduler 的 getScorePlugins)。
//
//   Score 插件 (6 个):
//     gpu-usage       — 按 GPU cache 利用率打分, 利用率越低分越高
//     least-latency   — 按 TTFT/TPOT 加权延迟打分, 越低分越高
//     least-request   — 按 Pod 在途请求数打分, 越少分越高
//     random          — 随机打分, 仅测试用 (配置时会自动避免与其他 Score 共存, 见 conf.go)
//     prefix-cache    — 按 prompt 前缀 hash 命中 KV Cache 块数打分
//     kvcache-aware   — 基于 Redis 的 token 级 KV Cache 前缀匹配打分
//
//   Filter 插件 (2 个):
//     least-request   — 过滤在途等待请求 ≥ maxWaitingRequests 的 Pod (与同名 Score 插件共享实现)
//     lora-affinity    — 过滤不支持请求中 LoRA 适配器的 Pod
func registerDefaultPlugins(registry *PluginRegistry) {
	// scorePlugin
	registry.registerScorePlugin(plugins.GPUCacheUsagePluginName, func(_ datastore.Store, args runtime.RawExtension) framework.ScorePlugin {
		return plugins.NewGPUCacheUsage()
	})
	registry.registerScorePlugin(plugins.LeastLatencyPluginName, func(_ datastore.Store, args runtime.RawExtension) framework.ScorePlugin {
		return plugins.NewLeastLatency(args)
	})
	registry.registerScorePlugin(plugins.LeastRequestPluginName, func(_ datastore.Store, args runtime.RawExtension) framework.ScorePlugin {
		return plugins.NewLeastRequest(args)
	})
	registry.registerScorePlugin(plugins.RandomPluginName, func(_ datastore.Store, args runtime.RawExtension) framework.ScorePlugin {
		return plugins.NewRandom(args)
	})
	registry.registerScorePlugin(plugins.PrefixCachePluginName, func(store datastore.Store, args runtime.RawExtension) framework.ScorePlugin {
		return plugins.NewPrefixCache(store, args) // 需要 store: prefix-cache 用 datastore 缓存
	})

	registry.registerScorePlugin(plugins.KVCacheAwarePluginName, func(_ datastore.Store, args runtime.RawExtension) framework.ScorePlugin {
		return plugins.NewKVCacheAware(args) // 不传 store: 自行创建 Redis client
	})
	// filterPlugin
	registry.registerFilterPlugin(plugins.LeastRequestPluginName, func(args runtime.RawExtension) framework.FilterPlugin {
		return plugins.NewLeastRequest(args)
	})
	registry.registerFilterPlugin(plugins.LoraAffinityPluginName, func(args runtime.RawExtension) framework.FilterPlugin {
		return plugins.NewLoraAffinity()
	})
}

// getFilterPlugins 按配置的插件名列表, 从注册表实例化 Filter 插件。
// filterPluginMap 来自 ConfigMap 的 enabled 列表, pluginsArgMap 是各插件的参数 JSON。
// TODO: lora-affinity 等 models 来自 metrics 可用后再默认启用
func getFilterPlugins(registry *PluginRegistry, filterPluginMap []string, pluginsArgMap map[string]runtime.RawExtension) []framework.FilterPlugin {
	var list []framework.FilterPlugin
	// TODO: enable lora affinity when models from metrics are available.
	for _, pluginName := range filterPluginMap {
		if builderFunc, exist := registry.getFilterPlugin(pluginName); !exist {
			klog.Errorf("Failed to get plugin %s.", pluginName) // 配置里写了不存在的插件名
			continue
		} else {
			plugin := builderFunc(pluginsArgMap[pluginName])
			if plugin != nil {
				list = append(list, plugin)
			}
		}
	}
	return list
}

// getScorePlugins 按配置的 map[插件名]权重, 从注册表实例化 Score 插件 (含权重校验)。
// weight < 0 视为非法, 强制改为 0 (即不参与打分, 但保留实例)。
// map 遍历顺序非确定, 但不影响结果 — 各插件独立打分最后加权累加, 顺序无关
func getScorePlugins(registry *PluginRegistry, store datastore.Store, scorePluginMap map[string]int, pluginsArgMap map[string]runtime.RawExtension) []*scorePlugin {
	var list []*scorePlugin
	for pluginName, weight := range scorePluginMap {
		if weight < 0 {
			klog.Errorf("Weight for plugin '%s' is invalid, value is %d. Setting to 0", pluginName, weight)
			weight = 0
		}

		if builderFunc, exist := registry.getScorePlugin(pluginName); !exist {
			klog.Errorf("Failed to get plugin %s.", pluginName)
		} else {
			plugin := builderFunc(store, pluginsArgMap[pluginName])
			if plugin != nil {
				list = append(list, &scorePlugin{
					plugin: plugin,
					weight: weight,
				})
			}
		}
	}
	return list
}

// getPostScheduleHooks 从已实例化的 Score 插件中, 提取实现了 PostScheduleHook 接口的。
// 一个插件可以同时是 ScorePlugin 和 PostScheduleHook (如 prefix-cache: 既打分又写缓存)。
// 通过类型断言探测, 而非单独配置 — 只要插件实现了 PostSchedule() 就会被纳入后处理链
func getPostScheduleHooks(scorePlugins []*scorePlugin) []framework.PostScheduleHook {
	var hooks []framework.PostScheduleHook
	for _, scorePlugin := range scorePlugins {
		if hook, ok := scorePlugin.plugin.(framework.PostScheduleHook); ok {
			hooks = append(hooks, hook)
		}
	}
	return hooks
}
