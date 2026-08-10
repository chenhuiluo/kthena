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

package conf

import (
	"fmt"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"sigs.k8s.io/yaml"
)

// =============================================================================
// 调度器配置解析 — 把 ConfigMap YAML 解析成运行时可用的插件配置
// =============================================================================
//
// 【配置来源】ConfigMap YAML, 路径由 routerConfigPath 传入 NewRouter()
//
// 【YAML 结构示例】
//   scheduler:
//     pluginConfig:                  # 各插件参数 (name → args JSON)
//       - name: least-request
//         args:
//           maxWaitingRequests: 10
//       - name: prefix-cache
//         args:
//           blockSizeToHash: 64
//     plugins:
//       Filter:
//         enabled: [least-request]    # 启用的 Filter 插件
//         disabled: [lora-affinity]
//       Score:
//         enabled:                    # 启用的 Score 插件 + 权重
//           - name: least-request
//             weight: 1
//           - name: prefix-cache
//             weight: 2
//         disabled: []
//
// 【解析流程】LoadSchedulerConfig():
//   1. unmarshalPlugins():    Score.Enabled → map[name]weight, Filter.Enabled → []name
//   2. handleRandomPluginConflicts(): random 与其他 Score 共存时删除 random (见其注释)
//   3. unmarshalPluginsConfig(): PluginConfig → map[name]RawExtension (插件参数)
//   返回三个 map/列表供 NewScheduler 实例化插件用
// =============================================================================

// RouterConfiguration 路由器顶层配置, 含调度器与鉴权两部分
type RouterConfiguration struct {
	Scheduler SchedulerConfiguration `yaml:"scheduler"`
	Auth      AuthenticationConfig   `yaml:"auth"`
}

// SchedulerConfiguration 调度器配置: 启用的插件列表 + 各插件参数
type SchedulerConfiguration struct {
	PluginConfig []PluginConfig `yaml:"pluginConfig"` // 插件参数列表
	Plugins      Plugins        `yaml:"plugins"`      // 启用/禁用的插件
}

// Plugins 分 Filter/Score 两类插件配置
type Plugins struct {
	Filter Filter `yaml:"Filter"`
	Score  Score  `yaml:"Score"`
}

// Filter 插件启用/禁用列表 (仅名称, Filter 无权重)
type Filter struct {
	Enabled  []string `yaml:"enabled"`
	Disabled []string `yaml:"disabled"`
}

// Score 插件启用/禁用列表 (含权重, 打分时 ×weight)
type Score struct {
	Enabled  []PluginWithWeight `yaml:"enabled"`
	Disabled []PluginWithWeight `yaml:"disabled"`
}

// PluginWithWeight 插件名 + 权重 (权重默认 1, <0 会被 getScorePlugins 改为 0)
type PluginWithWeight struct {
	Name   string `yaml:"name"`
	Weight int    `yaml:"weight"`
}

// PluginConfig 单个插件的参数 (args 为原始 JSON, 由各插件的 New 函数自行 unmarshal)
type PluginConfig struct {
	Name string               `yaml:"name"`
	Args runtime.RawExtension `yaml:"args,omitempty"`
}

// AuthenticationConfig JWT 鉴权配置 (Issuer/Audiences/JWKS URI)
type AuthenticationConfig struct {
	Issuer    string   `yaml:"issuer"`
	Audiences []string `yaml:"audiences"`
	JwksUri   string   `yaml:"jwksUri"`
}

// ParseRouterConfig 从文件读取并反序列化整个 RouterConfiguration。
// configMapPath 是 ConfigMap 挂载到容器内的 YAML 文件路径。
func ParseRouterConfig(configMapPath string) (*RouterConfiguration, error) {
	data, err := os.ReadFile(configMapPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", configMapPath, err)
	}
	var routerConfig RouterConfiguration
	if err := yaml.Unmarshal(data, &routerConfig); err != nil {
		klog.Errorf("failed to Unmarshal routerConfiguration: %v", err)
		return nil, fmt.Errorf("failed to Unmarshal routerConfiguration: %v", err)
	}
	return &routerConfig, nil
}

// LoadSchedulerConfig 解析调度器配置, 返回三样东西供 NewScheduler 用:
//   - scorePluginMap:  map[插件名]权重 (用于实例化 Score 插件)
//   - filterPlugins:   []插件名 (用于实例化 Filter 插件)
//   - pluginsArgMap:   map[插件名]RawExtension (各插件的参数 JSON)
func LoadSchedulerConfig(schedulerConfig *SchedulerConfiguration) (map[string]int, []string, map[string]runtime.RawExtension, error) {
	if schedulerConfig == nil {
		return nil, nil, nil, fmt.Errorf("schedulerConfig is nil")
	}

	scorePluginMap, filterPlugins, err := unmarshalPlugins(schedulerConfig)
	if err != nil {
		klog.Errorf("failed to Unmarshal Plugins: %v", err)
		return nil, nil, nil, fmt.Errorf("failed to Unmarshal Plugins: %v", err)
	}

	// 处理 random 插件冲突: random 与其他 Score 共存时移除 random
	scorePluginMap = handleRandomPluginConflicts(scorePluginMap)

	pluginsArgMap, err := unmarshalPluginsConfig(schedulerConfig)
	if err != nil {
		klog.Errorf("failed to Unmarshal PluginsConfig: %v", err)
		return nil, nil, nil, fmt.Errorf("failed to Unmarshal PluginsConfig: %v", err)
	}

	return scorePluginMap, filterPlugins, pluginsArgMap, nil
}

// handleRandomPluginConflicts 检查 random 插件是否与其他 Score 插件共存。
//
// why: random 是随机打分, 仅用于测试/压测。若与 least-request/prefix-cache 等有意义插件混用,
//   随机分会污染加权总分 — 一个本该被调度到 prefix 命中 Pod 的请求可能被 random 推到任意 Pod,
//   "智能调度"失去意义。故 random 必须独立使用, 混用时自动移除并告警。
func handleRandomPluginConflicts(scorePluginMap map[string]int) map[string]int {
	const randomPluginName = "random"

	// Check if random plugin exists using direct map lookup
	_, hasRandomPlugin := scorePluginMap[randomPluginName]

	// Check if there are other plugins besides random
	hasOtherScorePlugins := len(scorePluginMap) > 1 && hasRandomPlugin

	// If both random and other score plugins are configured, remove random plugin and warn
	if hasRandomPlugin && hasOtherScorePlugins {
		klog.Warningf("Random plugin is configured along with other score plugins. Random plugin will be removed as it should be used independently. " +
			"Mixing random scores with meaningful scores defeats the purpose of intelligent scheduling.")

		delete(scorePluginMap, randomPluginName)
	}

	return scorePluginMap
}

// unmarshalPlugins 从配置提取插件启用列表:
//   Score.Enabled  → map[name]weight (权重参与加权打分)
//   Filter.Enabled → []name (Filter 无权重, 串联执行)
func unmarshalPlugins(schedulerConfig *SchedulerConfiguration) (map[string]int, []string, error) {
	var filterPlugins []string
	scorePluginMap := make(map[string]int)
	if len(schedulerConfig.Plugins.Score.Enabled) > 0 {
		for _, plugin := range schedulerConfig.Plugins.Score.Enabled {
			scorePluginMap[plugin.Name] = plugin.Weight
		}
	}

	if len(schedulerConfig.Plugins.Filter.Enabled) > 0 {
		filterPlugins = schedulerConfig.Plugins.Filter.Enabled
	}
	return scorePluginMap, filterPlugins, nil
}

// unmarshalPluginsConfig 从 pluginConfig 提取各插件参数 → map[name]RawExtension
// 参数是原始 JSON, 透传给各插件的 New 函数自行 unmarshal (如 prefix-cache 的 blockSize 等)
func unmarshalPluginsConfig(schedulerConfig *SchedulerConfiguration) (map[string]runtime.RawExtension, error) {
	pluginsArgMap := make(map[string]runtime.RawExtension)

	if len(schedulerConfig.PluginConfig) > 0 {
		for _, pluginArg := range schedulerConfig.PluginConfig {
			pluginsArgMap[pluginArg.Name] = pluginArg.Args
		}
	}

	return pluginsArgMap, nil
}
