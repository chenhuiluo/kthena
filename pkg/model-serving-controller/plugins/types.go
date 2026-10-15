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
	"context"

	corev1 "k8s.io/api/core/v1"

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
)

// =============================================================================
// 插件接口 — ModelServing 在 Pod 生命周期关键点的扩展钩子
// =============================================================================
//
// 【钩子时机】
//   OnPodCreate: Pod 创建前(可原地改 req.Pod, 如注入 label/annotation/env)
//   OnPodReady:  Pod 就绪后(观察态, 一般不改)
// 插件由 ModelServing.Spec.Plugins 声明, 经 manager.NewChain 构建, 按 Scope 过滤执行。
//
// 【已有插件】lws-standard-labels(LWS 兼容, 注标准标签)、demo(示例)。
// =============================================================================

// HookRequest 插件钩子调用的上下文: 当前 ModelServing/ServingGroup/Role/Pod 信息
type HookRequest struct {
	ModelServing *workloadv1alpha1.ModelServing
	ServingGroup string
	RoleName     string
	RoleID       string
	IsEntry      bool
	Pod          *corev1.Pod
}

// Plugin 定义插件可实现的 Pod 生命周期钩子。
type Plugin interface {
	Name() string
	// OnPodCreate 在控制器创建 Pod 前调用, 可原地改 req.Pod
	OnPodCreate(ctx context.Context, req *HookRequest) error
	// OnPodReady 在控制器观测到 Pod RunningAndReady 后调用
	OnPodReady(ctx context.Context, req *HookRequest) error
}
