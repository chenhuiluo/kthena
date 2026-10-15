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

package v1alpha1

// =============================================================================
// ServingGroup / Role 模板类型 — ModelServing 的内嵌模板(非独立 CR!)
// =============================================================================
//
// 【关键澄清】ServingGroup 和 Role 都不是独立 K8s 自定义资源(无 +kubebuilder:object:root=true,
//   未在 register.go 注册, 无 CRD yaml)。它们是 ModelServing.Spec.Template 的内嵌 struct 类型。
//   kubectl get servinggroup 必 NotFound。运行态在控制器内存 datastore, 不进 etcd。
//
// 【层级】ModelServing.Spec.Template(ServingGroup) → Roles[](1~4 个 Role)
//   一个 ServingGroup 含 1~4 个 Role(如 PD 场景的 prefill + decode)。
//   每个 Role.Replicas = 该 Role 的实例数; 每 Role 实例 = 1 entry + WorkerReplicas workers。
//
// 【GangPolicy】MinRoleReplicas 是 gang 门槛(不是副本数): "至少各 N 个 pod 且都被调度成功后,
//   整个 ServingGroup 的 pod 才能运行"。PD 场景 {prefill:1,decode:1} 保证 P 和 D 一起上线。
//   不可变(XValidation: self == oldSelf)。
// =============================================================================

import (
	corev1 "k8s.io/api/core/v1"
	volcanoV1Beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"
)

// GangPolicy 定义 gang 调度配置。
type GangPolicy struct {
	// MinRoleReplicas 每 Role 参与 gang 的最小实例数(pods in each role are strictly gang required)。
	// 允许对不同 Role 设不同最小值。未设时默认所有 Role 全部 gang required。
	// 例: 2P4D 设 {prefill:1, decode:1} → 至少 1 个 prefill + 1 个 decode 被调度成功后,
	//   整个 ServingGroup 的 pod 才能运行; 且同一 Role 内的 pod 必须一起调度。
	// why: PD 场景保证 P 和 D 一起上线, 避免只起 P 或只起 D 无法工作。
	// +optional
	// +kubebuilder:validation:XValidation:rule="self == oldSelf", message="minRoleReplicas is immutable"
	MinRoleReplicas map[string]int32 `json:"minRoleReplicas,omitempty"`
}

// NetworkTopologySpec 定义网络拓扑亲和调度策略(仅 scheduler 支持网络拓扑时生效)。
type NetworkTopology struct {
	// GroupPolicy 整个 ServingGroup 内所有实例的网络拓扑调度要求
	GroupPolicy *volcanoV1Beta1.NetworkTopologySpec `json:"groupPolicy,omitempty"`

	// RolePolicy 单个 Role 实例的细粒度网络拓扑调度要求
	RolePolicy *volcanoV1Beta1.NetworkTopologySpec `json:"rolePolicy,omitempty"`
}

// Role 定义执行推理任务的 Pod 实例角色(PD 场景即 prefill/decode)。
type Role struct {
	// Role 名, 在一个 ServingGroup 内唯一。MaxLength=12。
	// +kubebuilder:validation:MaxLength=12
	// +kubebuilder:validation:Pattern=^[a-zA-Z0-9]([-a-zA-Z0-9]*[a-zA-Z0-9])?$
	Name string `json:"name"`

	// The number of a certain role.
	// For example, in Disaggregated Prefilling, setting the replica count for both the P and D roles to 1 results in 1P1D deployment configuration.
	// This approach can similarly be applied to configure a xPyD deployment scenario.
	// Default to 1.
	// +optional
	// +kubebuilder:default=1
	Replicas *int32 `json:"replicas,omitempty"`

	// EntryTemplate defines the template for the entry pod of a role.
	// Required: Currently, a role must have only one entry-pod.
	EntryTemplate PodTemplateSpec `json:"entryTemplate"`

	// WorkerReplicas defines the number for the worker pod of a role.
	// Required: Need to set the number of worker-pod replicas.
	WorkerReplicas int32 `json:"workerReplicas"`

	// WorkerTemplate defines the template for the worker pod of a role.
	// +optional
	WorkerTemplate *PodTemplateSpec `json:"workerTemplate,omitempty"`

	// RollingUpdateConfiguration defines the parameters to be used for RoleRollingUpdate.
	// It is inlined so rolling update fields can be set directly under a Role.
	// These fields do not take effect when ModelServing uses ServingGroupRollingUpdate.
	// +optional
	RollingUpdateConfiguration `json:",inline,omitempty"`
}

// PodTemplateSpec describes the data a pod should have when created from a template
type PodTemplateSpec struct {
	// Object's metadata.
	// +optional
	Metadata *Metadata `json:"metadata,omitempty"`
	// Specification of the desired behavior of the pod.
	// +optional
	Spec corev1.PodSpec `json:"spec,omitempty"`
}

// Metadata is a simplified version of ObjectMeta in Kubernetes.
type Metadata struct {
	// Map of string keys and values that can be used to organize and categorize
	// (scope and select) objects. May match selectors of replication controllers
	// and services.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// Annotations is an unstructured key value map stored with a resource that may be
	// set by external tools to store and retrieve arbitrary metadata. They are not
	// queryable and should be preserved when modifying objects.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
}

// ServingGroup 是完成推理任务的最小单元(ModelServing.Spec.Template 的类型, 非独立 CR)。
// PD 场景下一个 ServingGroup = 一套自洽的 P+D 配对单元(共享 KV 状态, 跨组不串配对)。
// +kubebuilder:validation:XValidation:rule="!has(oldSelf.gangPolicy) || has(self.gangPolicy)", message="gangPolicy is required once set"
type ServingGroup struct {
	// RestartGracePeriodSeconds 故障后重建 ServingGroup 的宽限时间。默认 0(立即重建)。
	// +optional
	// +kubebuilder:default=0
	RestartGracePeriodSeconds *int64 `json:"restartGracePeriodSeconds,omitempty"`

	// GangPolicy gang 调度配置(一旦设置不可删)。
	// +optional
	GangPolicy *GangPolicy `json:"gangPolicy,omitempty"`

	// NetworkTopology 网络拓扑亲和调度策略(仅 scheduler 支持网络拓扑感知时生效)。
	// +optional
	NetworkTopology *NetworkTopology `json:"networkTopology,omitempty"`

	// Roles 角色列表, 1~4 个, 名字唯一。PD 场景即 [prefill, decode]。
	// +kubebuilder:validation:MaxItems=4
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:XValidation:rule="self.all(x, self.exists_one(y, y.name == x.name))", message="roles name must be unique"
	Roles []Role `json:"roles"`
}
