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

// workload.serving.volcano.sh API 组的包文档。
// 本组包含三个 CRD: ModelServing(推理编排顶层)、ModelBooster(一键部署,已废弃)、AutoscalingPolicy(弹性伸缩)。
// 注意: ServingGroup/Role 不是 CRD, 是 ModelServing.Spec.Template 的内嵌 struct 类型(见 §1.3)。
//
// +k8s:openai-gen=true
// +kubebuilder:object:generate=true
// +groupName=workload.serving.volcano.sh
package v1alpha1
