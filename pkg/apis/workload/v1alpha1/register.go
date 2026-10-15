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

import (
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName API 组名
const GroupName = "workload.serving.volcano.sh"

// GroupVersion API 组版本
var GroupVersion = v1.GroupVersion{Group: GroupName, Version: "v1alpha1"}

// SchemeGroupVersion 注册用的 group version
// Deprecated: use GroupVersion instead.
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}

// Resource 取 group 限定的 GroupResource
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

// GroupVersionKind 定义 — 注意: 只有 ModelServing/ModelBooster/AutoscalingPolicy 三类有 Kind,
// 没有 ServingGroupKind/RoleKind — 这是 ServingGroup/Role 不是独立 CRD 的硬证据(见 §1.3)。
var (
	ModelServingKind                = SchemeGroupVersion.WithKind("ModelServing")
	ModelServingListKind            = SchemeGroupVersion.WithKind("ModelServingList")
	ModelKind                       = SchemeGroupVersion.WithKind("ModelBooster")
	AutoscalingPolicyKind           = SchemeGroupVersion.WithKind("AutoscalingPolicy")
	ModelServingEntryPodLeaderLabel = "leader"
)

var (
	// localSchemeBuilder and AddToScheme will stay in k8s.io/kubernetes.
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	// Deprecated: use Install instead
	AddToScheme = localSchemeBuilder.AddToScheme
	Install     = localSchemeBuilder.AddToScheme
)

func init() {
	// 只注册手写函数; 生成函数的注册在生成文件里(分离让缺生成文件也能编译)
	localSchemeBuilder.Register(addKnownTypes)
}

// addKnownTypes 把已知类型加到 Scheme — 注意只注册 ModelServing/ModelBooster/AutoscalingPolicy,
// 没有 ServingGroup/Role(它们是内嵌 struct, 不是独立 CR)。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&ModelServing{},
		&ModelServingList{},
		&ModelBooster{},
		&ModelBoosterList{},
		&AutoscalingPolicy{},
		&AutoscalingPolicyList{},
	)
	// AddToGroupVersion allows the serialization of client types like ListOptions.
	v1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
