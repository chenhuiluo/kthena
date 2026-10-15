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

package utils

import (
	"fmt"
	"hash"
	"hash/fnv"

	"k8s.io/apimachinery/pkg/util/dump"
	"k8s.io/apimachinery/pkg/util/rand"

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
)

// =============================================================================
// Revision 哈希计算 — 判定模板是否变化(是否触发版本升级)
// =============================================================================
//
// 【哈希算法】FNV + dump.ForHash(跟指针取实际值, 不受指针变化影响)。
//
// 【关键设计: 抹掉 Replicas/RollingUpdateConfiguration 再算哈希】
//   removeRoleReplicasForRevision 在算 ModelServing revision 哈希前, 把每个 Role 的 Replicas 和
//   RollingUpdateConfiguration 抹掉。why: 这些字段不影响"渲染出的 Pod 长啥样":
//     - Replicas 是"要几份", 改副本数是纯扩缩容, 不该触发版本升级/滚动重建。
//     - RollingUpdateConfiguration 是"怎么滚", 改滚动策略也不该触发版本升级。
//   若把它们算进哈希, 扩容会让所有旧副本被判 outdated 被滚动重建 — 违反"改副本数不升级"的设计。
//
// 【两个哈希】ModelServingRevision(整模板, 供 ControllerRevision 版本判定)
//   CalRoleTemplateHash(单 Role, 供 RoleRollingUpdate 判定某 Role 是否过时)
// =============================================================================

// Revision 用 FNV 哈希算对象的 revision
func Revision(obj interface{}) string {
	hasher := fnv.New32()
	DeepHashObject(hasher, obj)
	return rand.SafeEncodeString(fmt.Sprint(hasher.Sum32()))
}

// DeepHashObject 用 spew 库把对象写入哈希(跟指针取实际值, 确保指针变化不影响哈希)
func DeepHashObject(hasher hash.Hash, objectToWrite interface{}) {
	hasher.Reset()
	fmt.Fprintf(hasher, "%v", dump.ForHash(objectToWrite))
}

// removeRoleReplicasForRevision 算 ModelServing revision 哈希前抹掉 Replicas/RollingUpdateConfiguration。
// why: 这些字段不影响渲染出的 Pod — 改副本数/滚动策略不该触发版本升级(见文件头块)。
func removeRoleReplicasForRevision(ms *workloadv1alpha1.ModelServing) *workloadv1alpha1.ModelServing {
	copy := ms.DeepCopy()
	for i := range copy.Spec.Template.Roles {
		copy.Spec.Template.Roles[i].Replicas = nil
		copy.Spec.Template.Roles[i].RollingUpdateConfiguration = workloadv1alpha1.RollingUpdateConfiguration{}
	}

	return copy
}

// ModelServingRevision calculates the revision of a ModelServing object.
func ModelServingRevision(ms *workloadv1alpha1.ModelServing) string {
	roles := removeRoleReplicasForRevision(ms).Spec.Template.Roles
	return Revision(roles)
}

// removeRoleReplicasForRoleTemplateHash removes fields that do not change rendered pods when calculating role template hash.
func removeRoleReplicasForRoleTemplateHash(role workloadv1alpha1.Role) workloadv1alpha1.Role {
	copy := role
	copy.Replicas = nil
	copy.RollingUpdateConfiguration = workloadv1alpha1.RollingUpdateConfiguration{}
	return copy
}

// CalRoleTemplateHash calculates the revision hash for a Role template.
func CalRoleTemplateHash(role workloadv1alpha1.Role) string {
	copy := removeRoleReplicasForRoleTemplateHash(role)
	return Revision(copy)
}
