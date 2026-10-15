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

const (
	// ModelServingNameLabelKey Pod 标签 key: 所属 ModelServing 名
	ModelServingNameLabelKey = "modelserving.volcano.sh/name"
	// GroupNameLabelKey Pod 标签 key: 所属 ServingGroup 名(=ServingGroup 名)。
	// why 关键: 这是 PD 配对锚键 — ModelServer.PDGroup.GroupKey 取此 label,
	//   值=ServingGroup 名, 同组 prefill/decode 取相同值, 跨组不同, 调度边界=配对边界(见 §4.3)。
	GroupNameLabelKey = "modelserving.volcano.sh/group-name"
	// RoleLabelKey Pod 标签 key: 所属 Role 名(prefill/decode), PDGroup.PrefillLabels/DecodeLabels 据此匹配
	RoleLabelKey = "modelserving.volcano.sh/role"
	// RoleIDKey Pod 标签 key: Role 实例 ID(roleName-ordinal, 如 prefill-0)
	RoleIDKey = "modelserving.volcano.sh/role-id"
	// EntryLabelKey entry pod 标签 key(值=true), 区分 entry 和 worker
	EntryLabelKey = "modelserving.volcano.sh/entry"

	// RevisionLabelKey Pod 标签 key: 所属 revision
	RevisionLabelKey = "modelserving.volcano.sh/revision"
	// RoleTemplateHashLabelKey Pod 标签 key: Role 模板哈希, 供 RoleRollingUpdate 判定 Role 是否过时
	RoleTemplateHashLabelKey = "modelserving.volcano.sh/role-template-hash"
)
