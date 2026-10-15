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

package datastore

import (
	"errors"
	"fmt"
	"slices"
	"sync"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"

	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
)

// =============================================================================
// 内存 datastore — 控制器的"观测状态"镜像, 不进 etcd
// =============================================================================
//
// 【职责】在控制器进程内存里镜像 ServingGroup/Role 的运行态(状态/revision/运行 Pod),
//   供 Reconcile 比对"期望态(spec) vs 观测态(store)"驱动扩缩容。
//
// 【为什么用内存而非 etcd】ServingGroup/Role 不是 K8s 资源(无 CRD, 见架构文档 §1.3),
//   它们的运行态无需持久化 — 控制器重启时从 Pod 的 label(group-name/role/role-id)反向重建即可,
//   Pod 是唯一可信源(进 etcd 且带全 label)。故 store 只需内存镜像, 重启即失。
//
// 【数据结构】两层 map:
//   store.servingGroup: modelServing 名 → (组名 → *ServingGroup)
//   ServingGroup.roles: roleName → (roleID → *Role)  如 prefill → prefill-0 → *Role
//
// 【与 spec 层的区别】API 层 workloadv1alpha1.ServingGroup/Role 是用户 spec(模板);
//   本文件 datastore.ServingGroup/Role 是运行态(观测)。同名不同包, 别混。
//
// 【并发安全】store 用 sync.RWMutex 保护; 接口 Store 供测试注入 mock。
// =============================================================================

// Store 是存取 ServingGroup/Role 运行态的接口(供测试注入 mock)。
type Store interface {
	// GetServingGroupByModelServing 返回某 ModelServing 的所有 ServingGroup(按 ordinal 升序)
	GetServingGroupByModelServing(modelServingName types.NamespacedName) ([]ServingGroup, error)
	GetServingGroupRevision(modelServingName types.NamespacedName, groupName string) (string, bool)
	GetRunningPodNumByServingGroup(modelServingName types.NamespacedName, groupName string) (int, error)
	GetServingGroupStatus(modelServingName types.NamespacedName, groupName string) ServingGroupStatus
	GetRoleList(modelServingName types.NamespacedName, groupName, roleName string) ([]Role, error)
	// GetRolesByGroup 返回某组所有 Role: roleName → (roleID → *Role)
	GetRolesByGroup(modelServingName types.NamespacedName, groupName string) (map[string]map[string]*Role, error)
	GetRoleStatus(modelServingName types.NamespacedName, groupName, roleName, roleID string) RoleStatus
	UpdateRoleStatus(modelServingName types.NamespacedName, groupName, roleName, roleID string, status RoleStatus) error
	DeleteRole(modelServingName types.NamespacedName, groupName, roleName, roleID string)
	DeleteModelServing(modelServingName types.NamespacedName)
	DeleteServingGroup(modelServingName types.NamespacedName, groupName string)
	AddServingGroup(modelServingName types.NamespacedName, idx int, revision string)
	AddRole(modelServingName types.NamespacedName, groupName, roleName, roleID, revision, roleTemplateHash string)
	AddRunningPodToServingGroup(modelServingName types.NamespacedName, groupName, pod, revision, roleTemplateHash, roleName, roleID string)
	// AddServingGroupAndRole 不存在则加 servingGroup 和 role(首次同步用)
	AddServingGroupAndRole(modelServingName types.NamespacedName, servingGroupName, revision, roleTemplateHash, roleName, roleID string)
	DeleteRunningPodFromServingGroup(modelServingName types.NamespacedName, groupName string, pod string)
	UpdateServingGroupStatus(modelServingName types.NamespacedName, groupName string, Status ServingGroupStatus) error
	UpdateServingGroupRevision(modelServingName types.NamespacedName, groupName string, revision string) error
	// DumpCache 返回 store 缓存的 JSON dump(调试/监控用, /debug/modelserving/cache 端点)
	DumpCache() ([]byte, error)
}

// store 是 Store 的内存实现, 用 RWMutex 保护
type store struct {
	mutex sync.RWMutex

	// servingGroup: modelServing 名 → (组名 → *ServingGroup)
	servingGroup map[types.NamespacedName]map[string]*ServingGroup
}

// ServingGroup 是内存里的 ServingGroup 运行态(与 API 层 workloadv1alpha1.ServingGroup 同名不同包)。
type ServingGroup struct {
	Name        string
	runningPods map[string]struct{}   // 本组运行中的 pod 名集合
	Revision    string
	Status      ServingGroupStatus
	roles       map[string]map[string]*Role // roleName → roleID → *Role, 如 prefill → prefill-0 → *Role
}

// Role 是内存里的 Role 运行态。
type Role struct {
	Name             string
	Revision         string              // 继承所属 ServingGroup 的 Revision
	RoleTemplateHash string              // Role 模板哈希, 供 RoleRollingUpdate 判定过时
	Status           RoleStatus
}

// ServingGroupStatus ServingGroup 状态枚举(注意: 多一个 Scaling, Role 没有)
type ServingGroupStatus string

const (
	ServingGroupRunning  ServingGroupStatus = "Running"
	ServingGroupCreating ServingGroupStatus = "Creating"
	ServingGroupDeleting ServingGroupStatus = "Deleting"
	ServingGroupScaling  ServingGroupStatus = "Scaling"
	ServingGroupNotFound ServingGroupStatus = "NotFound"
)

// RoleStatus Role 状态枚举(注意: 无 RoleDraining — 无损升级的 drain 在 Router 侧, 不进控制面)
type RoleStatus string

const (
	RoleCreating RoleStatus = "Creating"
	RoleRunning  RoleStatus = "Running"
	RoleDeleting RoleStatus = "Deleting"
	RoleNotFound RoleStatus = "NotFound"
)

var ErrServingGroupNotFound = errors.New("serving group not found")

func New() Store {
	return &store{
		servingGroup: make(map[types.NamespacedName]map[string]*ServingGroup),
	}
}

// GetServingGroupByModelServing returns the list of ServingGroups and errors
func (s *store) GetServingGroupByModelServing(modelServingName types.NamespacedName) ([]ServingGroup, error) {
	s.mutex.RLock()
	servingGroups, ok := s.servingGroup[modelServingName]
	if !ok {
		s.mutex.RUnlock()
		return nil, ErrServingGroupNotFound
	}
	// sort ServingGroups by index
	servingGroupsSlice := make([]ServingGroup, 0, len(servingGroups))
	for _, servingGroup := range servingGroups {
		// This is a clone to prevent r/w conflict later
		servingGroupsSlice = append(servingGroupsSlice, *servingGroup)
	}
	s.mutex.RUnlock()

	slices.SortFunc(servingGroupsSlice, func(a, b ServingGroup) int {
		_, aIndex := utils.GetParentNameAndOrdinal(a.Name)
		_, bIndex := utils.GetParentNameAndOrdinal(b.Name)
		return aIndex - bIndex
	})

	return servingGroupsSlice, nil
}

// GetRoleList returns the list of roles and errors
func (s *store) GetRoleList(modelServingName types.NamespacedName, groupName, roleName string) ([]Role, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	servingGroups, ok := s.servingGroup[modelServingName]
	if !ok {
		return nil, fmt.Errorf("cannot list ServingGroup of modelServing %s", modelServingName.Name)
	}
	servingGroup, ok := servingGroups[groupName]
	if !ok {
		return nil, ErrServingGroupNotFound
	}
	roleMap, ok := servingGroup.roles[roleName]
	if !ok {
		// If the roleName does not exist, return an empty list instead of an error
		return []Role{}, nil
	}

	//Convert roles in map to a slice
	roleSlice := make([]Role, 0, len(roleMap))
	for _, role := range roleMap {
		roleSlice = append(roleSlice, *role)
	}

	slices.SortFunc(roleSlice, func(a, b Role) int {
		_, aIndex := utils.GetParentNameAndOrdinal(a.Name)
		_, bIndex := utils.GetParentNameAndOrdinal(b.Name)
		return aIndex - bIndex
	})

	return roleSlice, nil
}

func (s *store) GetRolesByGroup(modelServingName types.NamespacedName, groupName string) (map[string]map[string]*Role, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	servingGroups, ok := s.servingGroup[modelServingName]
	if !ok {
		return nil, fmt.Errorf("cannot find modelServing %s", modelServingName.Name)
	}
	servingGroup, ok := servingGroups[groupName]
	if !ok {
		return nil, fmt.Errorf("cannot find servingGroup %s", groupName)
	}

	// Return a snapshot copy of the roles map to avoid concurrent map access issues.
	copiedRoles := make(map[string]map[string]*Role, len(servingGroup.roles))
	for roleName, roleMap := range servingGroup.roles {
		if roleMap == nil {
			continue
		}
		copiedInner := make(map[string]*Role, len(roleMap))
		for roleID, role := range roleMap {
			roleCopy := *role
			copiedInner[roleID] = &roleCopy
		}
		copiedRoles[roleName] = copiedInner
	}
	return copiedRoles, nil
}

// UpdateRoleStatus updates the status of a specific role
func (s *store) UpdateRoleStatus(modelServingName types.NamespacedName, groupName, roleName, roleID string, status RoleStatus) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	servingGroups, ok := s.servingGroup[modelServingName]
	if !ok {
		return fmt.Errorf("cannot find modelServing %s", modelServingName.Name)
	}

	servingGroup, ok := servingGroups[groupName]
	if !ok {
		return ErrServingGroupNotFound
	}

	roleMap, ok := servingGroup.roles[roleName]
	if !ok {
		return fmt.Errorf("roleName %s not found in group %s", roleName, groupName)
	}

	role, ok := roleMap[roleID]
	if !ok {
		return fmt.Errorf("role %s not found in roleName %s of group %s", roleID, roleName, groupName)
	}

	role.Status = status
	return nil
}

// GetRoleStatus returns the status of a specific role
func (s *store) GetRoleStatus(modelServingName types.NamespacedName, groupName, roleName, roleID string) RoleStatus {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if servingGroups, exist := s.servingGroup[modelServingName]; exist {
		if group, ok := servingGroups[groupName]; ok {
			if roleMap, exists := group.roles[roleName]; exists {
				if role, found := roleMap[roleID]; found {
					return role.Status
				}
			}
		}
	}
	return RoleNotFound
}

// GetRunningPodNumByServingGroup returns the number of running pods and errors
func (s *store) GetRunningPodNumByServingGroup(modelServingName types.NamespacedName, groupName string) (int, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	groups, ok := s.servingGroup[modelServingName]
	if !ok {
		return 0, fmt.Errorf("modelServing %s not found", modelServingName)
	}

	group, ok := groups[groupName]
	if !ok {
		return 0, nil
	}
	return len(group.runningPods), nil
}

// GetServingGroupRevision returns the revision of a ServingGroup.
func (s *store) GetServingGroupRevision(modelServingName types.NamespacedName, groupName string) (string, bool) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if groups, ok := s.servingGroup[modelServingName]; ok {
		if group, ok := groups[groupName]; ok {
			return group.Revision, true
		}
	}
	return "", false
}

// GetServingGroupStatus returns the status of ServingGroup
func (s *store) GetServingGroupStatus(modelServingName types.NamespacedName, groupName string) ServingGroupStatus {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if groups, exist := s.servingGroup[modelServingName]; exist {
		if group, ok := groups[groupName]; ok {
			return group.Status
		}
	}
	return ServingGroupNotFound
}

// DeleteModelServing delete modelServing in ServingGroup map
func (s *store) DeleteModelServing(modelServingName types.NamespacedName) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.servingGroup, modelServingName)
}

// DeleteServingGroup delete ServingGroup in map
// Note: Revision history should be recorded using ControllerRevision before calling this method
// to ensure it's captured even if the deletion process fails.
func (s *store) DeleteServingGroup(modelServingName types.NamespacedName, groupName string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if groups, ok := s.servingGroup[modelServingName]; ok {
		delete(groups, groupName)
	}
}

// DeleteRole deletes a specific role from an ServingGroup
func (s *store) DeleteRole(modelServingName types.NamespacedName, groupName, roleName, roleID string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	servingGroups, ok := s.servingGroup[modelServingName]
	if !ok {
		return
	}

	servingGroup, ok := servingGroups[groupName]
	if !ok {
		return
	}

	roleMap, ok := servingGroup.roles[roleName]
	if !ok {
		return
	}
	delete(roleMap, roleID)
}

// AddServingGroup add ServingGroup item of one modelServing
func (s *store) AddServingGroup(modelServingName types.NamespacedName, idx int, revision string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	name := utils.GenerateServingGroupName(modelServingName.Name, idx)

	if _, ok := s.servingGroup[modelServingName]; !ok {
		s.servingGroup[modelServingName] = make(map[string]*ServingGroup)
	}

	if _, ok := s.servingGroup[modelServingName][name]; ok {
		return
	}
	s.servingGroup[modelServingName][name] = &ServingGroup{
		Name:        name,
		runningPods: make(map[string]struct{}),
		Status:      ServingGroupCreating,
		Revision:    revision,
		roles:       make(map[string]map[string]*Role),
	}
}

// AddRole adds a new role to an ServingGroup
func (s *store) AddRole(modelServingName types.NamespacedName, groupName, roleName, roleID, revision, roleTemplateHash string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.servingGroup[modelServingName]; !ok {
		s.servingGroup[modelServingName] = make(map[string]*ServingGroup)
	}

	group, ok := s.servingGroup[modelServingName][groupName]
	if !ok {
		group = &ServingGroup{
			Name:        groupName,
			runningPods: make(map[string]struct{}),
			Status:      ServingGroupCreating,
			Revision:    revision,
			roles:       make(map[string]map[string]*Role),
		}
		s.servingGroup[modelServingName][groupName] = group
	}

	if _, exists := group.roles[roleName]; !exists {
		group.roles[roleName] = make(map[string]*Role)
	}

	if existing, exists := group.roles[roleName][roleID]; exists {
		if existing.Revision != revision {
			klog.Warningf("AddRole: role %s/%s already exists with revision %s, but got revision %s; skipping",
				roleName, roleID, existing.Revision, revision)
		}
	} else {
		group.roles[roleName][roleID] = &Role{
			Name:             roleID,
			Status:           RoleCreating,
			Revision:         revision,
			RoleTemplateHash: roleTemplateHash,
		}
	}
}

// AddRunningPodToServingGroup add ServingGroup in runningPodOfServingGroup map
func (s *store) AddRunningPodToServingGroup(modelServingName types.NamespacedName, servingGroupName, runningPodName, revision, roleTemplateHash, roleName, roleID string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.servingGroup[modelServingName]; !ok {
		// If modelServingName not exist, create a new one
		s.servingGroup[modelServingName] = make(map[string]*ServingGroup)
	}

	group, ok := s.servingGroup[modelServingName][servingGroupName]
	if !ok {
		// If ServingGroupName not exist, create a new one
		group = &ServingGroup{
			Name:        servingGroupName,
			runningPods: map[string]struct{}{},
			Status:      ServingGroupCreating,
			Revision:    revision,
			roles:       make(map[string]map[string]*Role),
		}

		s.servingGroup[modelServingName][servingGroupName] = group
	}

	group.runningPods[runningPodName] = struct{}{} // runningPods map has been initialized during AddServingGroup.

	// Check if roleName exists, and initialize it if not
	if _, ok = group.roles[roleName]; !ok {
		group.roles[roleName] = make(map[string]*Role)
	}

	if _, ok = group.roles[roleName][roleID]; !ok {
		role := &Role{
			Name:             roleID,
			Status:           RoleCreating,
			Revision:         revision,
			RoleTemplateHash: roleTemplateHash,
		}
		group.roles[roleName][roleID] = role
	}
}

// AddServingGroupAndRole adds ServingGroup and roles if not exist
func (s *store) AddServingGroupAndRole(modelServingName types.NamespacedName, servingGroupName, revision, roleTemplateHash, roleName, roleID string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, ok := s.servingGroup[modelServingName]; !ok {
		// If modelServingName not exist, create a new one
		s.servingGroup[modelServingName] = make(map[string]*ServingGroup)
	}

	group, ok := s.servingGroup[modelServingName][servingGroupName]
	if !ok {
		// If ServingGroupName not exist, create a new one
		group = &ServingGroup{
			Name:        servingGroupName,
			runningPods: map[string]struct{}{},
			Status:      ServingGroupCreating,
			Revision:    revision,
			roles:       make(map[string]map[string]*Role),
		}

		s.servingGroup[modelServingName][servingGroupName] = group
	}

	// Check if roleName exists, and initialize it if not
	if _, ok = group.roles[roleName]; !ok {
		group.roles[roleName] = make(map[string]*Role)
	}

	if _, ok = group.roles[roleName][roleID]; !ok {
		role := &Role{
			Name:             roleID,
			Status:           RoleCreating,
			Revision:         revision,
			RoleTemplateHash: roleTemplateHash,
		}
		group.roles[roleName][roleID] = role
	}
}

// DeleteRunningPodFromServingGroup delete runningPod in map
func (s *store) DeleteRunningPodFromServingGroup(modelServingName types.NamespacedName, servingGroupName string, pod string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if groups, exist := s.servingGroup[modelServingName]; exist {
		if group, ok := groups[servingGroupName]; ok {
			delete(group.runningPods, pod)
		}
	}
}

// UpdateServingGroupStatus update status of one ServingGroup
func (s *store) UpdateServingGroupStatus(modelServingName types.NamespacedName, groupName string, status ServingGroupStatus) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	groups, ok := s.servingGroup[modelServingName]
	if !ok {
		return fmt.Errorf("failed to find modelServing %s", modelServingName.Namespace+"/"+modelServingName.Name)
	}
	if group, ok := groups[groupName]; ok {
		group.Status = status
		groups[groupName] = group
	} else {
		return fmt.Errorf("failed to find ServingGroup %s in modelServing %s", groupName, modelServingName.Namespace+"/"+modelServingName.Name)
	}
	return nil
}

// UpdateServingGroupRevision updates the revision of a ServingGroup
func (s *store) UpdateServingGroupRevision(modelServingName types.NamespacedName, groupName string, revision string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	groups, ok := s.servingGroup[modelServingName]
	if !ok {
		return fmt.Errorf("failed to find modelServing %s", modelServingName.Namespace+"/"+modelServingName.Name)
	}
	if group, ok := groups[groupName]; ok {
		group.Revision = revision
		groups[groupName] = group
	} else {
		return fmt.Errorf("failed to find ServingGroup %s in modelServing %s", groupName, modelServingName.Namespace+"/"+modelServingName.Name)
	}
	return nil
}
