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

package controller

// =============================================================================
// 装箱缩容评分 — 决定缩容时"先删谁"
// =============================================================================
//
// 【为什么不是简单删最高 ordinal】推理副本有"热数据/会话价值"差异: 带 KV cache 热数据、
// 会话亲和的副本删掉代价高。故引入"状态优先级 + pod deletionCost + ordinal index"三级评分,
// 让应用层用 controller.kubernetes.io/pod-deletion-cost 注解干预删谁。
//
// 【评分优先级】值越小越先删:
//   1. Status(主): Creating/Deleting/Scaling/NotFound=0(先删, 不健康), Running=1(后删)
//      未知状态当 0(safety first, 先清理不确定的)
//   2. DeletionCost(次): 低 cost 先删(用户给热副本设高 cost 保护它)
//   3. Index(末): 同状态同 cost 时高 ordinal 先删(保留旧"高 ordinal 优先删"兜底)
//
// 【为何用 deletionCost】纯按 ordinal 删做不到"应用语义"干预 — PD 场景下用户想保留带了
// KV cache 热数据的副本, 给它较高 deletionCost 让它最后才删。
// =============================================================================

import (
	"fmt"
	"strconv"

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/datastore"
	"github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
	corev1 "k8s.io/api/core/v1"
)

const (
	// PodDeletionCostAnnotation: Pod 删除代价注解 key, 复用 K8s 标准 controller.kubernetes.io/pod-deletion-cost
	PodDeletionCostAnnotation = corev1.PodDeletionCost

	// 状态优先级常量: 数字越小 = 删除优先级越高(越先删)
	//   0: 最高删除优先级 — 不健康或终止中的状态
	//   1: 最低删除优先级 — 健康运行状态
	//
	// ServingGroup 优先级:
	PriorityServingGroupCreating = 0
	PriorityServingGroupDeleting = 0
	PriorityServingGroupScaling  = 0
	PriorityServingGroupNotFound = 0
	PriorityServingGroupRunning  = 1

	// Role 优先级:
	PriorityRoleCreating = 0
	PriorityRoleDeleting = 0
	PriorityRoleNotFound = 0
	PriorityRoleRunning  = 1
)

// ServingGroupWithScore 存储 ServingGroup 缩容评分信息
// 优先级顺序: Status(主) > DeletionCost(次) > Index(末)
type ServingGroupWithScore struct {
	Name         string
	Priority     int // 状态优先级, 来自 getServingGroupStatusPriority()
	DeletionCost int // 越高越受保护(删除优先级越低)
	Index        int
}

// RoleWithScore 存储 Role 缩容评分信息
// 优先级顺序: Status(主) > DeletionCost(次) > Index(末)
type RoleWithScore struct {
	Name         string
	Priority     int // 状态优先级, 来自 getRoleStatusPriority()
	DeletionCost int // 越高越受保护(删除优先级越低)
	Index        int
}

// getServingGroupStatusPriority returns the deletion priority for a serving group status.
// Lower priority value means higher deletion priority (delete first).
func getServingGroupStatusPriority(status datastore.ServingGroupStatus) int {
	switch status {
	case datastore.ServingGroupRunning:
		return PriorityServingGroupRunning
	case datastore.ServingGroupCreating:
		return PriorityServingGroupCreating
	case datastore.ServingGroupDeleting:
		return PriorityServingGroupDeleting
	case datastore.ServingGroupScaling:
		return PriorityServingGroupScaling
	case datastore.ServingGroupNotFound:
		return PriorityServingGroupNotFound
	default:
		// Unknown status - treat as highest deletion priority (safety first)
		return 0
	}
}

// getRoleStatusPriority returns the deletion priority for a role status.
// Lower priority value means higher deletion priority (delete first).
func getRoleStatusPriority(status datastore.RoleStatus) int {
	switch status {
	case datastore.RoleRunning:
		return PriorityRoleRunning
	case datastore.RoleCreating:
		return PriorityRoleCreating
	case datastore.RoleDeleting:
		return PriorityRoleDeleting
	case datastore.RoleNotFound:
		return PriorityRoleNotFound
	default:
		// Unknown status - treat as highest deletion priority (safety first)
		return 0
	}
}

// getPodDeletionCost retrieves the pod deletion cost from the pod's annotations.
func (c *ModelServingController) getPodDeletionCost(pod *corev1.Pod) int {
	if costStr, exists := pod.Annotations[PodDeletionCostAnnotation]; exists {
		if cost, err := strconv.Atoi(costStr); err == nil {
			return cost
		}
	}
	// The implicit deletion cost for pods that don't set the annotation is 0.
	return 0
}

// calculateRoleScore calculates priority information for role scale-down that considers:
// 1. Role readiness (primary factor): Running vs not-running
// 2. Pod deletion cost (secondary factor)
func (c *ModelServingController) calculateRoleScore(ms *workloadv1alpha1.ModelServing, groupName, roleName, roleID string) RoleWithScore {
	// Get role status from store
	roleStatus := c.store.GetRoleStatus(utils.GetNamespaceName(ms), groupName, roleName, roleID)
	priority := getRoleStatusPriority(roleStatus)

	// Get pod deletion cost as secondary factor
	roleIDValue := fmt.Sprintf("%s/%s/%s/%s", ms.Namespace, groupName, roleName, roleID)
	pods, err := c.getPodsByIndex(RoleIDKey, roleIDValue)
	if err != nil {
		_, index := utils.GetParentNameAndOrdinal(roleID)
		return RoleWithScore{
			Name:         roleID,
			Priority:     priority,
			DeletionCost: 0, // Default to 0 on error
			Index:        index,
		}
	}

	// Sum pod deletion costs (0 if not set)
	deletionCost := 0
	for _, pod := range pods {
		deletionCost += c.getPodDeletionCost(pod)
	}

	_, index := utils.GetParentNameAndOrdinal(roleID)

	return RoleWithScore{
		Name:         roleID,
		Priority:     priority,
		DeletionCost: deletionCost,
		Index:        index,
	}
}

// calculateServingGroupScore calculates priority information for serving group scale-down with two-level sorting:
// 1. Status readiness (primary): Not-ready groups should be deleted first
// 2. Pod deletion cost (secondary): Among groups with same status, lower cost = delete first
func (c *ModelServingController) calculateServingGroupScore(ms *workloadv1alpha1.ModelServing, groupName string) ServingGroupWithScore {
	// Get serving group status from store
	groupStatus := c.store.GetServingGroupStatus(utils.GetNamespaceName(ms), groupName)
	priority := getServingGroupStatusPriority(groupStatus)

	// Get pod deletion cost as secondary factor
	groupNameValue := fmt.Sprintf("%s/%s", ms.Namespace, groupName)
	pods, err := c.getPodsByIndex(GroupNameKey, groupNameValue)
	if err != nil {
		_, index := utils.GetParentNameAndOrdinal(groupName)
		return ServingGroupWithScore{
			Name:         groupName,
			Priority:     priority,
			DeletionCost: 0, // Default to 0 on error
			Index:        index,
		}
	}

	// Sum pod deletion costs (0 if not set)
	deletionCost := 0
	for _, pod := range pods {
		deletionCost += c.getPodDeletionCost(pod)
	}

	_, index := utils.GetParentNameAndOrdinal(groupName)

	return ServingGroupWithScore{
		Name:         groupName,
		Priority:     priority,
		DeletionCost: deletionCost,
		Index:        index,
	}
}
