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
	"bytes"
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
)

// =============================================================================
// ControllerRevision 生命周期 — 类 StatefulSet 的模板版本管理
// =============================================================================
//
// 【为什么需要 ControllerRevision】ModelServing.Spec.Template 是可变的(用户随时改 spec),
//   但 partition 保护的低 ordinal 副本恢复时必须用"它创建时那一代"的模板, 不能用最新 spec。
//   ControllerRevision 持久化每代模板快照(进 etcd), 让 partition 恢复能取到正确的旧模板。
//   仅靠 Generation(计数) 不行 — 它不含模板内容。Generation 只记"改了几次", 快照记"每次长啥样"。
//
// 【核心不变量】数据不可变(Data is never modified):
//   - 等价数据复用既有 ControllerRevision, 只前进其 Revision 数字(不新建、不改 data)。
//   - 哈希冲突(同名)时自增 collisionCount 作盐重算名字, 不改 data。这样历史快照绝对可靠。
//
// 【借鉴 StatefulSet】CurrentRevision/UpdateRevision/CollisionCount 三件套 +
//   controllerRevisionLess 排序, 与 K8s controller history 一致。
// =============================================================================

// RecordModelServingRevision 把 StatefulSet 的 revision 生命周期套到规范化 revision data 上:
// 等价数据复用其 ControllerRevision(只前进 Revision 数字), 数据本身不可变。
// 返回的 collisionCount 不同于当前值时, 调用方须持久化到 ModelServingStatus。
func RecordModelServingRevision(
	ctx context.Context,
	client kubernetes.Interface,
	ms *workloadv1alpha1.ModelServing,
	data []byte,
) (*appsv1.ControllerRevision, *int32, error) {
	if ms == nil {
		return nil, nil, fmt.Errorf("model serving is nil")
	}
	if len(data) == 0 {
		return nil, nil, fmt.Errorf("revision data is empty")
	}
	if _, err := decodeRevisionPatch(data); err != nil {
		return nil, nil, fmt.Errorf("invalid revision data: %w", err)
	}

	selector := labels.SelectorFromSet(map[string]string{
		ControllerRevisionLabelKey: ms.Name,
	})
	history, err := client.AppsV1().ControllerRevisions(ms.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: selector.String(),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("list controller revisions: %w", err)
	}

	var latest *appsv1.ControllerRevision
	var equivalent *appsv1.ControllerRevision
	var maxRevision int64
	for i := range history.Items {
		revision := &history.Items[i]
		owner := metav1.GetControllerOfNoCopy(revision)
		if owner == nil || owner.UID != ms.UID {
			continue
		}
		if latest == nil || controllerRevisionLess(latest, revision) {
			latest = revision
		}
		if revision.Revision > maxRevision {
			maxRevision = revision.Revision
		}
		if revision.Annotations[ControllerRevisionDataVersionAnnotation] == ControllerRevisionDataVersionV1 &&
			bytes.Equal(revision.Data.Raw, data) {
			if equivalent == nil || controllerRevisionLess(equivalent, revision) {
				equivalent = revision
			}
		}
	}

	if latest != nil && equivalent != nil && latest.Name == equivalent.Name {
		return latest.DeepCopy(), modelServingCollisionCount(ms), nil
	}
	nextRevision := maxRevision + 1

	if equivalent != nil {
		result, err := updateControllerRevision(ctx, client, equivalent, nextRevision)
		if err != nil {
			return nil, nil, err
		}
		return result, modelServingCollisionCount(ms), nil
	}

	collisionCount := modelServingCollisionCount(ms)
	for {
		hash := RevisionDataHash(data, collisionCount)
		name := GenerateControllerRevisionName(ms.Name, hash)
		revision := &appsv1.ControllerRevision{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ms.Namespace,
				Labels: map[string]string{
					ControllerRevisionLabelKey:         ms.Name,
					ControllerRevisionRevisionLabelKey: hash,
				},
				Annotations: map[string]string{
					ControllerRevisionDataVersionAnnotation: ControllerRevisionDataVersionV1,
				},
				OwnerReferences: []metav1.OwnerReference{newModelServingOwnerRef(ms)},
			},
			Revision: nextRevision,
			Data: runtime.RawExtension{
				Raw: append([]byte(nil), data...),
			},
		}
		created, err := client.AppsV1().ControllerRevisions(ms.Namespace).Create(ctx, revision, metav1.CreateOptions{})
		if apierrors.IsAlreadyExists(err) {
			existing, getErr := client.AppsV1().ControllerRevisions(ms.Namespace).Get(ctx, name, metav1.GetOptions{})
			if getErr != nil {
				return nil, nil, fmt.Errorf("get existing controller revision %s: %w", name, getErr)
			}
			owner := metav1.GetControllerOfNoCopy(existing)
			if owner != nil && owner.UID == ms.UID &&
				existing.Annotations[ControllerRevisionDataVersionAnnotation] == ControllerRevisionDataVersionV1 &&
				bytes.Equal(existing.Data.Raw, data) {
				if existing.Revision < nextRevision {
					existing, err = updateControllerRevision(ctx, client, existing, nextRevision)
					if err != nil {
						return nil, nil, err
					}
				}
				return existing, collisionCount, nil
			}
			(*collisionCount)++
			continue
		}
		if err != nil {
			return nil, nil, fmt.Errorf("create controller revision %s: %w", name, err)
		}
		return created, collisionCount, nil
	}
}

// controllerRevisionLess matches the ordering used by Kubernetes controller
// history: numeric revision, creation timestamp, then name.
func controllerRevisionLess(left, right *appsv1.ControllerRevision) bool {
	if left.Revision != right.Revision {
		return left.Revision < right.Revision
	}
	if !left.CreationTimestamp.Equal(&right.CreationTimestamp) {
		return left.CreationTimestamp.Before(&right.CreationTimestamp)
	}
	return left.Name < right.Name
}

func updateControllerRevision(
	ctx context.Context,
	client kubernetes.Interface,
	revision *appsv1.ControllerRevision,
	newRevision int64,
) (*appsv1.ControllerRevision, error) {
	clone := revision.DeepCopy()
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		if clone.Revision >= newRevision {
			return nil
		}
		candidate := clone.DeepCopy()
		candidate.Revision = newRevision
		updated, err := client.AppsV1().ControllerRevisions(candidate.Namespace).Update(ctx, candidate, metav1.UpdateOptions{})
		if err == nil {
			clone = updated
			return nil
		}
		if !apierrors.IsConflict(err) {
			return err
		}
		current, getErr := client.AppsV1().ControllerRevisions(clone.Namespace).Get(ctx, clone.Name, metav1.GetOptions{})
		if getErr != nil {
			return fmt.Errorf("refresh controller revision %s after conflict: %w", clone.Name, getErr)
		}
		clone = current
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("advance controller revision %s: %w", revision.Name, err)
	}
	return clone, nil
}

func copyInt32(value *int32) *int32 {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func modelServingCollisionCount(ms *workloadv1alpha1.ModelServing) *int32 {
	if ms.Status.CollisionCount != nil {
		return copyInt32(ms.Status.CollisionCount)
	}
	value := int32(0)
	return &value
}
