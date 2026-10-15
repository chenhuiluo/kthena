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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
	schedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	workloadv1alpha1 "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
)

// =============================================================================
// 共享工具 — 命名/ordinal、Pod 生成、label、预算解析、条件设置等
// =============================================================================
//
// 【命名约定】两级建模通过命名串联, 所有命名可双向解析:
//   ModelServing: my-model
//     ├─ ServingGroup: my-model-0, my-model-1, ...       (miName-ordinal)
//     │   └─ Role 实例: prefill-0, decode-0, ...           (roleName-ordinal)
//     │       └─ Pod: my-model-0-prefill-0-0, ...-0-1, ... (group-roleID-podIndex)
//   正则 (.*)-([0-9]+)$ 从 xxx-N 解析父名+ordinal。
//
// 【Pod 结构】一个 Role 实例 = 1 entry pod + WorkerReplicas worker pod。
//   entry pod index 固定 0, worker 从 1 起。entry 打 EntryLabelKey=true,
//   注入 ENTRY_ADDRESS/WORKER_INDEX/GROUP_SIZE 环境变量。
// =============================================================================

const (
	// Entry: entry pod 的 label 值(EntryLabelKey=true)
	Entry = "true"

	// ModelServingStatus 的 condition message 常量
	AllGroupsIsReady         = "All Serving groups are ready"
	SomeGroupsAreProgressing = "Some groups is progressing"
	SomeGroupsAreUpdated     = "Updated Groups are"
)

// GetNamespaceName 从 metav1.Object 取 namespace/name
func GetNamespaceName(obj metav1.Object) types.NamespacedName {
	return types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}
}

// servingGroupRegex 从 ServingGroup 名解析父 ModelServing 名 + ordinal 的正则: (.*)-([0-9]+)$
var servingGroupRegex = regexp.MustCompile("(.*)-([0-9]+)$")

// GetParentNameAndOrdinal 从 ServingGroup 名解析父 ModelServing 名和 ordinal。
// 例: "vllm-sample-0" → ("vllm-sample", 0)。无法解析返回 ("", -1)。
func GetParentNameAndOrdinal(groupName string) (string, int) {
	parent := ""
	ordinal := -1
	subMatches := servingGroupRegex.FindStringSubmatch(groupName)
	if len(subMatches) < 3 {
		return parent, ordinal
	}
	parent = subMatches[1]
	if i, err := strconv.ParseInt(subMatches[2], 10, 32); err == nil {
		ordinal = int(i)
	}
	return parent, ordinal
}

// GenerateServingGroupName 生成 ServingGroup 名: miName + "-" + idx。例: ("my-model",1) → "my-model-1"
func GenerateServingGroupName(miName string, idx int) string {
	return miName + "-" + strconv.Itoa(idx)
}

// GenerateRoleID 生成 Role 实例 ID: roleName + "-" + idx。例: ("prefill",0) → "prefill-0"
func GenerateRoleID(roleName string, idx int) string {
	return roleName + "-" + strconv.Itoa(idx)
}

// GenerateControllerRevisionName 生成 ControllerRevision 名(截断 msName 至 223 字符, 对齐 K8s controller history 前缀约束)
func GenerateControllerRevisionName(msName, revision string) string {
	// 对齐 Kubernetes controller history 的前缀约束
	if len(msName) > 223 {
		msName = msName[:223]
	}
	return msName + "-" + revision
}

func GeneratePodName(groupName, roleName string, podIndex int) string {
	// worker-pod number starts from 1
	// For example, WorkerPodName is vllm-sample-0-prefill-1-1, represents the first worker-pod in the second replica of the prefill role
	return groupName + "-" + roleName + "-" + strconv.Itoa(podIndex)
}

// GenerateEntryPod 生成 entry pod: podIndex 固定 0, 打 EntryLabelKey=true,
// 用 EntryTemplate, 注入 ENTRY_ADDRESS/WORKER_INDEX/GROUP_SIZE 环境变量, SchedulerName 取 ms 的。
func GenerateEntryPod(role workloadv1alpha1.Role, ms *workloadv1alpha1.ModelServing, groupName, roleID, revision, roleTemplateHash string) *corev1.Pod {
	entryPodName := GeneratePodName(groupName, roleID, 0)
	entryPod := createBasePod(role, ms, entryPodName, groupName, roleID, revision, roleTemplateHash)
	entryPod.ObjectMeta.Labels[workloadv1alpha1.EntryLabelKey] = Entry
	addPodLabelAndAnnotation(entryPod, role.EntryTemplate.Metadata)
	entryPod.Spec = role.EntryTemplate.Spec
	entryPod.Spec.SchedulerName = ms.Spec.SchedulerName
	// 给每个容器注入公共环境变量
	envVars := createCommonEnvVars(role, entryPod, 0)
	addPodEnvVars(entryPod, envVars...)
	return entryPod
}

// GenerateWorkerPod 生成 worker pod: podIndex 从 1 起, 用 WorkerTemplate, 同样注入环境变量。
func GenerateWorkerPod(role workloadv1alpha1.Role, ms *workloadv1alpha1.ModelServing, entryPod *corev1.Pod, groupName, roleID string, podIndex int, revision, roleTemplateHash string) *corev1.Pod {
	if role.WorkerTemplate == nil {
		klog.Errorf("WorkerTemplate is required when workerReplicas > 0 for role %s", role.Name)
		return nil
	}

	workerPodName := GeneratePodName(groupName, roleID, podIndex)
	workerPod := createBasePod(role, ms, workerPodName, groupName, roleID, revision, roleTemplateHash)
	addPodLabelAndAnnotation(workerPod, role.WorkerTemplate.Metadata)
	workerPod.Spec = role.WorkerTemplate.Spec
	workerPod.Spec.SchedulerName = ms.Spec.SchedulerName
	envVars := createCommonEnvVars(role, entryPod, podIndex)
	addPodEnvVars(workerPod, envVars...)
	return workerPod
}

// createBasePod 构造 Pod 的基础骨架: 打 6 个关键 label + OwnerRef 指向 ModelServing。
//
// 【6 个 label】(都定义在 pkg/apis/workload/v1alpha1/labels.go):
//   ModelServingNameLabelKey: ms.Name       — 哪个 ModelServing
//   GroupNameLabelKey:        groupName     — 哪个 ServingGroup(PD 配对锚键, 见 §4.3)
//   RoleLabelKey:             role.Name     — 哪个 Role(prefill/decode)
//   RoleIDKey:                 roleID       — 哪个 Role 实例(prefill-0)
//   RevisionLabelKey:          revision     — 哪个 revision
//   RoleTemplateHashLabelKey:  roleTemplateHash — Role 模板哈希(供 RoleRollingUpdate 判定过时)
//
// 【OwnerRef】指向 ModelServing(不是 ServingGroup/Role — 它们不是 K8s 资源, 见 §1.3),
//   故 GC 级联删除是 ModelServing → Pod 直接关联。
func createBasePod(role workloadv1alpha1.Role, ms *workloadv1alpha1.ModelServing, name, groupName, roleID, revision, roleTemplateHash string) *corev1.Pod {
	return &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ms.Namespace,
			Labels: map[string]string{
				workloadv1alpha1.ModelServingNameLabelKey: ms.Name,
				workloadv1alpha1.GroupNameLabelKey:        groupName,
				workloadv1alpha1.RoleLabelKey:             role.Name,
				workloadv1alpha1.RoleIDKey:                roleID,
				workloadv1alpha1.RevisionLabelKey:         revision,
				workloadv1alpha1.RoleTemplateHashLabelKey: roleTemplateHash,
			},
			OwnerReferences: []metav1.OwnerReference{
				newModelServingOwnerRef(ms),
			},
		},
	}
}

func addPodLabelAndAnnotation(pod *corev1.Pod, metadata *workloadv1alpha1.Metadata) {
	if metadata == nil {
		return
	}
	if metadata.Labels != nil {
		if pod.Labels == nil {
			pod.Labels = make(map[string]string)
		}
		for k, v := range metadata.Labels {
			pod.Labels[k] = v
		}
	}
	if metadata.Annotations != nil {
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		for k, v := range metadata.Annotations {
			pod.Annotations[k] = v
		}
	}
}

func createCommonEnvVars(role workloadv1alpha1.Role, entryPod *corev1.Pod, workerIndex int) []corev1.EnvVar {
	return []corev1.EnvVar{
		{
			Name:  workloadv1alpha1.GroupSizeEnv,
			Value: strconv.Itoa(int(role.WorkerReplicas) + 1),
		},
		{
			Name: workloadv1alpha1.EntryAddressEnv,
			// entryPod name as same as headless service name
			Value: entryPod.GetName() + "." + entryPod.Namespace,
		},
		{
			Name:  workloadv1alpha1.WorkerIndexEnv,
			Value: strconv.Itoa(workerIndex),
		},
	}
}

// addPodEnvVars adds new env vars to the container.
func addPodEnvVars(pod *corev1.Pod, newEnvVars ...corev1.EnvVar) {
	if pod == nil {
		return
	}

	for i := range pod.Spec.Containers {
		addEnvVars(&pod.Spec.Containers[i], newEnvVars...)
	}

	for i := range pod.Spec.InitContainers {
		addEnvVars(&pod.Spec.InitContainers[i], newEnvVars...)
	}
}

func addEnvVars(container *corev1.Container, newEnvVars ...corev1.EnvVar) {
	if container == nil {
		return
	}
	// Used to quickly find whether the newly added environment variable already exists
	newEnvMap := make(map[string]struct{})
	for _, env := range newEnvVars {
		newEnvMap[env.Name] = struct{}{}
	}

	// Collect environment variables that need to be retained
	var retainedEnvVars []corev1.EnvVar
	for _, env := range container.Env {
		if _, exists := newEnvMap[env.Name]; !exists {
			// This environment variable does not need to be updated.
			retainedEnvVars = append(retainedEnvVars, env)
		}
	}
	// Merge existing variables that are retained and newly added variables
	container.Env = append(retainedEnvVars, newEnvVars...)
}

// newModelServingOwnerRef creates an OwnerReference pointing to the given ModelServing.
func newModelServingOwnerRef(ms *workloadv1alpha1.ModelServing) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion:         workloadv1alpha1.ModelServingKind.GroupVersion().String(),
		Kind:               workloadv1alpha1.ModelServingKind.Kind,
		Name:               ms.Name,
		UID:                ms.UID,
		BlockOwnerDeletion: ptr.To(true),
		Controller:         ptr.To(true),
	}
}

func CreateHeadlessService(ctx context.Context, k8sClient kubernetes.Interface, ms *workloadv1alpha1.ModelServing, serviceSelector map[string]string, groupName, roleLabel string, roleIndex int) error {
	serviceName := GeneratePodName(groupName, GenerateRoleID(roleLabel, roleIndex), 0)
	headlessService := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: ms.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				newModelServingOwnerRef(ms),
			},
			Labels: map[string]string{
				workloadv1alpha1.ModelServingNameLabelKey: ms.Name,
				workloadv1alpha1.GroupNameLabelKey:        groupName,
				workloadv1alpha1.RoleLabelKey:             roleLabel,
				workloadv1alpha1.RoleIDKey:                GenerateRoleID(roleLabel, roleIndex),
			},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                "None", // defines service as headless
			Selector:                 serviceSelector,
			PublishNotReadyAddresses: true,
		},
	}
	// create the service in the cluster
	klog.V(4).Infof("Creating headless service %s", headlessService.Name)
	_, err := k8sClient.CoreV1().Services(ms.Namespace).Create(ctx, &headlessService, metav1.CreateOptions{})

	if err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create headless service failed: %v", err)
		}
	}
	return nil
}

func GetModelServingAndGroupByLabel(podLabels map[string]string) (string, string, bool) {
	modelServingName, ok := podLabels[workloadv1alpha1.ModelServingNameLabelKey]
	if !ok {
		return "", "", false
	}
	servingGroupName, ok := podLabels[workloadv1alpha1.GroupNameLabelKey]
	if !ok {
		return "", "", false
	}
	return modelServingName, servingGroupName, true
}

// IsOwnedByModelServingWithUID returns true when the object is owned by the ModelServing with the provided UID.
func IsOwnedByModelServingWithUID(obj metav1.Object, uid types.UID) bool {
	for _, ownerRef := range obj.GetOwnerReferences() {
		if ownerRef.APIVersion == workloadv1alpha1.SchemeGroupVersion.String() &&
			ownerRef.Kind == workloadv1alpha1.ModelServingKind.Kind &&
			ownerRef.UID == uid {
			return true
		}
	}
	klog.Warningf("object %s/%s is not owned by ModelServing with UID %s", obj.GetNamespace(), obj.GetName(), uid)
	return false
}

// IsPodRunningAndReady returns true if pod is in the PodRunning Phase, if it has a condition of PodReady.
func IsPodRunningAndReady(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodRunning && isPodReady(pod)
}

// CheckPodRevision determine if the pod's revision is compliant or not.
func CheckPodRevision(pod *corev1.Pod, revision string) bool {
	podRevision, ok := pod.Labels[workloadv1alpha1.RevisionLabelKey]
	if !ok {
		return false
	}
	return podRevision == revision
}

// ObjectRevision returns the revision label of the resource.
func ObjectRevision(obj metav1.Object) string {
	return obj.GetLabels()[workloadv1alpha1.RevisionLabelKey]
}

func ObjectRoleTemplateHash(obj metav1.Object) string {
	return obj.GetLabels()[workloadv1alpha1.RoleTemplateHashLabelKey]
}

// GetRoleName returns the role name of the resource.
func GetRoleName(resource metav1.Object) string {
	return resource.GetLabels()[workloadv1alpha1.RoleLabelKey]
}

// GetRoleID returns the role id of the resource.
func GetRoleID(resource metav1.Object) string {
	return resource.GetLabels()[workloadv1alpha1.RoleIDKey]
}

func isPodReady(pod *corev1.Pod) bool {
	return isPodReadyConditionTrue(pod.Status)
}

func isPodReadyConditionTrue(status corev1.PodStatus) bool {
	condition := getPodReadyCondition(&status)
	return condition != nil && condition.Status == corev1.ConditionTrue
}

// getPodReadyCondition extracts the pod ready condition from the given status and returns that.
// Returns nil if the condition is not present.
// copied from k8s.io/kubernetes/pkg/api/v1/pod
func getPodReadyCondition(status *corev1.PodStatus) *corev1.PodCondition {
	if status == nil || status.Conditions == nil {
		return nil
	}

	for i := range status.Conditions {
		if status.Conditions[i].Type == corev1.PodReady {
			return &status.Conditions[i]
		}
	}
	return nil
}

// IsPodTerminating returns true if pod's DeletionTimestamp has been set
func IsPodTerminating(pod *corev1.Pod) bool {
	return pod.DeletionTimestamp != nil
}

// IsPodFailed returns true if pod has a Phase of PodFailed.
func IsPodFailed(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodFailed
}

func ExpectedPodNum(ms *workloadv1alpha1.ModelServing) int {
	num := 0
	for _, role := range ms.Spec.Template.Roles {
		// Calculate the expected number of pod replicas when the role is running normally
		// For each role, the expected number of pods is (entryPod.num + workerPod.num) * role.replicas
		num += (1 + int(role.WorkerReplicas)) * int(*role.Replicas)
	}
	return num
}

// ContainerRestarted return true when there is any container in the pod that gets restarted
func ContainerRestarted(pod *corev1.Pod) bool {
	if pod.Status.Phase == corev1.PodRunning || pod.Status.Phase == corev1.PodPending {
		for j := range pod.Status.InitContainerStatuses {
			stat := pod.Status.InitContainerStatuses[j]
			if stat.RestartCount > 0 {
				return true
			}
		}
		for j := range pod.Status.ContainerStatuses {
			stat := pod.Status.ContainerStatuses[j]
			if stat.RestartCount > 0 {
				return true
			}
		}
	}
	return false
}

func newCondition(condType workloadv1alpha1.ModelServingConditionType, message string) metav1.Condition {
	var conditionType, reason string
	switch condType {
	case workloadv1alpha1.ModelServingAvailable:
		conditionType = string(workloadv1alpha1.ModelServingAvailable)
		reason = "AllGroupsReady"
	case workloadv1alpha1.ModelServingProgressing:
		conditionType = string(workloadv1alpha1.ModelServingProgressing)
		reason = "GroupProgressing"
	case workloadv1alpha1.ModelServingUpdateInProgress:
		conditionType = string(workloadv1alpha1.ModelServingUpdateInProgress)
		reason = "GroupsUpdating"
	}

	return metav1.Condition{
		Type:               conditionType,
		Status:             metav1.ConditionStatus(corev1.ConditionTrue),
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
}

func SetCondition(ms *workloadv1alpha1.ModelServing, progressingGroups, updatedGroups, currentGroups []int) bool {
	partition := 0
	if ms.Spec.RolloutStrategy != nil && ms.Spec.RolloutStrategy.RollingUpdateConfiguration != nil && ms.Spec.RolloutStrategy.RollingUpdateConfiguration.Partition != nil {
		p := ms.Spec.RolloutStrategy.RollingUpdateConfiguration.Partition
		if p.Type == intstr.Int {
			partition = int(p.IntVal)
		} else if ms.Spec.Replicas != nil {
			partition, _ = intstr.GetScaledValueFromIntOrPercent(p, int(*ms.Spec.Replicas), true)
		}
	}
	return SetConditionWithRolloutState(ms, progressingGroups, updatedGroups, currentGroups, len(progressingGroups) > 0 && len(currentGroups) > partition)
}

// SetConditionWithRolloutState updates the mutually exclusive availability and
// progress conditions using the controller's ordinal-aware rollout predicate.
func SetConditionWithRolloutState(ms *workloadv1alpha1.ModelServing, progressingGroups, updatedGroups, currentGroups []int, rolloutActive bool) bool {
	return SetConditionWithRolloutAndProgressState(
		ms, progressingGroups, updatedGroups, currentGroups, rolloutActive, len(progressingGroups) > 0,
	)
}

// SetConditionWithRolloutAndProgressState distinguishes a template rollout
// from other reconciliation work such as initial creation or replica scaling.
func SetConditionWithRolloutAndProgressState(
	ms *workloadv1alpha1.ModelServing,
	progressingGroups, updatedGroups, currentGroups []int,
	rolloutActive, progressActive bool,
) bool {
	var newCond metav1.Condition
	found := false
	shouldUpdate := false

	if rolloutActive {
		message := SomeGroupsAreUpdated + ": " + fmt.Sprintf("%v", updatedGroups)
		if len(progressingGroups) > 0 {
			message = SomeGroupsAreProgressing + ": " + fmt.Sprintf("%v", progressingGroups) + ", " + message
		}
		newCond = newCondition(workloadv1alpha1.ModelServingUpdateInProgress, message)
	} else if !progressActive {
		newCond = newCondition(workloadv1alpha1.ModelServingAvailable, AllGroupsIsReady)
	} else {
		message := SomeGroupsAreProgressing + ": " + fmt.Sprintf("%v", progressingGroups)
		newCond = newCondition(workloadv1alpha1.ModelServingProgressing, message)
	}

	newCond.LastTransitionTime = metav1.Now()
	for i, curCondition := range ms.Status.Conditions {
		if newCond.Type == curCondition.Type {
			if newCond.Status != curCondition.Status {
				ms.Status.Conditions[i] = newCond
				shouldUpdate = true
			}
			found = true
		} else {
			// Available and progressing/updateInprogress are not allowed to be true at the same time.
			if exclusiveConditionTypes(curCondition, newCond) && curCondition.Status == metav1.ConditionTrue && newCond.Status == metav1.ConditionTrue {
				ms.Status.Conditions[i].Status = metav1.ConditionFalse
				shouldUpdate = true
			}
		}
	}

	if newCond.Status == metav1.ConditionTrue && !found {
		ms.Status.Conditions = append(ms.Status.Conditions, newCond)
		shouldUpdate = true
	}

	return shouldUpdate
}

// This function is refer to https://github.com/kubernetes-sigs/lws/blob/main/pkg/controllers/leaderworkerset_controller.go#L840
func exclusiveConditionTypes(condition1 metav1.Condition, condition2 metav1.Condition) bool {
	if (condition1.Type == string(workloadv1alpha1.ModelServingAvailable) && condition2.Type == string(workloadv1alpha1.ModelServingProgressing)) ||
		(condition1.Type == string(workloadv1alpha1.ModelServingProgressing) && condition2.Type == string(workloadv1alpha1.ModelServingAvailable)) {
		return true
	}

	if (condition1.Type == string(workloadv1alpha1.ModelServingAvailable) && condition2.Type == string(workloadv1alpha1.ModelServingUpdateInProgress)) ||
		(condition1.Type == string(workloadv1alpha1.ModelServingUpdateInProgress) && condition2.Type == string(workloadv1alpha1.ModelServingAvailable)) {
		return true
	}

	return false
}

// ParseAdmissionRequest parses the HTTP request and extracts the AdmissionReview and ModelServing.
func ParseModelServingFromRequest(r *http.Request) (*admissionv1.AdmissionReview, *workloadv1alpha1.ModelServing, error) {
	// Verify the content type is accurate
	contentType := r.Header.Get("Content-Type")
	if contentType != "application/json" {
		return nil, nil, fmt.Errorf("invalid Content-Type, expected application/json, got %s", contentType)
	}

	var body []byte
	if r.Body != nil {
		defer r.Body.Close()
		data, err := io.ReadAll(r.Body)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read request body: %v", err)
		}
		body = data
	}

	// Parse the AdmissionReview request
	var admissionReview admissionv1.AdmissionReview
	if err := json.Unmarshal(body, &admissionReview); err != nil {
		return nil, nil, fmt.Errorf("failed to decode body: %v", err)
	}

	var ms workloadv1alpha1.ModelServing
	if err := json.Unmarshal(admissionReview.Request.Object.Raw, &ms); err != nil {
		return nil, nil, fmt.Errorf("failed to decode modelServing: %v", err)
	}

	return &admissionReview, &ms, nil
}

// SendAdmissionResponse sends the AdmissionReview response back to the client
func SendAdmissionResponse(w http.ResponseWriter, admissionReview *admissionv1.AdmissionReview) error {
	// Send the response
	resp, err := json.Marshal(admissionReview)
	if err != nil {
		return fmt.Errorf("failed to encode response: %v", err)
	}

	klog.V(4).Infof("Sending response: %s", string(resp))
	w.Header().Set("Content-Type", "application/json")
	if _, err := w.Write(resp); err != nil {
		return fmt.Errorf("failed to write response: %v", err)
	}

	return nil
}

// getIndexKeyFromObject extract index key from objects
func getIndexKeyFromObject(obj interface{}) (map[string]string, string, bool) {
	switch v := obj.(type) {
	case *corev1.Pod:
		return v.GetLabels(), v.GetNamespace(), true
	case *corev1.Service:
		return v.GetLabels(), v.GetNamespace(), true
	case *schedulingv1beta1.PodGroup:
		return v.GetLabels(), v.GetNamespace(), true
	default:
		return nil, "", false
	}
}

func GroupNameIndexFunc(obj interface{}) ([]string, error) {
	labels, namespace, ok := getIndexKeyFromObject(obj)
	if !ok {
		return []string{}, nil
	}

	groupName, exists := labels[workloadv1alpha1.GroupNameLabelKey]
	if !exists {
		return []string{}, nil
	}
	compositeKey := fmt.Sprintf("%s/%s", namespace, groupName)
	return []string{compositeKey}, nil
}

func RoleIDIndexFunc(obj interface{}) ([]string, error) {
	labels, namespace, ok := getIndexKeyFromObject(obj)
	if !ok {
		return []string{}, nil
	}

	groupName, groupNameExists := labels[workloadv1alpha1.GroupNameLabelKey]
	roleName, roleNameExists := labels[workloadv1alpha1.RoleLabelKey]
	roleID, roleIDExists := labels[workloadv1alpha1.RoleIDKey]

	if !groupNameExists || !roleNameExists || !roleIDExists || groupName == "" || roleName == "" || roleID == "" {
		return []string{}, nil
	}

	compositeKey := fmt.Sprintf("%s/%s/%s/%s", namespace, groupName, roleName, roleID)
	return []string{compositeKey}, nil
}

// GetMaxUnavailable 解析 ModelServing 层 maxUnavailable(ServingGroupRollingUpdate 用), 默认 1。
// 百分比时按 replicas 算, 向下取整(GetScaledValueFromIntOrPercent ... false)。
//
// why 向下取整: unavailable 是"允许损失多少可用副本", 向下少删一点, 宁可慢也不超额降容(见 §5.3)。
func GetMaxUnavailable(ms *workloadv1alpha1.ModelServing) (int, error) {
	maxUnavailable := intstr.FromInt(1) // 默认 1
	replicas := int(*ms.Spec.Replicas)
	if ms.Spec.RolloutStrategy != nil && ms.Spec.RolloutStrategy.RollingUpdateConfiguration != nil {
		if ms.Spec.RolloutStrategy.RollingUpdateConfiguration.MaxUnavailable != nil {
			maxUnavailable = *ms.Spec.RolloutStrategy.RollingUpdateConfiguration.MaxUnavailable
		}
	}
	return intstr.GetScaledValueFromIntOrPercent(&maxUnavailable, replicas, false)
}

// GetMaxSurge 解析 ModelServing 层 maxSurge, 默认 0。百分比时按 replicas 算, 向上取整(... true)。
//
// why 向上取整: surge 是"允许临时多建多少", 向上多建一点, 保证有足够新容量顶替(见 §5.3)。
// 默认 0: 先删后建(替换式), 不要求额外资源余量即可滚动, 资源友好; surge 是 opt-in 奢侈。
func GetMaxSurge(ms *workloadv1alpha1.ModelServing) (int, error) {
	maxSurge := intstr.FromInt(0)
	if ms.Spec.Replicas == nil {
		return 0, fmt.Errorf("replicas must be set")
	}
	replicas := int(*ms.Spec.Replicas)
	if ms.Spec.RolloutStrategy != nil && ms.Spec.RolloutStrategy.RollingUpdateConfiguration != nil {
		if ms.Spec.RolloutStrategy.RollingUpdateConfiguration.MaxSurge != nil {
			maxSurge = *ms.Spec.RolloutStrategy.RollingUpdateConfiguration.MaxSurge
		}
	}
	return intstr.GetScaledValueFromIntOrPercent(&maxSurge, replicas, true)
}

func GetMaxUnavailableForRole(role workloadv1alpha1.Role) (int, bool, error) {
	if role.MaxUnavailable == nil {
		return 0, false, nil
	}
	replicas := 1
	if role.Replicas != nil {
		replicas = int(*role.Replicas)
	}
	maxUnavailable, err := intstr.GetScaledValueFromIntOrPercent(role.MaxUnavailable, replicas, false)
	return maxUnavailable, true, err
}

// GetMaxSurgeForRole resolves a Role's surge budget against its latest desired
// replica count. Percentage values are rounded up.
func GetMaxSurgeForRole(role workloadv1alpha1.Role) (int, error) {
	if role.MaxSurge == nil {
		return 0, nil
	}
	replicas := roleReplicas(role)
	return intstr.GetScaledValueFromIntOrPercent(role.MaxSurge, replicas, true)
}

func roleReplicas(role workloadv1alpha1.Role) int {
	if role.Replicas == nil {
		return 1
	}
	return int(*role.Replicas)
}
