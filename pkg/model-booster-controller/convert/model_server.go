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

package convert

import (
	"fmt"
	"sort"
	"strings"
	"time"

	networking "github.com/volcano-sh/kthena/pkg/apis/networking/v1alpha1"
	workload "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/model-booster-controller/utils"
	icUtils "github.com/volcano-sh/kthena/pkg/model-serving-controller/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// =============================================================================
// convert ModelServer 渲染 — 构造 ModelServer(PD 配对锚键 + KV connector 解析)
// =============================================================================
//
// 【职责】把 ModelBooster 转成 ModelServer CR, 关键是:
//   ① WorkloadSelector.MatchLabels={OwnerUIDKey: model.UID} 选中本 ModelServing 的 Pod
//   ② PDGroup=getPdGroup(声明 PD 配对锚键, 仅 disaggregated 后端生成)
//   ③ KVConnector=GetKvConnectorSpec(从 prefill/decode worker 的 kv-transfer-config 解析, 强制两侧同 type)
//
// 【注意】controller 只"声明锚键"(PDGroup), 不执行配对。真正配对在 router 侧按 label 分桶(见 §4.1)。
// =============================================================================

// VLLMKvConnectorType vLLM connector 名 → kthena KVConnectorType 映射
var VLLMKvConnectorType = map[string]networking.KVConnectorType{
	"MooncakeConnector":   networking.ConnectorTypeMoonCake,
	"MooncakeConnectorV1": networking.ConnectorTypeMoonCake,
	"NixlConnector":       networking.ConnectorTypeNIXL,
	"LMCacheConnectorV1":  networking.ConnectorTypeLMCache,
}

// vLLM kv_role 值。kv_both 允许 prefill 或 decode 任一侧用(某些 connector 如 NixlConnector
// 让单 worker 兼任 producer/consumer)。
const (
	kvRoleProducer = "kv_producer"
	kvRoleConsumer = "kv_consumer"
	kvRoleBoth     = "kv_both"
)

// kvRolesByWorkerType 每 worker type 可声明的 kv_role:
//   prefill 只能 kv_producer 或 kv_both; decode 只能 kv_consumer 或 kv_both。
var kvRolesByWorkerType = map[workload.ModelWorkerType][]string{
	workload.ModelWorkerTypePrefill: {kvRoleProducer, kvRoleBoth},
	workload.ModelWorkerTypeDecode:  {kvRoleConsumer, kvRoleBoth},
}

// kvWorkerConnector 单个 prefill/decode worker 的 kv-transfer-config 解析结果。
type kvWorkerConnector struct {
	connectorName string
	connectorType networking.KVConnectorType
	role          string
}

// BuildModelServer creates arrays of ModelServer for the given model.
// Each model backend will create one model server.
func BuildModelServer(model *workload.ModelBooster) ([]*networking.ModelServer, error) {
	var modelServers []*networking.ModelServer
	var backend = model.Spec.Backend
	var inferenceEngine networking.InferenceEngine
	switch backend.Type {
	case workload.ModelBackendTypeVLLM, workload.ModelBackendTypeVLLMDisaggregated:
		inferenceEngine = networking.VLLM
	default:
		return nil, fmt.Errorf("not support %s backend yet, please use vLLM backend", backend.Type)
	}
	enginePort, err := utils.GetEnginePort(&backend)
	if err != nil {
		return nil, err
	}
	servedModelName := getServedModelName(model, backend)
	pdGroup := getPdGroup(backend)
	kvConnector, err := GetKvConnectorSpec(backend)
	if err != nil {
		return nil, err
	}
	modelServer := networking.ModelServer{
		TypeMeta: metav1.TypeMeta{
			Kind:       networking.ModelServerKind,
			APIVersion: networking.GroupVersion.String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      utils.GetBackendResourceName(model.Name, backend.Name),
			Namespace: model.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				utils.NewModelOwnerRef(model),
			},
		},
		Spec: networking.ModelServerSpec{
			Model:           &servedModelName,
			InferenceEngine: inferenceEngine,
			WorkloadSelector: &networking.WorkloadSelector{
				MatchLabels: map[string]string{
					utils.OwnerUIDKey: string(model.UID),
				},
				PDGroup: pdGroup,
			},
			WorkloadPort: networking.WorkloadPort{
				Port: enginePort,
			},
			TrafficPolicy: &networking.TrafficPolicy{
				Retry: &networking.Retry{
					Attempts:      5,
					RetryInterval: &metav1.Duration{Duration: time.Duration(0) * time.Second},
				},
			},
			KVConnector: kvConnector,
		},
	}
	modelServer.Labels = utils.GetModelControllerLabels(model, backend.Name, icUtils.Revision(modelServer.Spec))
	modelServers = append(modelServers, &modelServer)

	return modelServers, nil
}

// GetKvConnectorSpec 从 prefill/decode worker 的 kv-transfer-config 解析 KVConnectorSpec。
// 导出供 webhook 复用同一套解析/校验规则。
//
// 三个强制校验(确保 P/D 两侧对齐, 否则 PD 无法协作):
//   ① 至多 1 prefill + 1 decode worker(重复报错, 避免结果依赖 worker 顺序)
//   ② 两侧同进同出: 任一侧有 kv-transfer-config 而另一侧没有 → 报错
//   ③ 两侧 connector type 相同 → 最终只返回一个 type(整个 ModelServer 共用一个 connector)
//   ④ kv_role 校验: prefill 只能 producer/both, decode 只能 consumer/both
// 两侧都没配 kv-transfer-config 也合法(KV 传输可选, 返回 nil,nil)。
//
// GetKvConnectorSpec derives the KVConnectorSpec for a backend from its prefill/decode
// workers' kv-transfer-config. It is exported so webhook validation can reuse the exact
// same parsing/consistency rules instead of duplicating them.
//
// A backend that has no kv-transfer-config on either its prefill or decode worker is
// valid (KV transfer is optional, e.g. PD backends that rely on another transfer
// mechanism); this returns (nil, nil). Once either worker declares a kv_connector,
// both workers must declare the same connector with a role compatible with their worker
// type, otherwise this returns a descriptive error. The result never depends on the
// order of backend.Workers.
//
// A backend must have at most one prefill worker and at most one decode worker: the
// ModelWorker API scales a role via Replicas/Pods on a single entry, not by repeating
// the worker type, so a second entry of the same type is rejected rather than silently
// picked (which would make the result depend on worker order).
func GetKvConnectorSpec(backend workload.ModelBackend) (*networking.KVConnectorSpec, error) {
	var prefillWorker, decodeWorker *workload.ModelWorker
	prefillCount, decodeCount := 0, 0

	for i := range backend.Workers {
		worker := &backend.Workers[i]
		switch worker.Type {
		case workload.ModelWorkerTypePrefill:
			prefillCount++
			prefillWorker = worker
		case workload.ModelWorkerTypeDecode:
			decodeCount++
			decodeWorker = worker
		}
	}
	if prefillCount > 1 {
		return nil, fmt.Errorf("backend %s: found %d prefill workers, expected at most 1; duplicate prefill workers are not supported",
			backend.Name, prefillCount)
	}
	if decodeCount > 1 {
		return nil, fmt.Errorf("backend %s: found %d decode workers, expected at most 1; duplicate decode workers are not supported",
			backend.Name, decodeCount)
	}

	var prefillConn, decodeConn *kvWorkerConnector
	if prefillWorker != nil {
		conn, err := parseKvWorkerConnector(*prefillWorker)
		if err != nil {
			return nil, err
		}
		prefillConn = conn
	}
	if decodeWorker != nil {
		conn, err := parseKvWorkerConnector(*decodeWorker)
		if err != nil {
			return nil, err
		}
		decodeConn = conn
	}

	if prefillConn == nil && decodeConn == nil {
		return nil, nil
	}
	if prefillConn == nil {
		return nil, fmt.Errorf(
			"backend %s: decode worker specifies kv_connector %q but the prefill worker has no kv-transfer-config; "+
				"prefill and decode must configure the same kv connector",
			backend.Name, decodeConn.connectorName)
	}
	if decodeConn == nil {
		return nil, fmt.Errorf(
			"backend %s: prefill worker specifies kv_connector %q but the decode worker has no kv-transfer-config; "+
				"prefill and decode must configure the same kv connector",
			backend.Name, prefillConn.connectorName)
	}
	if prefillConn.connectorType != decodeConn.connectorType {
		return nil, fmt.Errorf(
			"backend %s: kv_connector mismatch between prefill (%q) and decode (%q) workers; "+
				"prefill and decode must use the same kv connector",
			backend.Name, prefillConn.connectorName, decodeConn.connectorName)
	}

	return &networking.KVConnectorSpec{Type: prefillConn.connectorType}, nil
}

// parseKvWorkerConnector extracts and validates the kv_connector/kv_role pair from a
// single worker's kv-transfer-config. It returns (nil, nil) when the worker has no
// config or no kv-transfer-config field at all, since KV transfer is optional. Once
// kv-transfer-config is present, kv_connector and kv_role must both be present, the
// connector must be a supported type, and the role must be compatible with the
// worker's type.
func parseKvWorkerConnector(worker workload.ModelWorker) (*kvWorkerConnector, error) {
	if len(worker.Config.Raw) == 0 {
		return nil, nil
	}

	kvTransferConfig, err := utils.TryGetField(worker.Config.Raw, "kv-transfer-config")
	if err != nil {
		return nil, fmt.Errorf("worker %s: invalid config: %w", worker.Type, err)
	}
	if kvTransferConfig == nil {
		return nil, nil
	}
	kvTransferConfigStr, ok := kvTransferConfig.(string)
	if !ok {
		return nil, fmt.Errorf("worker %s: kv-transfer-config must be a JSON string, got %T", worker.Type, kvTransferConfig)
	}

	kvConnectorField, err := utils.TryGetField([]byte(kvTransferConfigStr), "kv_connector")
	if err != nil {
		return nil, fmt.Errorf("worker %s: invalid kv-transfer-config: %w", worker.Type, err)
	}
	connectorName, ok := kvConnectorField.(string)
	if !ok || connectorName == "" {
		return nil, fmt.Errorf("worker %s: kv-transfer-config is set but kv_connector is missing", worker.Type)
	}
	connectorType, exists := VLLMKvConnectorType[connectorName]
	if !exists {
		return nil, fmt.Errorf("worker %s: unsupported kv_connector %q (supported: %s)",
			worker.Type, connectorName, strings.Join(supportedKvConnectorNames(), ", "))
	}

	kvRoleField, err := utils.TryGetField([]byte(kvTransferConfigStr), "kv_role")
	if err != nil {
		return nil, fmt.Errorf("worker %s: invalid kv-transfer-config: %w", worker.Type, err)
	}
	role, ok := kvRoleField.(string)
	if !ok || role == "" {
		return nil, fmt.Errorf("worker %s: kv-transfer-config is set but kv_role is missing", worker.Type)
	}
	allowedRoles := kvRolesByWorkerType[worker.Type]
	if !containsString(allowedRoles, role) {
		return nil, fmt.Errorf("worker %s: invalid kv_role %q, expected one of [%s]",
			worker.Type, role, strings.Join(allowedRoles, ", "))
	}

	return &kvWorkerConnector{connectorName: connectorName, connectorType: connectorType, role: role}, nil
}

func supportedKvConnectorNames() []string {
	names := make([]string, 0, len(VLLMKvConnectorType))
	for name := range VLLMKvConnectorType {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func containsString(list []string, s string) bool {
	for _, item := range list {
		if item == s {
			return true
		}
	}
	return false
}

// getPdGroup 生成 PDGroup(仅 disaggregated 后端: vLLMDisaggregated/MindIEDisaggregated)。
//   GroupKey = GroupNameLabelKey(group-name label), 值=ServingGroup 名
//   PrefillLabels = {role: prefill}, DecodeLabels = {role: decode}
//
// why GroupKey=ServingGroup 名(group-name label): 调度边界与配对边界重合 —
//   ServingGroup 既是 gang 调度单元(PodGroup 名=组名)又是 PD 配对单元(GroupKey 值=group-name=组名),
//   天然同组配对、跨组不串。若用 ModelServing 名则所有副本混一桶失隔离; 若用 RoleIDKey 则 pre/decode 不同值无法对齐。
func getPdGroup(backend workload.ModelBackend) *networking.PDGroup {
	switch backend.Type {
	case workload.ModelBackendTypeVLLMDisaggregated, workload.ModelBackendTypeMindIEDisaggregated:
		return &networking.PDGroup{
			GroupKey: workload.GroupNameLabelKey,
			PrefillLabels: map[string]string{
				workload.RoleLabelKey: string(workload.ModelWorkerTypePrefill),
			},
			DecodeLabels: map[string]string{
				workload.RoleLabelKey: string(workload.ModelWorkerTypeDecode),
			},
		}
	}
	return nil
}

// getServedModelName gets served model name from the worker config. Default is the model name.
func getServedModelName(model *workload.ModelBooster, backend workload.ModelBackend) string {
	servedModelName := model.Name
	for _, worker := range backend.Workers {
		if worker.Type == workload.ModelWorkerTypeServer ||
			worker.Type == workload.ModelWorkerTypeDecode {
			valStr, err := utils.TryGetField(worker.Config.Raw, "served-model-name")
			if err != nil {
				return servedModelName
			}
			if valStr == nil {
				continue
			}
			if val, ok := valStr.(string); ok {
				servedModelName = val
				break
			}
		}
	}
	return servedModelName
}
