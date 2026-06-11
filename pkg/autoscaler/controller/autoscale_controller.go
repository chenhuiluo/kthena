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

import (
	"context"
	"fmt"
	"time"

	"github.com/volcano-sh/kthena/pkg/autoscaler/autoscaler"
	corev1 "k8s.io/api/core/v1"

	clientset "github.com/volcano-sh/kthena/client-go/clientset/versioned"
	informersv1alpha1 "github.com/volcano-sh/kthena/client-go/informers/externalversions"
	workloadLister "github.com/volcano-sh/kthena/client-go/listers/workload/v1alpha1"
	workload "github.com/volcano-sh/kthena/pkg/apis/workload/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/autoscaler/util"
	"istio.io/istio/pkg/util/sets"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	listerv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// AutoscaleController 是 Kthena 弹性伸缩的核心控制器。
// 它每 15 秒执行一次调和循环（Reconcile），遍历所有 AutoscalingPolicy，
// 根据策略类型分发到 Homogeneous（单角色独立伸缩）或 Heterogeneous（多角色联合伸缩）路径。
//
// PD 分离场景下的典型用法：
//   - 创建两个 Homogeneous Policy：一个伸缩 Prefill 角色，一个伸缩 Decode 角色
//   - 或者创建一个 Heterogeneous Policy：同时管理 Prefill + Decode，按成本优先级分配副本
//
// 内部维护两个 map：
//   - scalerMap:    Homogeneous 路径，key = namespace/policyName/targetNamespace/targetKind/targetRefName
//   - optimizerMap: Heterogeneous 路径，key = namespace/policyName（因为涉及多个 target，key 中不含 targetRef）
type AutoscaleController struct {
	// Client for k8s. Use it to call K8S API
	kubeClient kubernetes.Interface
	// client for custom resource
	client                      clientset.Interface
	autoscalingPoliciesLister   workloadLister.AutoscalingPolicyLister
	autoscalingPoliciesInformer cache.Controller
	modelServingLister          workloadLister.ModelServingLister
	modelServingInformer        cache.Controller
	podsLister                  listerv1.PodLister
	podsInformer                cache.Controller
	scalerMap                   map[string]*autoscaler.Autoscaler
	optimizerMap                map[string]*autoscaler.Optimizer
}

func NewAutoscaleController(kubeClient kubernetes.Interface, client clientset.Interface) *AutoscaleController {
	informerFactory := informersv1alpha1.NewSharedInformerFactory(client, 0)
	modelInferInformer := informerFactory.Workload().V1alpha1().ModelServings()
	autoscalingPoliciesInformer := informerFactory.Workload().V1alpha1().AutoscalingPolicies()

	selector, err := labels.NewRequirement(workload.GroupNameLabelKey, selection.Exists, nil)
	if err != nil {
		klog.Errorf("can not create label selector,err:%v", err)
		return nil
	}
	kubeInformerFactory := informers.NewSharedInformerFactoryWithOptions(
		kubeClient, 0, informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = selector.String()
		}),
	)
	podsInformer := kubeInformerFactory.Core().V1().Pods()
	ac := &AutoscaleController{
		kubeClient:                  kubeClient,
		client:                      client,
		autoscalingPoliciesLister:   autoscalingPoliciesInformer.Lister(),
		autoscalingPoliciesInformer: autoscalingPoliciesInformer.Informer(),
		modelServingLister:          modelInferInformer.Lister(),
		modelServingInformer:        modelInferInformer.Informer(),
		podsLister:                  podsInformer.Lister(),
		podsInformer:                podsInformer.Informer(),
		scalerMap:                   make(map[string]*autoscaler.Autoscaler),
		optimizerMap:                make(map[string]*autoscaler.Optimizer),
	}
	return ac
}

// 3 个 Informer 的职责和必要性：
//
//  1. autoscalingPoliciesInformer — 监听 AutoscalingPolicy CRD
//     用途：schedule() 中读取伸缩策略定义（指标目标值、Behavior 配置、恐慌阈值等）
//     必要性：没有 Policy 定义，Autoscaler 不知道用什么指标、什么阈值来做伸缩决策
//     注意：社区已将 AutoscalingPolicyBinding 合并入 AutoscalingPolicy，不再需要单独的 Binding Informer
//
//  2. modelServingInformer — 监听 ModelServing CRD
//     用途：
//     a) getTargetReplicas()：读取当前 spec.replicas 或 roles[x].replicas（伸缩的"当前值"）
//     b) updateTargetReplicas()：读取最新 ModelServing 后写回新副本数（避免用过期数据 patch）
//     必要性：ModelServing 是伸缩的目标对象，不知道它的结构就无法定位 roles[x].replicas
//     特别是 PD 分离场景，Prefill 和 Decode 是同一 ModelServing 下的两个 Role
//
//  3. podsInformer — 监听 Pod 资源
//     用途：
//     a) MetricCollector.UpdateMetrics() → GetMetricPods()：通过标签选择器找到目标 Pod，
//     然后 HTTP GET 每个 Pod 的 /metrics 端点采集伸缩指标
//     b) evaluatePodsReadiness()：判断 Pod 是否 Running+Ready，决定是否纳入指标计算
//     必要性：Kthena 的 Pod 直采模式不依赖 metrics-server 或 Prometheus，
//     需要自己发现 Pod、遍历 Pod、直连 Pod 抓取指标——
//     这正是 Kthena 可以完全不依赖 Prometheus 的关键原因
//
// 三者的协作关系：
//
//	Policy ──包含──► Target（提供伸缩策略 + 目标绑定）
//	  │
//	  └──指向──► ModelServing（提供当前副本数 + 接收新副本数）
//	                    │
//	                    └──创建──► Pod（提供运行时指标）
//
// AutoscaleController 的 Run() 方法按顺序启动这 3 个 Informer，
// 等待缓存同步完成后，再开始以 15 秒为周期执行 Reconcile()。
// 如果不等待缓存同步，List/Get 操作会返回空数据，导致错误伸缩。
func (ac *AutoscaleController) Run(ctx context.Context) {
	defer utilruntime.HandleCrash()

	// start informers
	go ac.autoscalingPoliciesInformer.RunWithContext(ctx)
	go ac.modelServingInformer.RunWithContext(ctx)
	go ac.podsInformer.RunWithContext(ctx)
	cache.WaitForCacheSync(ctx.Done(),
		ac.autoscalingPoliciesInformer.HasSynced,
		ac.modelServingInformer.HasSynced,
		ac.podsInformer.HasSynced,
	)

	klog.Info("start autoscale controller")
	go wait.Until(func() {
		ac.Reconcile(ctx)
	}, util.AutoscalingSyncPeriodSeconds*time.Second, nil)

	<-ctx.Done()
	klog.Info("shut down autoscale controller")
}

// Reconcile 是弹性伸缩的主调和循环，每 15 秒执行一次。完整流程：
//  1. 列出集群中所有 AutoscalingPolicy
//  2. 按目标类型分为 scalerSet（Homogeneous）和 optimizerSet（Heterogeneous）
//  3. 垃圾回收：删除 scalerMap/optimizerMap 中不再对应任何 policy 的条目
//  4. 对每个 policy 调用 schedule()，走 doScale() 或 doOptimize() 路径
func (ac *AutoscaleController) Reconcile(ctx context.Context) {
	klog.V(4).Info("start to reconcile")
	ctx, cancel := context.WithTimeout(ctx, util.AutoscaleCtxTimeoutSeconds*time.Second)
	defer cancel()
	// 步骤1：列出所有 AutoscalingPolicy
	policies, err := ac.autoscalingPoliciesLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("failed to list autoscaling policies, err: %v", err)
		return
	}

	// 步骤2：按目标类型分类
	// HomogeneousTarget → scalerSet（单角色独立伸缩，如单独伸缩 Prefill 或 Decode）
	// HeterogeneousTarget → optimizerSet（多角色联合伸缩，如同时管理 Prefill+Decode）
	scalerSet := sets.New[string]()
	optimizerSet := sets.New[string]()

	for _, policy := range policies {
		if policy.Spec.HomogeneousTarget != nil {
			scalerSet.Insert(formatAutoscalerMapKey(policy.Namespace, policy.Name, &policy.Spec.HomogeneousTarget.Target.TargetRef))
		} else if policy.Spec.HeterogeneousTarget != nil {
			optimizerSet.Insert(formatAutoscalerMapKey(policy.Namespace, policy.Name, nil))
		} else if policy.Spec.DisaggregatedTarget != nil {
			klog.V(2).Infof("disaggregated target scaling is not yet implemented, skip policy: %s", policy.Name)
		} else {
			klog.Warningf("no target set, policy name: %s", policy.Name)
		}
	}

	// 步骤3：垃圾回收——清理不再有对应 policy 的 scaler/optimizer 实例
	for key := range ac.scalerMap {
		if !scalerSet.Contains(key) {
			delete(ac.scalerMap, key)
		}
	}

	for key := range ac.optimizerMap {
		if !optimizerSet.Contains(key) {
			delete(ac.optimizerMap, key)
		}
	}

	// 步骤4：遍历所有 policy，逐个执行伸缩调度
	for _, policy := range policies {
		err := ac.schedule(ctx, policy)
		if err != nil {
			klog.Errorf("failed to process autoscale,err: %v", err)
			continue
		}
	}
}

// updateTargetReplicas 将新的副本数写回 ModelServing CRD。有两种 Patch 模式：
//
// 模式1 — Merge Patch（SubTarget 为 nil）：
//
//	直接修改 spec.replicas（顶层副本数）。适用于整个 ModelServing 只有一个角色的场景。
//	Patch 内容：{"spec":{"replicas":N}}
//
// 模式2 — JSON Patch（SubTarget 指定 Role 名称）：
//
//	修改 spec.template.roles[index].replicas。PD 分离场景走这条路径——
//	Prefill 和 Decode 是同一 ModelServing 中的不同 Role，需要精确定位到某个 Role 的 replicas。
//	Patch 内容：[{"op":"test","path":"/spec/template/roles/{index}/name","value":"{roleName}"},
//	             {"op":"add","path":"/spec/template/roles/{index}/replicas","value":N}]
//	其中 "test" 操作实现了字段级乐观锁：如果 Role 名称已被修改（如被人手动改了），patch 会失败。
func (ac *AutoscaleController) updateTargetReplicas(ctx context.Context, target *workload.Target, defaultNamespace string, replicas int32) error {
	targetRef := target.TargetRef
	namespaceScope := targetRef.Namespace
	if namespaceScope == "" {
		namespaceScope = defaultNamespace
	}

	if target.TargetRef.Kind != "" && target.TargetRef.Kind != workload.ModelServingKind.Kind {
		return fmt.Errorf("target ref kind %s, name: %s not supported", targetRef.Kind, targetRef.Name)
	}

	instance, err := ac.modelServingLister.ModelServings(namespaceScope).Get(targetRef.Name)
	if err != nil {
		return err
	}

	if instance.Spec.Replicas != nil && *instance.Spec.Replicas == replicas {
		return nil
	}
	patchBytes := []byte(fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas))
	_, err = ac.client.WorkloadV1alpha1().ModelServings(namespaceScope).Patch(
		ctx, targetRef.Name, types.MergePatchType, patchBytes, metav1.PatchOptions{})
	return err
}

// getTargetReplicas 读取目标当前的副本数。
// 同样分两种情况：SubTarget 为 nil 时读 spec.replicas；否则读 spec.template.roles[name].replicas。
func (ac *AutoscaleController) getTargetReplicas(target *workload.Target, defaultNamespace string) (int32, error) {
	targetRef := target.TargetRef
	namespaceScope := targetRef.Namespace
	if namespaceScope == "" {
		namespaceScope = defaultNamespace
	}

	if targetRef.Kind == workload.ModelServingKind.Kind || targetRef.Kind == "" {
		instance, err := ac.modelServingLister.ModelServings(namespaceScope).Get(targetRef.Name)
		if err != nil {
			return 0, err
		}
		if instance.Spec.Replicas != nil {
			return *instance.Spec.Replicas, nil
		}
	}
	return 0, fmt.Errorf("target ref kind %s, name: %s not supported", targetRef.Kind, targetRef.Name)
}

// schedule 是单个 policy 的伸缩调度入口。流程：
//  1. 根据 policy 类型分发：
//     - HeterogeneousTarget != nil → doOptimize()（异构联合伸缩）
//     - HomogeneousTarget != nil   → doScale()（同构单角色伸缩）
func (ac *AutoscaleController) schedule(ctx context.Context, autoscalePolicy *workload.AutoscalingPolicy) error {
	klog.V(2).Infof("start to process autoscaling policy %s", klog.KObj(autoscalePolicy))
	if autoscalePolicy.Spec.HeterogeneousTarget != nil {
		if err := ac.doOptimize(ctx, autoscalePolicy); err != nil {
			klog.Errorf("failed to do optimize, err: %v", err)
			return err
		}
	} else if autoscalePolicy.Spec.HomogeneousTarget != nil {
		if err := ac.doScale(ctx, autoscalePolicy); err != nil {
			klog.Errorf("failed to do scale, err: %v", err)
			return err
		}
	} else if autoscalePolicy.Spec.DisaggregatedTarget != nil {
		klog.V(2).Infof("disaggregated target scaling is not yet implemented, skip policy: %s", autoscalePolicy.Name)
	} else {
		klog.Warningf("policy %s has no target configuration", autoscalePolicy.Name)
	}

	return nil
}

// doOptimize 是异构联合伸缩路径。流程：
//  1. 获取或创建 Optimizer（如果 policy 的 generation 变化则重建）
//  2. 遍历所有后端（param），获取每个后端当前的副本数 → replicasMap
//  3. 调用 optimizer.Optimize() → 得到聚合推荐值，再由 RestoreReplicasOfEachBackend 分配到各后端
//  4. 对每个后端调用 updateTargetReplicas()，通过 JSON Patch 写回各自的副本数
//
// 异构伸缩的核心思路：先把所有后端看作一个整体算总推荐副本数，再按成本优先级分配。
// 例如有 A100 和 T4 两种后端，T4 更便宜，则优先把副本分配给 T4，A100 只在 T4 满了之后才分配。
func (ac *AutoscaleController) doOptimize(ctx context.Context, autoscalePolicy *workload.AutoscalingPolicy) error {
	key := formatAutoscalerMapKey(autoscalePolicy.Namespace, autoscalePolicy.Name, nil)
	optimizer, ok := ac.optimizerMap[key]
	if !ok || optimizer.NeedUpdate(autoscalePolicy) {
		optimizer = autoscaler.NewOptimizer(autoscalePolicy)
		ac.optimizerMap[key] = optimizer
		klog.Infof("asp: %s changed, create new optimizer", autoscalePolicy.Name)
	}
	// Fetch current replicas
	replicasMap := make(map[string]int32, len(optimizer.Meta.Config.Params))
	for _, param := range optimizer.Meta.Config.Params {
		currentInstancesCount, err := ac.getTargetReplicas(&param.Target, autoscalePolicy.Namespace)
		if err != nil {
			klog.Errorf("failed to get current replicas, err: %v", err)
			return err
		}
		replicasMap[param.Target.TargetRef.Name] = currentInstancesCount
	}

	// Get recommended replicas
	recommendedInstances, err := optimizer.Optimize(ctx, ac.podsLister, autoscalePolicy, replicasMap)
	if err != nil {
		klog.Errorf("failed to do optimize, err: %v", err)
		return err
	}
	// Do update replicas
	for _, param := range optimizer.Meta.Config.Params {
		instancesCount, exists := recommendedInstances[param.Target.TargetRef.Name]
		if !exists {
			klog.Warningf("recommended instances not exists, target ref name: %s", param.Target.TargetRef.Name)
			continue
		}
		if err := ac.updateTargetReplicas(ctx, &param.Target, autoscalePolicy.Namespace, instancesCount); err != nil {
			klog.Errorf("failed to update target kind:%s name: %s replicas:%d, err: %v", param.Target.TargetRef.Kind, param.Target.TargetRef.Name, instancesCount, err)
			return err
		}
	}

	return nil
}

// doScale 是同构单角色伸缩路径。流程：
//  1. 获取或创建 Autoscaler（如果 policy 的 generation 变化则重建）
//  2. 获取目标当前的副本数
//  3. 调用 scaler.Scale() → 两阶段：推荐算法算出推荐值，修正算法算出修正值
//  4. 如果推荐值 >= 0，调用 updateTargetReplicas() 写回
//
// PD 分离场景下，Prefill 角色和 Decode 角色各自走一次 doScale，
// 两者之间没有任何协调——这就是缺陷 #4（独立角色伸缩器缺乏协调）的来源。
func (ac *AutoscaleController) doScale(ctx context.Context, autoscalePolicy *workload.AutoscalingPolicy) error {
	target := autoscalePolicy.Spec.HomogeneousTarget.Target
	key := formatAutoscalerMapKey(autoscalePolicy.Namespace, autoscalePolicy.Name, &target.TargetRef)
	scaler, ok := ac.scalerMap[key]
	if !ok || scaler.NeedUpdate(autoscalePolicy) {
		scaler = autoscaler.NewAutoscaler(autoscalePolicy)
		ac.scalerMap[key] = scaler
		klog.Infof("asp: %s changed, create new scaler", autoscalePolicy.Name)
	}
	// Fetch current replicas
	currentInstancesCount, err := ac.getTargetReplicas(&target, autoscalePolicy.Namespace)
	if err != nil {
		klog.Errorf("failed to get current replicas, err: %v", err)
		return err
	}
	// Get recommended replicas
	klog.InfoS("do homogeneous scaling for target", "targetRef", target.TargetRef, "currentInstancesCount", currentInstancesCount)
	recommendedInstances, err := scaler.Scale(ctx, ac.podsLister, autoscalePolicy, currentInstancesCount)
	if err != nil {
		klog.Errorf("failed to do homogeneous scaling for target %s, err: %v", target.TargetRef.Name, err)
		return err
	}
	if recommendedInstances < 0 {
		return nil
	}
	// Do update replicas
	if err := ac.updateTargetReplicas(ctx, &target, autoscalePolicy.Namespace, recommendedInstances); err != nil {
		klog.Errorf("failed to update target replicas %s, err: %v", target.TargetRef.Name, err)
		return err
	}
	klog.InfoS("successfully update target replicas", "targetRef", target.TargetRef, "recommendedInstances", recommendedInstances)
	return nil
}

// formatAutoscalerMapKey 生成 scalerMap/optimizerMap 的 key。
// Homogeneous 路径：key 包含 target 信息 → "ns/policyName/targetNs/targetKind/targetName"
//
//	同一 policy 不同 target 会产生不同 key，所以每个 target 有独立的 Autoscaler
//
// Heterogeneous 路径：targetRef 为 nil → key = "ns/policyName"
//
//	一个 policy 下所有后端共享一个 Optimizer，通过 ReplicaBlock 分配
func formatAutoscalerMapKey(policyNamespace, policyName string, targetRef *corev1.ObjectReference) string {
	key := policyNamespace + "/" + policyName
	if targetRef != nil {
		targetKind := targetRef.Kind
		if targetKind == "" {
			targetKind = workload.ModelServingKind.Kind
		}
		targetNamespace := targetRef.Namespace
		if targetNamespace == "" {
			targetNamespace = policyNamespace
		}
		key += "/" + targetNamespace + "/" + targetKind + "/" + targetRef.Name
	}
	return key
}
