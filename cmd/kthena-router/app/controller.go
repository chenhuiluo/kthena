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

package app

import (
	"context"
	"fmt"
	"os"
	"strconv"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	inferencev1 "sigs.k8s.io/gateway-api-inference-extension/api/v1"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"
	gatewayclientset "sigs.k8s.io/gateway-api/pkg/client/clientset/versioned"
	gatewayinformers "sigs.k8s.io/gateway-api/pkg/client/informers/externalversions"

	clientset "github.com/volcano-sh/kthena/client-go/clientset/versioned"
	kthenaInformers "github.com/volcano-sh/kthena/client-go/informers/externalversions"
	"github.com/volcano-sh/kthena/pkg/kthena-router/controller"
	"github.com/volcano-sh/kthena/pkg/kthena-router/datastore"
)

// Controller 抽象接口,唯一方法 HasSynced() 用于判断所有控制器是否已完成初始缓存同步
type Controller interface {
	HasSynced() bool
}

// aggregatedController 聚合多个 Controller,实现 Controller 接口
// HasSynced() 只有当所有子控制器都同步完成才返回 true
type aggregatedController struct {
	controllers []Controller
}

// 编译期断言: aggregatedController 实现了 Controller 接口
var _ Controller = &aggregatedController{}

// startControllers 是 kthena-router 所有 K8s 控制器的启动入口
// 参数说明:
//   - store:          DataStore,存储路由表/Pod列表/限流状态/指标,是控制器的写入目标
//   - stop:           信号通道,收到关闭信号时通知所有 Informer 停止
//   - enableGatewayAPI:           是否启用 Gateway API 控制器 (Gateway + HTTPRoute)
//   - defaultPort:   默认 HTTP 监听端口 (字符串),传给 ensureDefaultGateway 创建默认 Gateway 的 listener
//   - enableGatewayAPIInferenceExtension: 是否启用 InferencePool 控制器 (需先启用 Gateway API)
//   - kubeAPIQPS:    K8s API 客户端 QPS 限速 (>0 时覆盖默认值 5)
//   - kubeAPIBurst:  K8s API 客户端 Burst 突发上限 (>0 时覆盖默认值 10)
//
// 返回值: Controller (聚合控制器),调用方通过 HasSynced() 等待缓存就绪
func startControllers(store datastore.Store, stop <-chan struct{}, enableGatewayAPI bool, defaultPort string, enableGatewayAPIInferenceExtension bool, kubeAPIQPS float32, kubeAPIBurst int) Controller {
	// ===== 第 1 步: 构建 K8s REST 客户端配置 =====
	// BuildConfigFromFlags 使用 in-cluster 配置 (空字符串表示使用 Pod 内的 ServiceAccount)
	// 如果不在集群内运行,会返回错误并 Fatal 退出
	cfg, err := clientcmd.BuildConfigFromFlags("", "")
	if err != nil {
		klog.Fatalf("Error building kubeconfig: %s", err.Error())
	}
	// 如果命令行传入了 QPS/Burst 限速参数,覆盖默认配置
	// 防止大量 Informer watch 事件压垮 API Server
	if kubeAPIQPS > 0 {
		cfg.QPS = kubeAPIQPS
	}
	if kubeAPIBurst > 0 {
		cfg.Burst = kubeAPIBurst
	}

	// ===== 第 2 步: 创建三类 K8s 客户端 =====
	// kubeClient: 标准核心资源客户端 (Pod, Namespace 等)
	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("Error building kubernetes clientset: %s", err.Error())
	}

	// kthenaClient: Kthena CRD 客户端 (ModelRoute, ModelServer 等)
	// 由 client-go 代码生成器根据 CRD types 自动生成
	kthenaClient, err := clientset.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("Error building kthena clientset: %s", err.Error())
	}

	// ===== 第 3 步: 创建 SharedInformerFactory =====
	// NewSharedInformerFactory(client, resyncPeriod):
	//   - 共享底层 watch 连接,多个控制器复用同一个 Informer
	//   - resyncPeriod=0: 不做周期性全量重新同步,完全依赖 watch 事件驱动
	// kubeInformerFactory: watch 核心资源 (Pod, Namespace)
	// kthenaInformerFactory: watch Kthena CRD (ModelRoute, ModelServer)
	kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, 0)
	kthenaInformerFactory := kthenaInformers.NewSharedInformerFactory(kthenaClient, 0)

	// ===== 第 4 步: 创建必须控制器 (始终启动) =====
	// ModelRouteController: watch ModelRoute CRD,将模型名→ModelServer 的映射关系写入 DataStore
	// 同时处理限流配置 (RateLimit) 的变更回调
	modelRouteController, err := controller.NewModelRouteController(kthenaInformerFactory, store)
	if err != nil {
		klog.Fatalf("Error creating model route controller: %s", err.Error())
	}
	// ModelServerController: watch ModelServer CRD + Pod 资源
	// 将 ModelServer CR 的 Spec (模型名、端口、PDGroup 等) 和对应 Pod 列表写入 DataStore
	modelServerController, err := controller.NewModelServerController(kthenaInformerFactory, kubeInformerFactory, store)
	if err != nil {
		klog.Fatalf("Error creating model server controller: %s", err.Error())
	}

	// ===== 第 5 步: 注册缓存同步检查点 =====
	// cacheSyncs 收集所有必须同步完成的 Informer 的 HasSynced 函数
	// 后面调用 cache.WaitForCacheSync() 会逐一调用这些函数,全部返回 true 才继续
	// ModelRoute + ModelServer + Pod = 必须同步的三类核心资源
	cacheSyncs := []cache.InformerSynced{
		kthenaInformerFactory.Networking().V1alpha1().ModelRoutes().Informer().HasSynced,
		kthenaInformerFactory.Networking().V1alpha1().ModelServers().Informer().HasSynced,
		kubeInformerFactory.Core().V1().Pods().Informer().HasSynced,
	}

	// ===== 第 6 步: Gateway API 可选控制器 (条件启动) =====
	var gatewayInformerFactory gatewayinformers.SharedInformerFactory
	var gatewayController *controller.GatewayController
	var httpRouteController *controller.HTTPRouteController
	var inferencePoolController *controller.InferencePoolController

	if enableGatewayAPI {
		// 创建 Gateway API CRD 客户端 (Gateway, HTTPRoute, GatewayClass 等)
		gatewayClient, err := gatewayclientset.NewForConfig(cfg)
		if err != nil {
			klog.Fatalf("Error building gateway clientset: %s", err.Error())
		}

		// 启动前先确保默认 GatewayClass 存在
		// GatewayClass 是集群级资源,定义了使用哪个控制器 (此处为 kthena-router)
		// 如果不存在则创建,已存在则跳过
		if err := ensureDefaultGatewayClass(gatewayClient); err != nil {
			klog.Fatalf("Failed to ensure default GatewayClass: %s", err.Error())
		}

		// 启动前先确保默认 Gateway 存在
		// Gateway 是命名空间级资源,定义了监听端口和协议
		// 使用 router 的 --port 参数作为默认端口,命名空间从 POD_NAMESPACE 环境变量获取
		if err := ensureDefaultGateway(gatewayClient, defaultPort); err != nil {
			klog.Fatalf("Failed to ensure default Gateway: %s", err.Error())
		}

		// 创建 Gateway API 的 SharedInformerFactory,watch Gateway + HTTPRoute 资源
		gatewayInformerFactory = gatewayinformers.NewSharedInformerFactory(gatewayClient, 0)

		// GatewayController: watch Gateway CRD,将 Gateway 的 listener 配置写入 DataStore
		gatewayController, err = controller.NewGatewayController(gatewayInformerFactory, store)
		if err != nil {
			klog.Fatalf("Error creating gateway controller: %s", err.Error())
		}
		// 额外注册 Gateway + HTTPRoute 的 Informer 同步检查点
		cacheSyncs = append(cacheSyncs,
			gatewayInformerFactory.Gateway().V1().Gateways().Informer().HasSynced,
			gatewayInformerFactory.Gateway().V1().HTTPRoutes().Informer().HasSynced,
		)

		// ===== 第 6b 步: Inference Extension 可选控制器 =====
		// 这是 Gateway API 的子选项,仅在两者都启用时才启动
		// 包含 HTTPRouteController + InferencePoolController
		if enableGatewayAPIInferenceExtension {
			// HTTPRouteController: watch Gateway API 的 HTTPRoute CRD
			// 将 HTTPRoute 的路由规则 (路径匹配、后端引用) 写入 DataStore
			// 用于处理非 /v1/ 路径的请求,路由到 InferencePool
			httpRouteController, err = controller.NewHTTPRouteController(gatewayInformerFactory, kubeInformerFactory, store)
			if err != nil {
				klog.Fatalf("Error creating httproute controller: %s", err.Error())
			}

			// InferencePool 是 Gateway API Inference Extension 的自定义资源,不属于标准 Gateway API CRD
			// 因此需要 dynamicClient + dynamicInformerFactory 来动态 watch
			dynamicClient, err := dynamic.NewForConfig(cfg)
			if err != nil {
				klog.Fatalf("Error building dynamic client: %s", err.Error())
			}
			dynamicInformerFactory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, 0)

			// InferencePoolController: watch InferencePool CRD
			// InferencePool 定义了一组推理 Pod 的池子 + 目标端口
			// 控制器将池子信息写入 DataStore,供路由时查找
			inferencePoolController, err = controller.NewInferencePoolController(dynamicInformerFactory, store)
			if err != nil {
				klog.Fatalf("Error creating inferencepool controller: %s", err.Error())
			}
			// 额外注册 Namespace + InferencePool 的 Informer 同步检查点
			// Namespace: HTTPRouteController 需要判断路由的命名空间归属
			// InferencePool: InferencePoolController 需要同步池子状态
			cacheSyncs = append(cacheSyncs,
				kubeInformerFactory.Core().V1().Namespaces().Informer().HasSynced,
				dynamicInformerFactory.ForResource(inferencev1.SchemeGroupVersion.WithResource("inferencepools")).Informer().HasSynced,
			)
			// 启动 dynamic Informer 的 watch 循环
			dynamicInformerFactory.Start(stop)
		}
		// 启动 Gateway API Informer 的 watch 循环
		gatewayInformerFactory.Start(stop)
	} else {
		klog.Info("Gateway API controllers are disabled")
	}

	// ===== 第 7 步: 启动所有 Informer 的 watch 循环 =====
	// Start() 会启动底层 reflector,开始对 API Server 做 List + Watch
	// 所有注册的 Informer 共享 watch 连接
	kubeInformerFactory.Start(stop)
	kthenaInformerFactory.Start(stop)

	// ===== 第 8 步: 等待所有 Informer 缓存同步完成 =====
	// WaitForCacheSync 会阻塞,直到所有 cacheSyncs 中的函数都返回 true
	// stop 通道关闭时会提前返回 false,触发 Fatal 退出
	// 缓存未同步完成时绝不能启动路由处理,否则会出现找不到 Pod/ModelServer 的错误
	if !cache.WaitForCacheSync(stop, cacheSyncs...) {
		klog.Fatalf("Failed to sync informer caches")
	}

	// ===== 第 9 步: 启动控制器工作循环 =====
	// 将所有已创建的控制器收集到 controllers 列表中
	controllers := []Controller{modelRouteController, modelServerController}

	// 每个 controller.Run(stop) 内部启动 worker goroutine 持续消费 Informer 事件
	// 用独立 goroutine 启动,避免互相阻塞
	go func() {
		if err := modelRouteController.Run(stop); err != nil {
			klog.Fatalf("Error running model route controller: %s", err.Error())
		}
	}()
	go func() {
		if err := modelServerController.Run(stop); err != nil {
			klog.Fatalf("Error running model server controller: %s", err.Error())
		}
	}()

	// ===== 第 10 步: 启动 Gateway API 可选控制器的工作循环 =====
	if enableGatewayAPI {
		go func() {
			if err := gatewayController.Run(stop); err != nil {
				klog.Fatalf("Error running gateway controller: %s", err.Error())
			}
		}()

		controllers = append(controllers, gatewayController)

		// Inference Extension 控制器是可选的子集
		if enableGatewayAPIInferenceExtension {
			go func() {
				if err := httpRouteController.Run(stop); err != nil {
					klog.Fatalf("Error running httproute controller: %s", err.Error())
				}
			}()
			go func() {
				if err := inferencePoolController.Run(stop); err != nil {
					klog.Fatalf("Error running inferencepool controller: %s", err.Error())
				}
			}()
			controllers = append(controllers, httpRouteController, inferencePoolController)
		} else {
			klog.Info("Gateway API Inference Extension controllers are disabled")
		}
	}

	// 返回聚合控制器,调用方可通过 HasSynced() 判断所有控制器是否就绪
	return &aggregatedController{
		controllers: controllers,
	}
}

// HasSynced 检查所有子控制器是否都已同步完成
// 只要有一个控制器未就绪就返回 false
// 该方法被 Server.Run() 中 cache.WaitForCacheSync 调用,用于阻塞启动直到缓存就绪
func (c *aggregatedController) HasSynced() bool {
	for _, controller := range c.controllers {
		if !controller.HasSynced() {
			return false
		}
	}
	return true
}

// ensureDefaultGatewayClass 创建默认 GatewayClass (集群级资源)
// GatewayClass 定义了"谁负责处理这个 Gateway" — 即控制器名称
// kthena-router 启动时需要确保自己的 GatewayClass 存在,否则 Gateway 无法被绑定
//
// 流程:
//   1. 先 Get 检查是否已存在 → 已存在则直接返回
//   2. 如果错误不是 NotFound → 返回意外错误
//   3. NotFound → 创建新的 GatewayClass,ControllerName 设为 kthena-router
//   4. 如果 Create 返回 AlreadyExists (另一个 router 实例抢先创建了) → 仍算成功
func ensureDefaultGatewayClass(gatewayClient gatewayclientset.Interface) error {
	ctx := context.Background()

	// 检查默认 GatewayClass 是否已存在
	_, err := gatewayClient.GatewayV1().GatewayClasses().Get(ctx, controller.DefaultGatewayClassName, metav1.GetOptions{})
	if err == nil {
		klog.V(2).Infof("Default GatewayClass %s already exists", controller.DefaultGatewayClassName)
		return nil
	}

	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check GatewayClass %s: %w", controller.DefaultGatewayClassName, err)
	}

	// 构造并创建默认 GatewayClass
	// ControllerName 标识由 kthena-router 控制器负责处理关联的 Gateway
	gatewayClass := &gatewayv1.GatewayClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: controller.DefaultGatewayClassName, // 默认名称,由 controller 包常量定义
		},
		Spec: gatewayv1.GatewayClassSpec{
			ControllerName: gatewayv1.GatewayController(controller.ControllerName), // kthena-router 的控制器标识
		},
	}

	_, err = gatewayClient.GatewayV1().GatewayClasses().Create(ctx, gatewayClass, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			// 另一个 router 实例并发创建了同一个 GatewayClass,属于正常情况
			klog.V(2).Infof("GatewayClass %s was created by another process", controller.DefaultGatewayClassName)
			return nil
		}
		return fmt.Errorf("failed to create GatewayClass %s: %w", controller.DefaultGatewayClassName, err)
	}

	klog.Infof("Created default GatewayClass %s", controller.DefaultGatewayClassName)
	return nil
}

// ensureDefaultGateway 创建默认 Gateway (命名空间级资源)
// Gateway 定义了监听端口和协议,是 Gateway API 的核心入口点
// kthena-router 启动时需要确保默认 Gateway 存在,否则请求无法被路由
//
// 流程:
//   1. 确定命名空间: 优先从 POD_NAMESPACE 环境变量读取 (K8s Downward API 注入),
//      不存在则回退到 "default"
//   2. 解析 defaultPort 字符串为整数端口号
//   3. Get 检查是否已存在 → 已存在则直接返回
//   4. NotFound → 创建新的 Gateway,配置 HTTP listener:
//      - 端口: 使用 router 的 --port 参数 (默认 8080)
//      - 协议: HTTP
//      - 允许路由来源: 所有命名空间 (NamespacesFromAll)
//      - Hostname: nil (匹配所有主机名)
func ensureDefaultGateway(gatewayClient gatewayclientset.Interface, defaultPort string) error {
	ctx := context.Background()
	// 默认命名空间,如果 Pod 上注入了 POD_NAMESPACE 则使用实际命名空间
	namespace := "default"
	name := "default"

	// 从环境变量获取 Pod 所在命名空间 (K8s Downward API: fieldRef: fieldPath: metadata.namespace)
	if podNamespace := os.Getenv("POD_NAMESPACE"); podNamespace != "" {
		namespace = podNamespace
	}

	// 将端口字符串转为整数 (如 "8080" → 8080)
	port, err := strconv.Atoi(defaultPort)
	if err != nil {
		return fmt.Errorf("invalid default port %s: %w", defaultPort, err)
	}

	// 检查默认 Gateway 是否已存在
	_, err = gatewayClient.GatewayV1().Gateways(namespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		klog.V(2).Infof("Default Gateway %s/%s already exists", namespace, name)
		return nil
	}

	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to check Gateway %s/%s: %w", namespace, name, err)
	}

	// NamespacesFromAll 表示 Gateway 允许来自所有命名空间的 HTTPRoute 绑定
	namespacesFromAll := gatewayv1.NamespacesFromAll

	// 构造并创建默认 Gateway
	gateway := &gatewayv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,      // "default"
			Namespace: namespace, // Pod 所在命名空间或 "default"
		},
		Spec: gatewayv1.GatewaySpec{
			GatewayClassName: gatewayv1.ObjectName(controller.DefaultGatewayClassName), // 引用上面创建的 GatewayClass
			Listeners: []gatewayv1.Listener{
				{
					Name:     gatewayv1.SectionName("default"), // listener 名称
					Port:     gatewayv1.PortNumber(port),       // 监听端口 (来自 --port 参数)
					Protocol: gatewayv1.HTTPProtocolType,      // 仅支持 HTTP 协议
					AllowedRoutes: &gatewayv1.AllowedRoutes{
						Namespaces: &gatewayv1.RouteNamespaces{From: &namespacesFromAll}, // 允许所有命名空间的路由
					},
					// Hostname 为 nil → 匹配所有主机名 (不限制 SNI/Host 头)
				},
			},
		},
	}

	_, err = gatewayClient.GatewayV1().Gateways(namespace).Create(ctx, gateway, metav1.CreateOptions{})
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			// 另一个 router 实例并发创建了同一个 Gateway,属于正常情况
			klog.V(2).Infof("Gateway %s/%s was created by another process", namespace, name)
			return nil
		}
		return fmt.Errorf("failed to create Gateway %s/%s: %w", namespace, name, err)
	}

	klog.Infof("Created default Gateway %s/%s", namespace, name)
	return nil
}
