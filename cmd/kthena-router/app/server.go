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
	"os"
	"time"

	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"github.com/volcano-sh/kthena/pkg/kthena-router/datastore"
	"github.com/volcano-sh/kthena/pkg/kthena-router/utils"
)

// defaultDrainTimeout 是 HTTP Server 优雅关机的默认超时时间
// 在此时间内等待在途请求完成;超时后强制关闭连接
const defaultDrainTimeout = 5 * time.Minute

// =============================================================================
// kthena-router HTTP Server 定义 & 生命周期管理
// =============================================================================
//
// 【Server 结构体】
//   封装了 Router(请求处理) + DataStore(数据存储) + Controllers(K8s watch) +
//   ListenerManager(Gateway API 多端口) + 配置(TLS/Gateway/debug)
//
// 【Run() 启动流程 — 这是整个路由进程的 main 入口】
//   1. 创建 K8s Client (InClusterConfig)
//   2. 创建 DataStore: 存储路由表/ModelServer/Pod 列表/限流状态/指标缓存
//   3. 初始化 Controller:
//      - ModelRouteController  (必须) — watch ModelRoute CRD
//      - ModelServerController  (必须) — watch ModelServer CRD
//      - GatewayController       (可选, --enable-gateway-api)
//      - HTTPRouteController     (可选, --enable-gateway-api)
//      - InferencePoolController (可选, --enable-gateway-api-inference-extension)
//   4. NewRouter(store): 组装调度器+限流器+分词器
//   5. 启动所有 Informer → 等待缓存同步(HasSynced)
//   6. 启动 Default Server 或 Gateway API Server (startRouter)
//   7. 可选: 启动 Debug Server (--debug-port > 0)
//   8. 阻塞等待 ctx.Done() (SIGTERM/SIGINT 触发 cancel)
//
// 【优雅关机】
//   ctx.Done() → srv.Shutdown(drainTimeout) → 等待在途请求完成
//   → 超时(默认 5 分钟, DRAIN_TIMEOUT 环境变量) → srv.Close() 强制关闭
// =============================================================================

// Server 是 kthena-router 的 HTTP 服务器结构体
// 封装了路由处理核心 + 数据存储 + K8s 控制器 + 监听器管理 + 全部运行配置
type Server struct {
	// store 是核心数据存储,保存路由表/ModelServer/Pod列表/限流状态/指标缓存
	// 在 Run() 中创建,在 Controller 和 Router 之间共享
	store datastore.Store
	// controllers 是聚合控制器,管理所有 K8s Informer + Controller 工作循环
	// 在 Run() 中由 startControllers() 创建
	controllers Controller
	// listenerManager 管理 Gateway API 模式下的多端口监听器
	// 仅在 --enable-gateway-api 时创建
	listenerManager *ListenerManager
	// EnableTLS 是否启用 HTTPS (需要 cert + key 文件)
	EnableTLS bool
	// TLSCertFile TLS 证书文件路径
	TLSCertFile string
	// TLSKeyFile TLS 私钥文件路径
	TLSKeyFile string
	// Port HTTP 监听端口 (字符串格式,如 "8080")
	Port string
	// EnableGatewayAPI 是否启用 Gateway API 控制器 (Gateway + HTTPRoute)
	EnableGatewayAPI bool
	// EnableGatewayAPIInferenceExtension 是否启用 Inference Extension 控制器
	// 需要先启用 Gateway API (--enable-gateway-api)
	EnableGatewayAPIInferenceExtension bool
	// DebugPort 调试服务器端口 (>0 时启动 localhost debug server,提供 pprof + config_dump)
	DebugPort int
	// KubeAPIQPS K8s API 客户端的 QPS 限速 (0 = 使用 client-go 默认值 5)
	KubeAPIQPS float32
	// KubeAPIBurst K8s API 客户端的 Burst 突发上限 (0 = 使用 client-go 默认值 10)
	KubeAPIBurst int
	// drainTimeout 是 HTTP Server 优雅关机的超时时间
	// 不是 DataStore 的状态,而是 HTTP Server 层面的配置
	// 从 DRAIN_TIMEOUT 环境变量解析,默认 5 分钟
	drainTimeout time.Duration
}

// NewServer 创建 Server 实例,保存配置参数
// 注意: 此时不会创建 DataStore / Controller / Router,这些在 Run() 中延迟初始化
// 参数说明:
//   - port:    HTTP 监听端口 (字符串格式)
//   - enableTLS: 是否启用 HTTPS
//   - cert, key: TLS 证书/私钥文件路径
//   - enableGatewayAPI: 是否启用 Gateway API 控制器
//   - enableGatewayAPIInferenceExtension: 是否启用 InferencePool 控制器
//   - debugPort: 调试端口 (0 = 不启动)
//   - kubeAPIQPS/Burst: K8s API 客户端限速参数
func NewServer(port string, enableTLS bool, cert, key string, enableGatewayAPI bool, enableGatewayAPIInferenceExtension bool, debugPort int, kubeAPIQPS float32, kubeAPIBurst int) *Server {
	return &Server{
		store:                              nil, // 在 Run() 中创建
		EnableTLS:                          enableTLS,
		TLSCertFile:                        cert,
		TLSKeyFile:                         key,
		Port:                               port,
		EnableGatewayAPI:                   enableGatewayAPI,
		EnableGatewayAPIInferenceExtension: enableGatewayAPIInferenceExtension,
		DebugPort:                          debugPort,
		KubeAPIQPS:                         kubeAPIQPS,
		KubeAPIBurst:                       kubeAPIBurst,
		drainTimeout:                       parseDrainTimeout(), // 从环境变量解析,立即确定
	}
}

// parseDrainTimeout 从 DRAIN_TIMEOUT 环境变量解析优雅关机超时时间
// 支持任何 Go duration 格式 (如 "5m", "300s", "5m30s")
// 无效值或负值会被忽略,回退到默认 5 分钟
// 此函数在 NewServer() 中调用一次,结果存储在 Server.drainTimeout 中
func parseDrainTimeout() time.Duration {
	if v := os.Getenv("DRAIN_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			return d
		}
		klog.Warningf("Invalid DRAIN_TIMEOUT %q, using default %v", v, defaultDrainTimeout)
	}
	return defaultDrainTimeout
}

// Run 是 kthena-router 的核心启动入口
// 创建所有组件 → 启动 Informer → 启动 HTTP 监听 → 等待优雅关机
// 整个函数的生命周期对应进程的生命周期
func (s *Server) Run(ctx context.Context) {
	// ===== 第 1 步: 配置 DataStore 可选组件 =====
	// 如果设置了 REDIS_HOST 环境变量,尝试创建 Redis 客户端
	// Redis 用于多副本 router 之间共享在途请求计数 (on-flight counter)
	// 这样多个 router 副本能看到全局的 Pod 负载,做出更好的调度决策
	// 如果 Redis 连接失败,回退到本地内存计数器 (各副本独立,看不到其他副本的流量)
	var storeOpts []datastore.Option
	if os.Getenv("REDIS_HOST") != "" {
		if redisClient := utils.TryGetRedisClient(); redisClient != nil {
			klog.Infof("Redis on-flight counter enabled: cross-router in-flight tracking active")
			// 创建 Redis 后端的在途计数器,注入 DataStore 作为 Option
			storeOpts = append(storeOpts, datastore.WithRedisOnFlightCounter(datastore.NewRedisOnFlightCounter(redisClient)))
		} else {
			klog.Warningf("REDIS_HOST is set but Redis connection failed; falling back to local on-flight counter")
		}
	}

	// ===== 第 2 步: 创建 DataStore =====
	// DataStore 是整个 router 的核心数据存储:
	//   - ModelRoute → ModelServer 映射 (路由表)
	//   - Pod 列表 + IP/端口/状态 (每个 ModelServer 对应的 Pod)
	//   - 限流状态 (令牌桶)
	//   - 指标缓存 (活跃请求数、在途请求数)
	//   - PDGroup 分类 (decode/prefill Pod 按 groupKey 分组)
	store := datastore.New(storeOpts...)
	s.store = store

	// ===== 第 3 步: 创建 Router (必须在 Controller 之前!) =====
	// 原因: NewRouter() 内部会向 DataStore 注册回调函数
	//   - ModelRoute 回调: 限流配置变更 → 更新令牌桶
	//   - Pod 变更回调: 更新分词器 (tokenizer)
	// 如果先启动 Controller,回调还未注册,第一批事件会丢失
	r := NewRouter(store)

	// ===== 第 4 步: 启动所有 Controller =====
	// startControllers() 创建 K8s 客户端 + Informer + Controller 工作循环
	// 返回聚合控制器,后续通过 HasSynced() 判断缓存是否就绪
	s.controllers = startControllers(store, ctx.Done(), s.EnableGatewayAPI, s.Port, s.EnableGatewayAPIInferenceExtension, s.KubeAPIQPS, s.KubeAPIBurst)

	// ===== 第 5 步: 等待所有 Informer 缓存同步完成 =====
	// 必须等待!否则后续 Schedule/Proxy 可能找不到 Pod/ModelServer,导致 404 错误
	// WaitForCacheSync 会阻塞直到所有 HasSynced() 返回 true
	// 如果 stop 通道先关闭 (SIGTERM/SIGINT),则返回 false → Fatal 退出
	if !cache.WaitForCacheSync(ctx.Done(), s.controllers.HasSynced) {
		klog.Fatalf("Failed to sync controllers")
	}
	klog.Infof("Controllers have synced, starting store periodic update loop")

	// ===== 第 6 步: 启动 DataStore 周期更新循环 =====
	// store.Run() 启动后台 goroutine,定期刷新内部状态
	// (如清理过期的 token 追踪记录、更新指标缓存等)
	store.Run(ctx)

	// ===== 第 7 步: 启动 HTTP 监听 =====
	// startRouter() 根据 --enable-gateway-api 选择:
	//   - false: 启动默认 HTTP Server (单端口,带 /healthz /readyz /metrics)
	//   - true:  启动 ListenerManager,按 Gateway CRD 动态管理多端口监听
	s.startRouter(ctx, r, store)

	// ===== 第 8 步: 阻塞等待关机信号 =====
	// ctx 来自 main.go 中的 signal.NotifyContext,当收到 SIGTERM/SIGINT 时 cancel
	// 此处阻塞,保证主 goroutine 不退出;退出后进程结束
	klog.Info("Router server started, waiting for shutdown signal...")
	<-ctx.Done()
	klog.Info("Router server shutting down...")
}

// HasSynced 判断 Server 是否已完成初始化同步
// 两个条件必须同时满足:
//   1. controllers.HasSynced() — 所有 K8s Informer 的本地缓存已同步
//   2. store.HasSynced() — DataStore 内部周期更新循环已完成至少一次
// 该方法被 startListener 中的 /readyz 端点使用,只有 HasSynced()=true 时
// 才会返回 200 OK,否则返回 503 Service Unavailable
func (s *Server) HasSynced() bool {
	return s.controllers.HasSynced() && s.store.HasSynced()
}
