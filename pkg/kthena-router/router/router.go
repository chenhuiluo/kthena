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

package router

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	"github.com/volcano-sh/kthena/pkg/apis/networking/v1alpha1"
	"github.com/volcano-sh/kthena/pkg/kthena-router/accesslog"
	"github.com/volcano-sh/kthena/pkg/kthena-router/common"
	"github.com/volcano-sh/kthena/pkg/kthena-router/connectors"
	"github.com/volcano-sh/kthena/pkg/kthena-router/datastore"
	"github.com/volcano-sh/kthena/pkg/kthena-router/filters/auth"
	"github.com/volcano-sh/kthena/pkg/kthena-router/filters/ratelimit"
	"github.com/volcano-sh/kthena/pkg/kthena-router/filters/tokenizer"
	"github.com/volcano-sh/kthena/pkg/kthena-router/handlers"
	"github.com/volcano-sh/kthena/pkg/kthena-router/metrics"
	"github.com/volcano-sh/kthena/pkg/kthena-router/scheduler"
	"github.com/volcano-sh/kthena/pkg/kthena-router/scheduler/framework"
	"github.com/volcano-sh/kthena/pkg/kthena-router/scheduler/plugins/conf"
	"github.com/volcano-sh/kthena/pkg/kthena-router/utils"
)

// Gin 上下文键定义
const (
	// GatewayKey: 存储网关标识，由 Gateway 监听器设置，用于 HTTPRoute 匹配
	// PromptKey: 存储已解析的 ChatMessage 对象，避免重复解析
	// Context keys for gin context
	GatewayKey = "gatewayKey"
	PromptKey  = "promptKey" // store parsed ChatMessage, which will be reused
)

// getEnvBool 从环境变量读取布尔值，支持默认值回退
func getEnvBool(key string, fallback bool) bool {
	if value, ok := os.LookupEnv(key); ok {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return fallback
}

// EnableFairnessScheduling 启用每模型用户公平排队调度，按用户近期 Token 用量排序
// EnableFairnessScheduling enables the router's per-model user-fairness queue,
// which orders requests by each user's recent token usage. EnableSessionBoost
// enables session-aware boosting to maximize prefix cache reuse. The two are
// mutually exclusive scheduling strategies; enable at most one.
var EnableFairnessScheduling = getEnvBool("ENABLE_FAIRNESS_SCHEDULING", false)
var EnableSessionBoost = getEnvBool("ENABLE_SESSION_BOOST", false)

// Router 是 kthena-router 的请求处理核心结构体
// 持有调度器/限流器/分词器等组件,是 HTTP 请求到 Pod 代理的中枢
type Router struct {
	scheduler       scheduler.Scheduler    // 调度器，执行 Filter -> Score -> TopN 管线
	authenticator   *auth.JWTAuthenticator // JWT 认证器
	store           datastore.Store        // 数据存储，持有所有 Pod/ModelServer/ModelRoute/限流状态/指标
	loadRateLimiter *ratelimit.TokenRateLimiter // 令牌桶限流器（支持单机或全局 Redis 模式）
	accessLogger    accesslog.AccessLogger // 访问日志记录器
	metrics         *metrics.Metrics       // 请求指标收集器
	tokenizer       tokenizer.Tokenizer    // Token 数量估算器

	// KV 连接器工厂，用于 PD 分离场景
	// KV Connector management
	connectorFactory *connectors.Factory

	// 优先队列配置
	// Priority queue configuration
	queueTimeout     time.Duration // 队列等待超时时间
	tokenWeight      float64 // 公平调度策略中 Token 用量的权重（默认 1.0） in the fairness strategy (default 1.0)
	requestNumWeight float64 // 公平调度策略中请求计数权重（默认 0.0） in the fairness strategy (default 0.0)
}

// ActiveRequestCount returns the number of requests currently being handled by the router.
func (r *Router) ActiveRequestCount() int64 {
	return r.metrics.ActiveRequestsCount()
}

// NewRouter 创建新的 Router 实例，初始化限流器、指标收集器、Token估算器、调度器等核心组件
func NewRouter(store datastore.Store, routerConfigPath string) *Router {
	// 公平调度和 SessionBoost 互斥，不能同时启用
	// User fairness and session boost are mutually exclusive scheduling strategies.
	// Enabling both is a configuration error.
	if EnableFairnessScheduling && EnableSessionBoost {
		klog.Fatalf("ENABLE_FAIRNESS_SCHEDULING and ENABLE_SESSION_BOOST are mutually exclusive; enable only one")
	}

	// 创建统一的令牌桶限流器，覆盖所有模型
	// Create a unified rate limiter for all models
	loadRateLimiter := ratelimit.NewTokenRateLimiter()

	// 使用全局单例 Prometheus 指标收集器
	// Use global metrics instance
	metricsInstance := metrics.DefaultMetrics

	// 初始化 Token 估算器 — 默认实现用 len(prompt)/4 兜底
	// Initialize tokenizer
	tokenizerInstance := tokenizer.NewSimpleEstimateTokenizer()

	// 注册 ModelRoute 变更回调，动态更新限流器配置
	// 当 ModelRoute CRD 增删改时, DataStore 会触发此回调
	store.RegisterCallback("ModelRoute", func(data datastore.EventData) {
		switch data.EventType {
		case datastore.EventAdd, datastore.EventUpdate:
			if data.ModelRoute == nil || data.ModelRoute.Spec.RateLimit == nil {
				return
			}
			klog.Infof("add or update rate limit for model %s", data.ModelName)

			// 将限流规则配置到统一限流器
			// Configure the unified rate limiter for this model
			if err := loadRateLimiter.AddOrUpdateLimiter(data.ModelName, data.ModelRoute.Spec.RateLimit); err != nil {
				klog.Errorf("failed to configure rate limiter for model %s: %v", data.ModelName, err)
			}

				// 删除: 清除该模型的令牌桶
		case datastore.EventDelete:
			klog.Infof("delete rate limit for model %s", data.ModelName)
			loadRateLimiter.DeleteLimiter(data.ModelName)
		}
	})

	// 解析调度器配置文件（ConfigMap 挂载到 /etc/config/）
	// 包含: 启用的 Filter/Score 插件列表、各插件的权重和参数
	routerConfig, err := conf.ParseRouterConfig(routerConfigPath)
	if err != nil {
		klog.Fatalf("failed to parse router config: %v", err)
	}

	// 配置访问日志记录器，通过环境变量控制开关/格式/输出目标
	// Initialize access logger with configuration from environment variables
	accessLogConfig := &accesslog.AccessLoggerConfig{
			Enabled: true,  // 默认启用
		Format:  accesslog.FormatText,
		Output:  "stdout",
	}

	// 读取 ACCESS_LOG_ENABLED 环境变量（true/false）
	// Read access log configuration from environment variables
	if enabled := os.Getenv("ACCESS_LOG_ENABLED"); enabled != "" {
		if enabledBool, err := strconv.ParseBool(enabled); err == nil {
			accessLogConfig.Enabled = enabledBool
		}
	}

	// 读取 ACCESS_LOG_FORMAT 环境变量（text/json）
	if format := os.Getenv("ACCESS_LOG_FORMAT"); format != "" {
		if format == "json" {
			accessLogConfig.Format = accesslog.FormatJSON
		} else if format == "text" {
			accessLogConfig.Format = accesslog.FormatText
		}
	}

	// 读取 ACCESS_LOG_OUTPUT 环境变量（stdout/文件路径）
	if output := os.Getenv("ACCESS_LOG_OUTPUT"); output != "" {
		accessLogConfig.Output = output
	}

	accessLogger, err := accesslog.NewAccessLogger(accessLogConfig)
	if err != nil {
		klog.Fatalf("failed to create access logger: %v", err)
	}

	// 构建 Router 实例，填充所有核心组件字段
	return &Router{
		store:            store,
		scheduler:        scheduler.NewScheduler(store, routerConfig),
		authenticator:    auth.NewJWTAuthenticator(routerConfig),
		loadRateLimiter:  loadRateLimiter,
		accessLogger:     accessLogger,
		metrics:          metricsInstance,
		tokenizer:        tokenizerInstance,
		connectorFactory: connectors.NewDefaultFactory(),
		queueTimeout:     parseQueueTimeout(),
		tokenWeight:      parseEnvFloat("FAIRNESS_PRIORITY_TOKEN_WEIGHT", 1.0),
		requestNumWeight: parseEnvFloat("FAIRNESS_PRIORITY_REQUEST_NUM_WEIGHT", 0.0),
	}
}

// 默认公平调度队列超时时间（60秒）
const defaultQueueTimeout = 60 * time.Second

// parseQueueTimeout 从 FAIRNESS_QUEUE_TIMEOUT 环境变量解析队列超时时间
// 支持 Go duration 格式（如 "30s"、"2m"），默认 60 秒
func parseQueueTimeout() time.Duration {
	if s, ok := os.LookupEnv("FAIRNESS_QUEUE_TIMEOUT"); ok {
		if d, err := time.ParseDuration(s); err == nil && d > 0 {
			return d
		}
		klog.Warningf("Invalid FAIRNESS_QUEUE_TIMEOUT %q, using default %v", s, defaultQueueTimeout)
	}
	return defaultQueueTimeout
}

// parseEnvFloat 从环境变量读取浮点数，支持 NaN/Inf/负数校验
// 用于读取公平调度权重参数（FAIRNESS_PRIORITY_TOKEN_WEIGHT 等）
func parseEnvFloat(key string, fallback float64) float64 {
	if s, ok := os.LookupEnv(key); ok {
		if v, err := strconv.ParseFloat(s, 64); err == nil && !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0 {
			return v
		}
		klog.Warningf("Invalid %s %q, using default %v", key, s, fallback)
	}
	return fallback
}

// calculateRequestPriority 计算请求在公平调度队列中的优先级
// 优先级 = tokenWeight × 用户Token用量 + requestNumWeight × 用户请求次数
// 值越低 → 优先级越高 → 越先出队 (最近使用少的用户优先服务)
func (r *Router) calculateRequestPriority(userID, modelName string) float64 {
	// ──────────────────────────────────────────────────────────
	// calculateRequestPriority: 公平调度优先级计算
	//
	// 原理: 用得越多的用户, 优先级越低 → 避免高频用户饿死低频用户
	//
	// 公式 (详见 datastore.CalculateFairnessPriority):
	//   priority = tokenWeight * normalizedTokenUsage
	//           + requestNumWeight * normalizedRequestCount
	//
	// 其中:
	//   normalizedTokenUsage  = 用户近1h的Token用量 / 全局平均用量
	//   normalizedRequestCount = 用户近1h的请求数 / 全局平均请求数
	//   tokenWeight (默认0.7)  + requestNumWeight (默认0.3) = 1.0
	//
	// 返回值含义: 值越小 → 优先级越高 (越先出队)
	//   未认证用户: math.MaxFloat64 (最低优先级)
	//   计算失败: 0 (中等优先级, 不惩罚)
	// ──────────────────────────────────────────────────────────
	priority, err := datastore.CalculateFairnessPriority(r.store, userID, modelName, r.tokenWeight, r.requestNumWeight)
	if err != nil {
		klog.Warningf("failed to calculate fairness priority for user=%s model=%s: %v", userID, modelName, err)
		return 0
	}
	return priority
}

// ModelRequest 是模型推理请求的通用映射类型
type ModelRequest map[string]interface{}

// HandlerFunc 是 kthena-router 的核心入口 — 所有 /v1/ 请求都经过这里
// 主流程: 限流检查 → Token估算 → 分支(直接调度 / 公平排队)
func (r *Router) HandlerFunc() gin.HandlerFunc {
	return func(c *gin.Context) {
		// ── 1. 活跃请求计数 ──────────────────────────────────────
		// 用于优雅关机: drain 时等待此计数归零再关闭连接
		r.metrics.IncActiveRequests()
		defer r.metrics.DecActiveRequests()

		// ── 2. OpenAI 兼容的模型列表端点 ──────────────────────────
		// GET /v1/models → 返回 DataStore 中所有注册的模型名
		// 很多 OpenAI 客户端库初始化时会调用此端点验证连接
		if c.Request.Method == http.MethodGet &&
			(c.Request.URL.Path == "/v1/models" || c.Request.URL.Path == "/models") {
			r.ListModels(c)
			return
		}

		// ── 3. 解析请求体 ──────────────────────────────────────────
		// 从 JSON body 提取 model 字段 + 完整请求 map
		modelRequest, err := ParseModelRequest(c)
		if err != nil {
			accesslog.SetError(c, "request_parsing", err.Error())
			return
		}

		// ── 4. 提取模型名 + 初始化指标追踪 ───────────────────────
		modelName := modelRequest["model"].(string)

		// 访问日志: 记录本次请求的模型名
		accesslog.SetModelName(c, modelName)
		// Gin context: 存储模型名供指标中间件读取
		c.Set("model", modelName)

		// 创建本次请求的指标记录器, 跟踪延迟/Token用量/限流状态
		path := c.Request.URL.Path
		metricsRecorder := metrics.NewRequestMetricsRecorder(r.metrics, modelName, path)

		// 增加下游活跃请求数 (router 视角的"已接受未完成"请求数)
		r.metrics.IncActiveDownstreamRequests(modelName)
		defer func() {
			r.metrics.DecActiveDownstreamRequests(modelName)
			// 请求完成时: 记录最终状态码和结束原因
			if metricsRecorder != nil {
				statusCode := strconv.Itoa(c.Writer.Status())
				reason := "successful_request"
				if r, exists := c.Get("finishReason"); exists {
					reason = r.(string)
				}
				metricsRecorder.Finish(statusCode, reason)
			}
		}()

		// ── 5. 解析 Prompt 文本 ──────────────────────────────────
		// 从请求体的 messages 字段提取用户输入文本
		prompt, err := utils.ParsePrompt(modelRequest)
		if err != nil {
			accesslog.SetError(c, "prompt_parsing", "prompt not found")
			c.AbortWithStatusJSON(http.StatusNotFound, "prompt not found")
			c.Set("finishReason", "prompt_parsing")
			return
		}
		// 缓存到 Gin context, 后续 doLoadbalance() 直接取用, 避免重复解析
		c.Set(PromptKey, prompt)
		promptStr := utils.GetPromptString(prompt)

		// ── 6. 估算输入 Token 数 ─────────────────────────────────
		// SimpleEstimateTokenizer: 基于 len(prompt)/4 的粗略估计
		inputTokens, err := r.tokenizer.CalculateTokenNum(promptStr)
		if err != nil {
			klog.Errorf("failed to calculate token number: %v", err)
			inputTokens = len(promptStr) / 4 // fallback estimation
		}

		// 访问日志: 记录输入 Token 数 (输出 Token 在响应完成时补充)
		accesslog.SetTokenCounts(c, inputTokens, 0)
		// 标记请求处理阶段结束 (区分"排队等待"和"实际处理"时间)
		accesslog.MarkRequestProcessingEnd(c)
		// 立即记录输入 Token 到 Prometheus 指标
		metricsRecorder.RecordInputTokens(inputTokens)

		// ── 7. 令牌桶限流 ─────────────────────────────────────────
		// 三维度检查: input tokens/s, output tokens/s, requests/s
		// 超限返回 429 Too Many Requests
		if err := r.loadRateLimiter.RateLimit(modelName, promptStr); err != nil {
			var errorMsg string
			var errorType string
			var tokenType string
			switch err.(type) {
			case *ratelimit.InputRateLimitExceededError:
				errorMsg = "input token rate limit exceeded"
				errorType = "input_rate_limit"
				tokenType = metrics.LimitTypeInputTokens
			case *ratelimit.OutputRateLimitExceededError:
				errorMsg = "output token rate limit exceeded"
				errorType = "output_rate_limit"
				tokenType = metrics.LimitTypeOutputTokens
			default:
				errorMsg = "token usage exceeds rate limit"
				errorType = "rate_limit"
				tokenType = metrics.LimitTypeRequests
			}
			accesslog.SetError(c, errorType, errorMsg)
			metricsRecorder.RecordRateLimitExceeded(tokenType)
			c.AbortWithStatusJSON(http.StatusTooManyRequests, errorMsg)
			c.Set("finishReason", "rate_limit")
			return
		}

		// ── 8. 生成 Request ID ────────────────────────────────────
		// 如果客户端未提供 x-request-id, 则自动生成一个 UUID
		requestID := uuid.New().String()
		if c.Request.Header.Get("x-request-id") == "" {
			c.Request.Header.Set("x-request-id", requestID)
		}
		// Gin context: 存储指标记录器供 proxyModelEndpoint/proxy 等函数使用
		c.Set("metricsRecorder", metricsRecorder)

		// ── 9. 分支: 直接调度 vs 排队调度 ────────────────────────
		// 9a. 无 Fairness/SessionBoost → 最快路径: 限流通过后立即调度
		if !EnableFairnessScheduling && !EnableSessionBoost {
			_ = r.doLoadbalance(c, modelRequest)
			return
		}
		// 9b. 有 Fairness/SessionBoost → 先入优先队列排队, 出队后调度
		// 超时/客户端断连 → 请求被取消
		if err := r.handleFairnessScheduling(c, modelRequest, requestID, modelName); err != nil {
			accesslog.SetError(c, "scheduling", err.Error())
			c.Set("finishReason", "scheduling")
			return
		}
	}
}

// doLoadbalance 是负载均衡的核心函数 — 从路由匹配到调度决策到代理转发
// 主流程: 模型匹配 → 取Pod列表 → 构建调度上下文 → 调度打分 → 代理转发
//   同构/PD聚合: scheduler 返回 BestPods → proxyModelEndpoint() → proxy()
//   PD 分离:     scheduler 返回 DecodePods+PrefillPods → proxyToPDDisaggregated()
func (r *Router) doLoadbalance(c *gin.Context, modelRequest ModelRequest) error {
	modelName := modelRequest["model"].(string)

	// ── 1. 声明本函数的输出变量 ──────────────────────────────
	var pods []*datastore.PodInfo     // 候选 Pod 列表
	var port int32                     // ModelServer 的工作端口
	var modelServerName types.NamespacedName // ModelServer CR 的命名空间/名称
	var modelRoute *v1alpha1.ModelRoute     // 匹配到的 ModelRoute CR
	var modelServer *v1alpha1.ModelServer   // ModelServer CR 对象

	// ── 2. 获取 Gateway 标识 (Gateway API 模式) ──────────────
	// 由 ListenerManager 的中间件注入, 值为 "namespace/name"
	// 用于 Gateway 作用域过滤: 只有属于此 Gateway 的 ModelRoute 才参与匹配
	var gatewayKey string
	if key, exists := c.Get(GatewayKey); exists {
		if k, ok := key.(string); ok {
			gatewayKey = k
		}
	}
	if gatewayKey != "" {
		accesslog.SetGatewayAPIInfo(c, gatewayKey, "", "")
	}

	// ── 3. 路由匹配: model 名 → ModelServer ──────────────────
	// MatchModelServer 内部逻辑:
	//   a. 精确匹配 routes[model] → ModelServer (基础模型路由)
	//   b. LoRA 适配器匹配 loraRoutes[model] (LoRA 路由)
	//   c. Gateway 作用域过滤 (parentRefs 匹配)
	//   d. 规则匹配 selectRule() (按 Body/Header/Query 条件)
	//   e. 加权随机 selectDestination() (多目标按权重分配)
	var isLora bool
	var err error
	modelServerName, isLora, modelRoute, err = r.store.MatchModelServer(modelName, c.Request, gatewayKey)
	if err != nil {
		// 匹配失败不立即返回, 先记日志, 后面还有 HTTPRoute 兜底
		accesslog.SetError(c, "model_server_matching", fmt.Sprintf("can't find corresponding model server: %v", err))
	}

	// ── 4. 根据匹配结果走不同分支取 Pod 和端口 ─────────────
	if err == nil && strings.HasPrefix(c.Request.URL.Path, "/v1/") {
		// ── 4a. ModelServer 路径: /v1/ 请求且 ModelRoute 匹配成功 ──
		// 获取该 ModelServer 下所有 Ready 的 Pod + ModelServer CR
		pods, modelServer, err = r.getPodsAndServer(modelServerName)
		if err != nil || len(pods) == 0 {
			klog.Errorf("failed to get pods and model server: %v, %v", modelServerName, err)
			accesslog.SetError(c, "pod_discovery", fmt.Sprintf("can't find model server: %v", modelServerName))
			c.AbortWithStatusJSON(http.StatusNotFound, fmt.Sprintf("can't find model server: %v", modelServerName))
			return fmt.Errorf("can't find model server: %v", modelServerName)
		}

		// 如果不是 LoRA 请求,将 model 字段替换为 ModelServer CR 中的基础模型名
		// 原因: 推理引擎 (vLLM/SGLang) 只认识自己加载的模型名,不认识路由别名
		model := modelServer.Spec.Model
		if model != nil && !isLora {
			modelRequest["model"] = *model
		}

		// 记录推理引擎的工作端口 (Pod IP + 此端口 = 引擎实际监听地址)
		port = modelServer.Spec.WorkloadPort.Port
	} else if matched, inferencePoolName := r.handleHTTPRoute(c, gatewayKey); matched {
		// ── 4b. HTTPRoute 路径: ModelRoute 未匹配,但 HTTPRoute 匹配 ──
		// 这是 Gateway API Inference Extension 的路径
		// 通过 InferencePool 获取 Pod 列表和端口

		inferencePoolKey := fmt.Sprintf("%s/%s", inferencePoolName.Namespace, inferencePoolName.Name)
		inferencePool := r.store.GetInferencePool(inferencePoolKey)
		if inferencePool == nil {
			klog.Errorf("failed to get inference pool: %v", inferencePoolName)
			accesslog.SetError(c, "inference_pool_discovery", fmt.Sprintf("can't find inference pool: %v", inferencePoolName))
			c.AbortWithStatusJSON(http.StatusNotFound, fmt.Sprintf("can't find inference pool: %v", inferencePoolName))
			return fmt.Errorf("can't find inference pool: %v", inferencePoolName)
		}

		// 从 InferencePool 获取关联的 Pod 列表
		pods, err = r.store.GetPodsByInferencePool(inferencePoolName)
		if err != nil || len(pods) == 0 {
			klog.Errorf("failed to get pods for inference pool: %v, %v", inferencePoolName, err)
			accesslog.SetError(c, "pod_discovery", fmt.Sprintf("can't find pods for inference pool: %v", inferencePoolName))
			c.AbortWithStatusJSON(http.StatusNotFound, fmt.Sprintf("can't find pods for inference pool: %v", inferencePoolName))
			return fmt.Errorf("can't find pods for inference pool: %v", inferencePoolName)
		}

		// 从 InferencePool.Spec.TargetPorts 获取目标端口
		if len(inferencePool.Spec.TargetPorts) == 0 {
			klog.Errorf("inference pool %v has no target ports", inferencePoolName)
			accesslog.SetError(c, "port_discovery", fmt.Sprintf("inference pool %v has no target ports", inferencePoolName))
			c.AbortWithStatusJSON(http.StatusBadRequest, fmt.Sprintf("inference pool %v has no target ports", inferencePoolName))
			return fmt.Errorf("inference pool %v has no target ports", inferencePoolName)
		}
		port = int32(inferencePool.Spec.TargetPorts[0].Number)

		klog.V(4).Infof("InferencePool is %v, pods count: %d, port: %d", inferencePoolName, len(pods), port)
	} else {
		// ── 4c. 两条路都没匹配 → 404 ──
		accesslog.SetError(c, "route_not_found", "route not found")
		c.AbortWithStatusJSON(http.StatusNotFound, "route not found")
		return fmt.Errorf("route not found")
	}

	// ── 5. 构建 framework.Context — 调度管线在插件间传递的上下文 ──
	// 从 Gin context 取出 HandlerFunc 中缓存的 prompt
	var prompt *common.ChatMessage
	if cached, exists := c.Get(PromptKey); exists {
		var ok bool
		if prompt, ok = cached.(*common.ChatMessage); !ok {
			accesslog.SetError(c, "prompt_parsing", "internal error: invalid prompt type")
			c.AbortWithStatusJSON(http.StatusInternalServerError, "internal error")
			return fmt.Errorf("invalid prompt type")
		}
	} else {
		accesslog.SetError(c, "prompt_parsing", "prompt not found")
		c.AbortWithStatusJSON(http.StatusNotFound, "prompt not found")
		return fmt.Errorf("prompt not found")
	}

	// 从 Gin context 取出 HandlerFunc 中创建的指标记录器
	var metricsRecorder *metrics.RequestMetricsRecorder
	if recorder, exists := c.Get("metricsRecorder"); exists {
		if rec, ok := recorder.(*metrics.RequestMetricsRecorder); ok {
			metricsRecorder = rec
		}
	}

	// 提取 PDGroup (仅 ModelServer 请求才有)
	// PDGroup 是 ModelServer.Spec.WorkloadSelector.PDGroup 字段
	// 如果存在, 说明是 PD 分离部署, scheduler 会分别调度 decode/prefill Pod
	var pdGroup *v1alpha1.PDGroup
	if modelServer != nil && modelServer.Spec.WorkloadSelector != nil {
		pdGroup = modelServer.Spec.WorkloadSelector.PDGroup
	}

	// 提取 session ID (用于 Session Boost: 相同 session 优先到同一 Pod)
	sessionHeader := r.store.GetSessionIDHeader()
	var sessionID string
	if sessionHeader != "" {
		sessionID = c.Request.Header.Get(sessionHeader)
	}

	// 组装调度上下文 — Schedule() 会填充 BestPods 或 DecodePods+PrefillPods
	ctx := &framework.Context{
		Model:           modelName,
		Prompt:          prompt,
		SessionID:       sessionID,
		ModelServerName: modelServerName,
		PDGroup:         pdGroup,
		MetricsRecorder: metricsRecorder,
	}

	// ── 6. 执行调度: Filter → Score → TopN ──────────────────
	// 同构: 填充 ctx.BestPods (Top5 Pod)
	// PD分离: 填充 ctx.DecodePods (Top5) + ctx.PrefillPods (每个 Decode 配 1 个 Prefill)
	err = r.scheduler.Schedule(ctx, pods)
	if err != nil {
		accesslog.SetError(c, "scheduling", fmt.Sprintf("can't schedule to target pod: %v", err))
		c.AbortWithStatusJSON(http.StatusBadRequest, fmt.Sprintf("can't schedule to target pod: %v", err))
		return fmt.Errorf("can't schedule to target pod: %v", err)
	}

	// ── 7. 记录访问日志的路由信息 ─────────────────────────────
	// ModelRoute → ModelServer → 选中 Pod, 完整链路
	modelServerFullName := fmt.Sprintf("%s/%s", modelServerName.Namespace, modelServerName.Name)
	modelRouteName := ""
	if modelRoute != nil {
		modelRouteName = fmt.Sprintf("%s/%s", modelRoute.Namespace, modelRoute.Name)
		// 供上游连接中间件使用
		c.Set("modelRouteName", modelRouteName)
	}

	if len(ctx.BestPods) > 0 {
		selectedPod := ctx.BestPods[0].GetPodNamespacedName().Name
		accesslog.SetRequestRouting(c, modelRouteName, modelServerFullName, selectedPod)
	} else {
		// PD 分离场景 — 此时还没有选定 decode Pod
		accesslog.SetRequestRouting(c, modelRouteName, modelServerFullName, "")
	}

	// ── 8. 代理请求到选中的 Pod ──────────────────────────────
	req := c.Request
	if err := r.proxyModelEndpoint(c, req, ctx, modelRequest, port); err != nil {
		klog.Errorf("request failed reqID: %s: %v", c.Request.Header.Get("x-request-id"), err)
		accesslog.SetError(c, "proxy", "request processing failed")
		c.AbortWithStatusJSON(http.StatusInternalServerError, "request processing failed")
		return err
	}
	return nil
}

// ParseModelRequest 从 Gin context 解析 HTTP 请求体为 ModelRequest (map[string]interface{})
// 步骤: 读取 body → JSON 反序列化 → 校验 model 字段非空
// 如果解析失败, 直接写回 400/500 错误响应并终止请求
func ParseModelRequest(c *gin.Context) (ModelRequest, error) {
	bodyBytes, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, err)
		return nil, err
	}
	var modelRequest ModelRequest
	if err := json.Unmarshal(bodyBytes, &modelRequest); err != nil {
		c.AbortWithStatusJSON(http.StatusBadRequest, err)
		return nil, err
	}

	modelName, ok := modelRequest["model"].(string)
	if !ok || strings.TrimSpace(modelName) == "" {
		c.AbortWithStatusJSON(http.StatusNotFound, "model not found")
		return nil, fmt.Errorf("model not found")
	}
	klog.V(4).Infof("model name is %v", modelName)

	return modelRequest, nil
}

// getPodsAndServer 根据 ModelServer 的 NamespacedName 从 DataStore 获取:
//   1. 该 ModelServer 下所有 Ready 的 Pod 列表 (PodInfo 含 IP/端口/指标/在途计数等)
//   2. ModelServer CR 对象 (含模型名/端口/PDGroup 等配置)
func (r *Router) getPodsAndServer(modelServerName types.NamespacedName) ([]*datastore.PodInfo, *v1alpha1.ModelServer, error) {
	pods, err := r.store.GetPodsByModelServer(modelServerName)
	if err != nil || len(pods) == 0 {
		return nil, nil, fmt.Errorf("can't find target pods of model server: %v, err: %v", modelServerName, err)
	}
	modelServer := r.store.GetModelServer(modelServerName)
	if modelServer == nil {
		return nil, nil, fmt.Errorf("can't find model server: %v", modelServerName)
	}
	return pods, modelServer, nil
}

// handleHTTPRoute 处理非 /v1/ 路径的 HTTPRoute 匹配，返回是否匹配及 InferencePool 名称
// handleHTTPRoute 处理 Gateway API 的 HTTPRoute 匹配 (非 /v1/ 路径的请求)
// 返回值:
//   - matched: true = HTTPRoute 匹配成功, 请求应路由到 InferencePool
//   - inferencePoolName: 匹配到的 InferencePool 的 NamespacedName
func (r *Router) handleHTTPRoute(c *gin.Context, gatewayKey string) (bool, types.NamespacedName) {
	matchResult, matched := r.findHTTPRouteMatch(c, gatewayKey)
	if !matched {
		return false, types.NamespacedName{}
	}

	// Record Gateway API match into access log (gatewayKey is already "namespace/name").
	httpRouteKey := fmt.Sprintf("%s/%s", matchResult.route.Namespace, matchResult.route.Name)
	accesslog.SetGatewayAPIInfo(c, gatewayKey, httpRouteKey, "")

	// Store the matched prefix in context for URL rewriting
	if matchResult.matchedPrefix != "" {
		c.Set("matchedPrefix", matchResult.matchedPrefix)
	}

	inferencePoolName, found := inferencePoolFromHTTPRouteRule(matchResult.route, matchResult.rule)
	if !found {
		return false, types.NamespacedName{}
	}

	// Record InferencePool match into access log.
	inferencePoolKey := fmt.Sprintf("%s/%s", inferencePoolName.Namespace, inferencePoolName.Name)
	accesslog.SetGatewayAPIInfo(c, "", "", inferencePoolKey)

	// Apply HTTPURLRewriteFilter from the same rule that matched the request.
	if matchResult.rule.Filters != nil {
		for _, filter := range matchResult.rule.Filters {
			if filter.Type == gatewayv1.HTTPRouteFilterURLRewrite && filter.URLRewrite != nil {
				r.applyURLRewrite(c, filter.URLRewrite)
			}
		}
	}

	return true, inferencePoolName
}

// applyURLRewrite 应用 Gateway API 的 HTTPURLRewriteFilter 规则
// applyURLRewrite 应用 HTTPURLRewriteFilter 到请求
// 支持 Gateway API 规范中的两种路径重写:
//   - ReplaceFullPath: 替换完整路径 (如 /api/v2/chat → /v1/chat)
//   - ReplacePrefixMatch: 只替换匹配的前缀部分
// 还支持 Hostname 重写 (替换 HTTP Host 头)
func (r *Router) applyURLRewrite(c *gin.Context, urlRewrite *gatewayv1.HTTPURLRewriteFilter) {
	// Apply hostname rewrite
	if urlRewrite.Hostname != nil {
		newHostname := string(*urlRewrite.Hostname)
		c.Request.Host = newHostname
		klog.V(4).Infof("Rewrote hostname to: %s", newHostname)
	}

	// Apply path rewrite
	if urlRewrite.Path != nil {
		originalPath := c.Request.URL.Path
		newPath := originalPath

		switch urlRewrite.Path.Type {
		case gatewayv1.FullPathHTTPPathModifier:
			// Replace the full path
			if urlRewrite.Path.ReplaceFullPath != nil {
				newPath = *urlRewrite.Path.ReplaceFullPath
				klog.V(4).Infof("Rewrote full path from %s to %s", originalPath, newPath)
			}

		case gatewayv1.PrefixMatchHTTPPathModifier:
			// Replace the matched prefix with the specified replacement
			if urlRewrite.Path.ReplacePrefixMatch != nil {
				// Get the matched prefix from context
				prefix, exists := c.Get("matchedPrefix")
				if !exists {
					klog.Errorf("matchedPrefix not found in context for path rewrite")
					break
				}
				matchedPrefix, ok := prefix.(string)
				if !ok || matchedPrefix == "" {
					klog.Errorf("matchedPrefix is not a valid string in context")
					break
				}
				// Replace the matched prefix
				replacement := *urlRewrite.Path.ReplacePrefixMatch
				newPath = replacement + strings.TrimPrefix(originalPath, matchedPrefix)
				klog.V(4).Infof("Rewrote path prefix from %s to %s (matched prefix: %s)", originalPath, newPath, matchedPrefix)
			}
		}

		// Update the request path
		c.Request.URL.Path = newPath
		// Also update the raw path to maintain consistency
		c.Request.URL.RawPath = ""
	}
}

// proxy 遍历 BestPods (Top5) 逐个尝试代理，失败自动换下一个 Pod 重试
//
// 关键实现:
//   - body 只能读一次: RoundTrip 会消耗 req.Body，因此先缓存 bodyBytes，每次重试前重建 Body
//   - 在途请求计数 (on-flight): 代理前 Incr，完成后 Decr，供 least-request 插件实时打分
//   - 成功后调用 RunPostHooks (如 prefix-cache 插件写缓存)
//   - 如果响应已部分写入 (c.Writer.Written())，不能重试，直接返回错误
func (r *Router) proxy(
	c *gin.Context,
	req *http.Request,
	ctx *framework.Context,
	stream bool,
	port int32,
	onUsage func(u handlers.OpenAIResponse),
) error {
	modelServerName := fmt.Sprintf("%s/%s", ctx.ModelServerName.Namespace, ctx.ModelServerName.Name)

	// Get model route name from context
	var modelRouteName string
	if routeName, exists := c.Get("modelRouteName"); exists {
		if name, ok := routeName.(string); ok {
			modelRouteName = name
		}
	}

	// Capture body bytes once so each retry attempt gets a fresh reader.
	// transport.RoundTrip drains req.Body on every call, so reusing the same
	// request across loop iterations sends an empty body to subsequent pods.
	var bodyBytes []byte
	if req.Body != nil {
		b, err := io.ReadAll(req.Body)
		req.Body.Close()
		if err != nil {
			return fmt.Errorf("failed to read request body: %w", err)
		}
		bodyBytes = b
	}

	for i := 0; i < len(ctx.BestPods); i++ {
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
		pod := ctx.BestPods[i]
		podObj := pod.GetPod()
		podName := types.NamespacedName{Namespace: podObj.Namespace, Name: podObj.Name}

		// Track this request as in-flight to the chosen pod.
		r.store.IncrPodOnFlightRequests(podName)

		// Increment upstream request count with both modelServer and modelRoute
		r.metrics.IncActiveUpstreamRequests(modelServerName, modelRouteName)

		// Request dispatched to the pod.
		err := proxyRequest(c, req, podObj.Status.PodIP, port, stream, onUsage)

		// Decrement upstream request count when request completes
		r.metrics.DecActiveUpstreamRequests(modelServerName, modelRouteName)

		// Request is complete (success or failure) — decrement on-flight counter.
		r.store.DecrPodOnFlightRequests(podName)

		if err != nil {
			klog.Errorf(" pod request error: %v", err)
			if c.Writer.Written() {
				return err
			}
			continue
		}
		// record in prefix cache
		r.scheduler.RunPostHooks(ctx, i)
		return nil
	}
	c.AbortWithStatusJSON(http.StatusNotFound, "request to all pods failed")
	return fmt.Errorf("request to all pods failed")
}

// proxyModelEndpoint 请求代理的入口函数,根据调度结果选择代理路径:
//   - ctx.BestPods 非空 (同构/PD聚合) → proxy() — 单阶段代理
//   - ctx.BestPods 为空 (PD分离) → proxyToPDDisaggregated() — 两阶段代理
//
// 该函数还负责:
//   - 构建 decode 请求 (connectors.BuildDecodeRequest: 修正 model 字段, 将路由别名替换为基础模型名)
//   - 解析流式/非流式响应中的 Token Usage (用于限流计数和公平调度指标)
//   - 更新用户的 Token 使用量 (UpdateTokenCount: 影响下次公平调度的优先级)
func (r *Router) proxyModelEndpoint(
	c *gin.Context,
	req *http.Request,
	ctx *framework.Context,
	modelRequest ModelRequest,
	port int32,
) error {
	// ──────────────────────────────────────────────────────────
	// proxyModelEndpoint: 请求代理的分流枢纽
	//
	// 根据 Schedule() 的结果, 将请求分流到两条完全不同的代理路径:
	//   - ctx.BestPods != nil → 同构/PD聚合模式 (单阶段代理)
	//     调度器已选出 TopN 候选 Pod, 逐个尝试直到成功;
	//     这些 Pod 同时承担 prefill + decode, 无需跨 Pod 传输 KV Cache.
	//   - ctx.BestPods == nil → PD分离模式 (两阶段代理)
	//     调度器已选出 (DecodePods[], PrefillPods[]) 配对,
	//     需要由 KV Connector 协调 prefill→decode 的 KV Cache 传输.
	//
	// 本函数还承担三个横切关注点:
	//   1. 构建解码请求: BuildDecodeRequest 将路由别名(如 "gpt-4")替换为基础模型名,
	//      因为后端推理引擎只认模型文件名, 不认路由别名.
	//   2. 解析 Token Usage: 从流式/非流式响应中提取 CompletionTokens,
	//      用于限流计数 (loadRateLimiter) 和公平调度指标 (UpdateTokenCount).
	//   3. 更新用户配额: UpdateTokenCount 写入滑动窗口计数器,
	//      影响该用户下次请求在 handleFairnessScheduling 中的优先级.
	// ──────────────────────────────────────────────────────────

	// [步骤1] 标记上游处理开始时间, 用于 access log 计算上游耗时 (TTFB 等)
	accesslog.MarkUpstreamStart(c)

	// [步骤2] 从 gin.Context 中提取指标记录器 (由中间件注入)
	// 后续在解析 Token Usage 时, 将输出 token 数写入 Prometheus 指标
	var metricsRecorder *metrics.RequestMetricsRecorder
	if recorder, exists := c.Get("metricsRecorder"); exists {
		if rec, ok := recorder.(*metrics.RequestMetricsRecorder); ok {
			metricsRecorder = rec
		}
	}

	// ═══════════ 分支 A: 同构/PD聚合模式 (ctx.BestPods != nil) ═══════════
	// 调度器返回了 TopN 候选 Pod, 每个都同时运行 prefill + decode,
	// 无需跨 Pod 传输 KV Cache, 代理逻辑最简单.
	if ctx.BestPods != nil {
		// [A-1] 构建解码请求
		// BuildDecodeRequest 的核心作用: 将请求体中的 model 字段从路由别名
		// 替换为基础模型名. 例如用户请求 model="gpt-4o-mini",
		// 但后端 vLLM 只认 "Qwen2.5-72B-Instruct", 不做替换会 404.
		decodeRequest := connectors.BuildDecodeRequest(c, req, modelRequest)

		// [A-2] 判断是否为流式请求, 决定 proxy 内部的响应转发方式
		// 流式: 逐行转发 SSE 事件; 非流式: TeeReader 边转发边缓冲解析
		stream := isStreaming(modelRequest)
		modelName := ctx.Model
		userID := c.GetString(common.UserIdKey)

		// [A-3] 调用 proxy 执行实际转发
		// proxy 内部对 BestPods 逐个尝试, 直到某个 Pod 返回成功响应.
		// onUsage 回调: 在解析到 Token Usage 时触发, 处理三个横切关注点.
		err := r.proxy(c, decodeRequest, ctx, stream, port, func(resp handlers.OpenAIResponse) {
			// onUsage 回调 — 仅在解析到有效 Token Usage 时触发
			// (流式: 从最后一个 SSE chunk 解析; 非流式: 从完整 JSON 响应解析)
			if resp.Usage.TotalTokens <= 0 {
				return
			}

			// [横切1] 限流计数: 将输出 token 记入滑动窗口,
			// 当窗口内 token 数超限时, 后续请求会被 handleFairnessScheduling 排队或降权.
			// 这是"用得多的用户优先级低"公平调度的数据来源.
			if r.loadRateLimiter != nil {
				r.loadRateLimiter.RecordOutputTokens(modelName, resp.Usage.CompletionTokens)
			}

			// [横切2] 访问日志: 将输出 token 数写入 access log context,
			// 便于事后分析每个请求的 token 消耗分布.
			if accessCtx := accesslog.GetAccessLogContext(c); accessCtx != nil {
				accessCtx.SetTokenCounts(accessCtx.InputTokens, resp.Usage.CompletionTokens)
			}

			// [横切3] Prometheus 指标: 将输出 token 数记录到指标,
			// 用于 Grafana 面板展示全局/每模型 token 吞吐量.
			if metricsRecorder != nil {
				metricsRecorder.RecordOutputTokens(resp.Usage.CompletionTokens)
			}

			// [横切4] 用户配额更新: 将本次请求的 prompt/completion token 数
			// 累加到该用户+模型的滑动窗口计数器 (默认 1h 窗口).
			// 这直接影响该用户下次请求在 calculateRequestPriority 中的优先级得分:
			//   窗口内用量越高 → 优先级越低 → 越可能被排队.
			if userID == "" || modelName == "" {
				return
			}
			_ = r.store.UpdateTokenCount(userID, modelName, float64(resp.Usage.PromptTokens), float64(resp.Usage.CompletionTokens))
		})

		// [A-4] 标记上游处理结束, 用于 access log 计算总上游耗时
		accesslog.MarkUpstreamEnd(c)
		return err
	}

	// ═══════════ 分支 B: PD分离模式 (ctx.BestPods == nil) ═══════════
	// 调度器返回了 (DecodePods, PrefillPods) 配对列表,
	// 需要由 KV Connector 协调 prefill→decode 的 KV Cache 传输.
	// 详见 proxyToPDDisaggregated 的注释.

	// [B-1] 获取该 ModelServer 对应的 KV Connector 实例
	// 不同模型服务可能使用不同的 KV 传输实现 (HTTP/LMCache/MoonCake+NIXL/eBPF).
	// getKVConnector 根据 ModelServer 的 annotation 选择 connector 类型.
	kvConnector, err := r.getKVConnector(ctx.ModelServerName)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusInternalServerError, fmt.Sprintf("failed to get KV connector: %v", err))
		return fmt.Errorf("failed to get KV connector: %w", err)
	}

	// [B-2] 进入 PD分离代理流程
	// proxyToPDDisgregated 负责:
	//   1. 向选中的 PrefillPod 发送 prefill 请求 (含 prompt)
	//   2. PrefillPod 完成计算后, 通过 KV Connector 将 KV Cache 传给 DecodePod
	//   3. 向选中的 DecodePod 发送 decode 请求 (接收 KV Cache, 逐 token 生成)
	// 如果第一对 (decode, prefill) 失败, 依次 fallback 到后续配对.
	return r.proxyToPDDisaggregated(c, req, ctx, kvConnector, modelRequest, port)
}

// GetModelServer 根据模型名查找对应的 ModelServer CR 对象
// 对外暴露的工具方法, 用于 debug 端点或内部组件查询
func (r *Router) GetModelServer(modelName string, req *http.Request) (*v1alpha1.ModelServer, error) {
	modelServerName, isLora, _, err := r.store.MatchModelServer(modelName, req, "")
	if err != nil {
		return nil, fmt.Errorf("can't find corresponding model server: %v", err)
	}
	klog.V(4).Infof("modelServer is %v, is_lora: %v", modelServerName, isLora)

	pods, modelServer, err := r.getPodsAndServer(modelServerName)
	if err != nil || len(pods) == 0 {
		klog.Errorf("failed to get pods and model server: %v, %v", modelServerName, err)
		return nil, fmt.Errorf("can't find model server: %v", modelServerName)
	}

	return modelServer, nil
}

// modelObject 是 OpenAI API 兼容的模型信息对象
type modelObject struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

// modelsResponse 是 OpenAI API 兼容的模型列表响应
type modelsResponse struct {
	Object string        `json:"object"`
	Data   []modelObject `json:"data"`
}

// ListModels 实现 OpenAI 兼容的 GET /v1/models 接口，返回所有通过 ModelRoute 注册的模型名称
// ListModels 实现 OpenAI 兼容的 GET /v1/models 端点
// 返回 DataStore 中通过 ModelRoute 注册的所有模型名
// 很多 OpenAI 客户端库初始化时会调用此端点验证连接
func (r *Router) ListModels(c *gin.Context) {
	modelNames := r.store.GetModelNames()

	data := make([]modelObject, 0, len(modelNames))
	for _, name := range modelNames {
		data = append(data, modelObject{
			ID:      name,      // 模型名 (来自 ModelRoute.Spec.ModelName)
			Object:  "model",   // OpenAI 规范固定值
			Created: 0,         // kthena 未跟踪创建时间
			OwnedBy: "kthena",  // 归属方标识
		})
	}

	c.JSON(http.StatusOK, modelsResponse{
		Object: "list", // OpenAI 规范固定值
		Data:   data,
	})
}

// Auth 返回 JWT 认证中间件 — 仅对 /v1/ 路径生效
func (r *Router) Auth() gin.HandlerFunc {
	return r.authenticator.Authenticate()
}

// AccessLog 返回访问日志中间件 — 仅对 /v1/ 路径生效
func (r *Router) AccessLog() gin.HandlerFunc {
	return accesslog.AccessLogMiddleware(r.accessLogger)
}

// proxyRequest 底层 HTTP 代理函数,将请求转发到目标 Pod 并回传响应
//
// 流式响应处理:
//   逐行读取 SSE 事件 (data: ...), 尝试从最后一个 chunk 解析 usage 中的 CompletionTokens
//   如果 > 0, 触发 onUsage 回调记录输出 Token (用于限流计数)
//   使用 Gin 的 Stream 方法逐行转发, 不缓冲整个响应
//
// 非流式响应处理:
//   使用 TeeReader 同时写入客户端和内部 buffer
//   响应完成后从 buffer 解析完整 JSON 提取 Usage
func proxyRequest(
	c *gin.Context,
	req *http.Request,
	podIP string,
	port int32,
	stream bool,
	onUsage func(u handlers.OpenAIResponse),
) error {
	resp, err := doRequest(req, podIP, port)
	if err != nil {
		return fmt.Errorf("decode request error: %w", err)
	}
	for k, vv := range resp.Header {
		for _, v := range vv {
			c.Header(k, v)
		}
	}
	defer resp.Body.Close()

	c.Status(resp.StatusCode)

	if stream {
		// If the request is a streaming request, we need to stream the response body.
		// Stream response: read and forward each event (line) one by one, and parse usage if present
		c.Status(resp.StatusCode)
		reader := bufio.NewReader(resp.Body)
		var streamErr error
		c.Stream(func(w io.Writer) bool {
			line, err := reader.ReadBytes('\n')
			if len(line) > 0 {
				// Try to parse usage from this line, assuming it's a data line
				parsed := handlers.ParseStreamRespForUsage(string(line))
				if parsed.Usage.CompletionTokens > 0 {
					klog.V(4).Infof("Parsed usage: %+v", parsed.Usage)

					// Always call onUsage callback to record output tokens
					if onUsage != nil {
						onUsage(parsed)
					}

					// The token usage is set by router, so remove it before sending to downstream
					if v, ok := c.Get(common.TokenUsageKey); ok && v.(bool) {
						return true
					}
				}
				// Forward to downstream
				_, _ = w.Write(line)
			}
			if err != nil {
				if err != io.EOF {
					klog.Errorf("error reading stream body: %v", err)
					streamErr = err
				}
				return false
			}
			return true
		})
		return streamErr
	} else {
		// Non-stream: efficiently stream response while capturing for parsing
		var buf bytes.Buffer
		ttee := io.TeeReader(resp.Body, &buf)

		_, err := io.Copy(c.Writer, ttee)
		if err != nil {
			klog.Errorf("copy response to downstream failed: %v", err)
			return err
		}

		// Parse usage if present
		parsed, _ := handlers.ParseOpenAIResponseBody(buf.Bytes())
		if parsed != nil && parsed.Usage.CompletionTokens > 0 {
			klog.V(4).Infof("Parsed usage: %+v", parsed.Usage)
			if onUsage != nil {
				onUsage(*parsed)
			}
		}
	}

	return nil
}

// doRequest 使用 HTTP Transport 发送请求到目标 Pod
// 步骤: 1. 将 URL Host 替换为 podIP:Port  2. RoundTrip 发送请求  3. 非 2xx 视为错误
func doRequest(
	req *http.Request,
	podIP string,
	port int32,
) (*http.Response, error) {
	// step 1: change request URL to prefill pod URL.
	req.URL.Host = net.JoinHostPort(podIP, strconv.Itoa(int(port)))

	// step 2: use http.Transport to do request to prefill pod.
	transport := http.DefaultTransport
	resp, err := transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		resp.Body.Close()
		return nil, fmt.Errorf("http resp error, http code is %d", resp.StatusCode)
	}
	return resp, nil
}

// isStreaming 检查请求是否启用 SSE 流式输出
// OpenAI 格式: {"stream": true} → 引擎逐 Token 推送 chunk, 前端实时渲染
func isStreaming(modelRequest ModelRequest) bool {
	if v, ok := modelRequest["stream"]; ok {
		if stream, isBool := v.(bool); isBool && stream {
			return true
		}
	}
	return false
}

// getKVConnector 获取 ModelServer 对应的 KV 连接器 (仅 PD 分离场景使用)
//
// KV 连接器负责协调 prefill 和 decode 两阶段:
//   1. 将 prompt 发送给 prefill Pod, 生成 KV Cache
//   2. 将 KV Cache 传输给 decode Pod, 逐 Token 生成输出
//
// 类型选择优先级:
//   1. 显式指定: ModelServer.Spec.KVConnector.Type (最高优先)
//   2. 引擎推断: InferenceEngine == SGLang → ZMQ bootstrap 协议
//   3. 默认回退: HTTP (通用 NIXL/MoonCake 传输层)
func (r *Router) getKVConnector(modelServerName types.NamespacedName) (connectors.KVConnector, error) {
	// ──────────────────────────────────────────────────────────
	// getKVConnector: 获取 PD 分离场景的 KV Cache 传输连接器
	//
	// 背景: PD 分离模式下, prefill Pod 计算完 KV Cache 后,
	// 需要将 KV Cache 传输到 decode Pod. 不同推理引擎/硬件
	// 使用不同的传输协议, 因此需要通过此函数选择对应的
	// KVConnector 实现.
	//
	// 选择策略 (优先级从高到低):
	//   1. 用户显式指定: ModelServer.Spec.KVConnector.Type
	//      (如 "nixl", "lmcache", "mooncake", "http")
	//   2. 推理引擎推断: 如果引擎是 SGLang → 自动选 SGLang connector
	//      (SGLang 使用 ZMQ bootstrap 协议, 要求 prefill/decode 并发)
	//   3. 兜底默认: HTTP connector
	//      (最简单的 prefill→decode 顺序调用, 无需特殊硬件支持)
	//
	// 5 种 Connector 差异:
	//   HTTP:    prefill 顺序完成后, 通过 HTTP 传 KV Cache metadata, decode 再拉取
	//   LMCache: 复用 HTTP connector 逻辑, 但使用 LMCache 库管理 KV Cache 存储层
	//   MoonCake:使用 MoonCake 传输层 + NIXL 引擎, RDMA 高速传输
	//   NIXL:   使用 NIXL 引擎直接 RDMA 传输 KV Cache 到共享内存
	//   SGLang:  prefill + decode 并发启动 (ZMQ bootstrap 要求双方同时在线)
	// ──────────────────────────────────────────────────────────

	// [步骤1] 从 DataStore 获取 ModelServer CR 对象
	modelServer := r.store.GetModelServer(modelServerName)
	if modelServer == nil {
		return nil, fmt.Errorf("model server %s not found", modelServerName)
	}

	// [步骤2] 确定 connector 类型: 显式指定 > 引擎推断 > 默认 HTTP
	connectorType := v1alpha1.ConnectorTypeHTTP // 兜底: 最通用, 无需 RDMA 等特殊硬件
	if modelServer.Spec.KVConnector != nil && modelServer.Spec.KVConnector.Type != "" {
		// 用户在 CR 中显式指定了 connector 类型, 优先使用
		connectorType = modelServer.Spec.KVConnector.Type
	} else if modelServer.Spec.InferenceEngine == v1alpha1.SGLang {
		// SGLang 引擎使用 ZMQ bootstrap 协议, 要求 prefill 和 decode 同时在线
		// 不能用顺序 HTTP connector, 必须用 SGLang 专用 connector
		connectorType = connectors.ConnectorTypeSGLang
	}

	// [步骤3] 从工厂获取对应类型的 connector 实例
	connector := r.connectorFactory.GetConnector(connectorType)
	if connector == nil {
		return nil, fmt.Errorf("failed to get connector %s", connectorType)
	}

	return connector, nil
}

// proxyToPDDisaggregated PD 分离场景的请求代理 — 两阶段协调
//
// PD 分离原理:
//   prefill Pod: 只处理输入 Token, 生成 KV Cache (计算密集)
//   decode Pod:  只读取 KV Cache 逐 Token 生成输出 (访存密集)
//   两者独立扩缩容, 互不干扰
//
// 重试策略:
//   maxRetry = min(len(decodePods), len(prefillPods))
//   每次换一对全新的 (prefill, decode) Pod
//
// 在途请求计数 (OnFlightHooks):
//   connector.Proxy() 内部在 prefill/decode 阶段开始/结束时调用这些回调
//   精确更新该 Pod 的在途计数, 确保 least-request 插件看到实时负载
//
// 三种 Connector 模式:
//   NIXL/HTTP: 顺序 prefill(写 KV Cache 到共享内存) → decode(读 KV Cache)
//   SGLang:    并发 prefill + decode (ZMQ bootstrap 协议要求同时在线)
//   MoonCake:  类似 NIXL, 使用 MoonCake 传输层
func (r *Router) proxyToPDDisaggregated(
	c *gin.Context,
	req *http.Request,
	ctx *framework.Context,
	kvConnector connectors.KVConnector,
	modelRequest ModelRequest,
	port int32,
	) error {
		// [步骤1] 获取指标记录器 (与 proxyModelEndpoint 相同)
		var metricsRecorder *metrics.RequestMetricsRecorder
		if recorder, exists := c.Get("metricsRecorder"); exists {
			if rec, ok := recorder.(*metrics.RequestMetricsRecorder); ok {
				metricsRecorder = rec
			}
		}

		modelServerName := fmt.Sprintf("%s/%s", ctx.ModelServerName.Namespace, ctx.ModelServerName.Name)

		// [步骤2] 获取 ModelRoute 名 (用于 Prometheus 指标标签)
		var modelRouteName string
		if routeName, exists := c.Get("modelRouteName"); exists {
			if name, ok := routeName.(string); ok {
				modelRouteName = name
			}
		}

		// [步骤3] 将上游连接信息写入指标记录器
		// 用于在 Grafana 中按 ModelServer/ModelRoute 维度查看请求延迟分布
		if metricsRecorder != nil {
			metricsRecorder.SetUpstreamConnectionInfo(modelServerName, modelRouteName)
		}

		// [步骤4] 确定最大重试次数 = min(decodePods数, prefillPods数)
		// 因为每次重试需要消耗一对 (prefill, decode) Pod, 不能超过任何一侧的可用数量.
		maxRetry := len(ctx.DecodePods)
		if len(ctx.PrefillPods) < maxRetry {
			maxRetry = len(ctx.PrefillPods)
		}

		// [步骤5] 逐对尝试 PD 代理
		for i := 0; i < maxRetry; i++ {
			// 跳过空槽位 (调度器可能没有为某个 decode pod 找到配对的 prefill pod)
			if ctx.PrefillPods[i] == nil || ctx.DecodePods[i] == nil {
				continue
			}
			prefillPod := ctx.PrefillPods[i].GetPod()
			decodePod := ctx.DecodePods[i].GetPod()

			// [5-1] 构建 prefill 和 decode Pod 的地址 (IP:Port)
			prefillAddr := net.JoinHostPort(prefillPod.Status.PodIP, strconv.Itoa(int(port)))
			decodeAddr := net.JoinHostPort(decodePod.Status.PodIP, strconv.Itoa(int(port)))

			klog.V(4).Infof("Attempting PD disaggregated request: prefill=%s, decode=%s", prefillAddr, decodeAddr)

			// [5-2] 构建在途请求计数回调 (OnFlightHooks)
			// 这些回调会被传入 kvConnector.Proxy(), connector 在以下时机调用:
			//   IncrPrefill: prefill 请求即将发出前
			//   DecrPrefill: prefill 请求完成后 (无论成功失败)
			//   IncrDecode:  decode 请求即将发出前
			//   DecrDecode:  decode 请求完成后 (无论成功失败)
			// 这样 least-request 插件能在 Score 阶段看到精确的在途负载,
			// 避免将请求堆到已经有大量在途请求的 Pod 上.
			prefillPodName := types.NamespacedName{Namespace: prefillPod.Namespace, Name: prefillPod.Name}
			decodePodName := types.NamespacedName{Namespace: decodePod.Namespace, Name: decodePod.Name}
			hooks := &connectors.OnFlightHooks{
				IncrPrefill: func() { r.store.IncrPodOnFlightRequests(prefillPodName) },
				DecrPrefill: func() { r.store.DecrPodOnFlightRequests(prefillPodName) },
				IncrDecode:  func() { r.store.IncrPodOnFlightRequests(decodePodName) },
				DecrDecode:  func() { r.store.DecrPodOnFlightRequests(decodePodName) },
			}

			// [5-3] 执行 PD 分离代理
			// kvConnector.Proxy() 内部封装了三种模式:
			//   HTTP/NIXL:  顺序执行 prefill → 等待完成 → 发起 decode
			//   SGLang:     并发执行 prefill + decode (ZMQ bootstrap 要求双方同时在线)
			//   MoonCake:   使用 MoonCake 传输层, 类似 NIXL 流程
			// 返回值: outputTokens (本次请求生成的 token 数), 0 表示无法解析
			outputTokens, err := kvConnector.Proxy(c, modelRequest, prefillAddr, decodeAddr, hooks)

			if err != nil {
				klog.Errorf("proxy failed for prefill pod %s, decode pod %s: %v",
					prefillPod.Name, decodePod.Name, err)
				continue
			}

			// [步骤6] 成功后处理横切关注点 (与 proxyModelEndpoint 的 onUsage 回调对称)
			// 限流计数: 将输出 token 记入滑动窗口
			if outputTokens > 0 && r.loadRateLimiter != nil {
				r.loadRateLimiter.RecordOutputTokens(ctx.Model, outputTokens)
			}

			// Prometheus 指标
			if metricsRecorder != nil {
				metricsRecorder.RecordOutputTokens(outputTokens)
			}

			// [步骤7] 执行 PostHooks (如 prefix-cache 插件写入缓存)
			r.scheduler.RunPostHooks(ctx, i)

			klog.V(4).Infof("kv connector run successful for prefill pod %s, decode pod %s, output tokens: %d",
				prefillPod.Name, decodePod.Name, outputTokens)

			return nil
		}

		// [步骤8] 所有配对都失败 → 500
		c.AbortWithStatusJSON(http.StatusInternalServerError, "all prefill/decode attempts failed")
		return fmt.Errorf("all prefill/decode attempts failed")
	}

// handleFairnessScheduling 公平调度/会话提升的排队流程
//
// 背景: 当多个用户/租户同时发请求, 如果只看 Pod 负载 (least-request),
// 高频用户会持续占据所有 GPU 资源, 低频用户被饿死
//
// 解决方式: 请求先进优先队列排队, 按策略排序后逐个出队执行
//   - Fairness: 按用户最近 Token 用量排序, 用得少 → 优先级高 → 先出队
//   - SessionBoost: 按 session 亲和排序, 同 session → 优先级高 → 先出队
//
// 出队后执行 doLoadbalance(), 成功则 MarkSessionRequestCompleted()
// 让同 session 的后续请求获得 SessionBoost 优先级提升
//
// 两种退出方式:
//   a. NotifyChan 收到信号 → 请求被选中, 执行调度
//   b. reqCtx.Done() → 超时 (504) 或客户端断连 (503)
func (r *Router) handleFairnessScheduling(c *gin.Context, modelRequest ModelRequest, requestID string, modelName string) error {
	// ──────────────────────────────────────────────────────────
	// handleFairnessScheduling: 公平调度/会话提升的排队流程
	//
	// 背景: 当多个用户/租户同时发请求, 如果只看 Pod 负载 (least-request),
	// 高频用户会持续占据所有 GPU 资源, 低频用户被饿死
	//
	// 解决方式: 请求先进优先队列排队, 按策略排序后逐个出队执行
	//   - Fairness: 按用户最近 Token 用量排序, 用得少 → 优先级高 → 先出队
	//   - SessionBoost: 按 session 亲和排序, 同 session → 优先级高 → 先出队
	//
	// 出队后执行 doLoadbalance(), 成功则 MarkSessionRequestCompleted()
	// 让同 session 的后续请求获得 SessionBoost 优先级提升
	//
	// 两种退出方式:
	//   a. NotifyChan 收到信号 → 请求被选中, 执行调度
	//   b. reqCtx.Done() → 超时 (504) 或客户端断连 (503)
	// ──────────────────────────────────────────────────────────

	// [步骤1] 提取 Session ID — 多轮对话的会话标识
	// Session ID 来自 HTTP Header (由用户或上层网关注入)
	// 用于 SessionBoost: 同一 session 的后续请求自动获得更高优先级,
	// 使它们路由到已有该 session KV Cache 的 Pod, 避免冷启动 prefill
	sessionHeader := r.store.GetSessionIDHeader()
	var sessionID string
	if sessionHeader != "" {
		sessionID = c.Request.Header.Get(sessionHeader)
	}

	// [步骤2] 确定请求 ID — 优先使用客户端提供的 X-Request-ID
	if headerReqID := c.Request.Header.Get("X-Request-ID"); headerReqID != "" {
		requestID = headerReqID
	}

	// [步骤3] 提取用户 ID — 用于 Fairness 优先级计算
	// 用户 ID 由 Auth 中间件从 JWT Token 中解析并注入 gin.Context
	var userId string
	if userIdVal, ok := c.Get(common.UserIdKey); ok {
		if s, ok := userIdVal.(string); ok {
			userId = s
		}
	}
	if userId == "" {
		klog.Warningf("user ID not found in request %s", requestID)
	}

	klog.V(4).Infof("[FairnessScheduling] incoming request: reqID=%s user=%s model=%s",
		requestID, userId, modelName)

	// [步骤4] 创建请求级上下文 — 统一客户端断连和服务端超时
	// 超时时间由 Router.queueTimeout 控制 (默认 60s)
	reqCtx, cancel := context.WithTimeout(c.Request.Context(), r.queueTimeout)
	defer cancel()

	// [步骤5] 计算排队优先级
	// 两种模式的优先级策略不同:
	//   SessionBoost 模式: pri=0, 队列按 session boost 排序 (非数值比较)
	//   Fairness 模式:     pri=calculateRequestPriority() 计算值
	//   未认证用户:        pri=MaxFloat64 (最低优先级, 防止饿死认证用户)
	var pri float64
	if EnableSessionBoost {
		pri = 0
	} else if userId != "" {
		pri = r.calculateRequestPriority(userId, modelName)
	} else {
		pri = math.MaxFloat64
	}

	// [步骤6] 构建请求对象并入队
	// NotifyChan: 出队信号, 当请求被优先队列选中时关闭
	// CancelCh:   取消信号, 超时或客户端断连时触发
	queueReq := &datastore.Request{
		UserID:      userId,
		ModelName:   modelName,
		SessionID:   sessionID,
		Priority:    pri,
		RequestTime: time.Now(),
		NotifyChan:  make(chan struct{}),
		CancelCh:    reqCtx.Done(),
	}

	if err := r.store.Enqueue(queueReq); err != nil {
		klog.Errorf("[FairnessScheduling] failed to enqueue: reqID=%s sessionID=%s user=%s model=%s err=%v",
			requestID, sessionID, userId, modelName, err)
		c.AbortWithStatusJSON(http.StatusInternalServerError, fmt.Sprintf("failed to enqueue request: %v", err))
		return fmt.Errorf("failed to enqueue request: %v", err)
	}

	// [步骤7] 阻塞等待: 被选中 or 超时/断连
	select {
	case <-queueReq.NotifyChan:
		// [7a] 请求被优先队列选中出队 → 执行实际调度
		if queueReq.Release != nil {
			defer queueReq.Release()
		}
		klog.V(4).Infof("[FairnessScheduling] request dequeued: reqID=%s user=%s model=%s sessionBoost=%v waitTime=%v",
			requestID, userId, modelName, queueReq.SessionBoost, time.Since(queueReq.RequestTime))
		lbErr := r.doLoadbalance(c, modelRequest)

		// [7a-1] 调度成功后标记 session 完成
		// 这一步是 SessionBoost 的关键: MarkSessionRequestCompleted 会让
		// 同一 session 的后续请求在下次入队时获得 SessionBoost=true,
		// 优先级高于所有非 session 请求, 从而路由到已有 KV Cache 的 Pod
		// 失败时不标记: 因为失败的请求没有预热任何 Pod 的 prefix cache
		if lbErr == nil && sessionID != "" {
			r.store.MarkSessionRequestCompleted(modelName, sessionID)
		}
		return nil
	case <-reqCtx.Done():
		// [7b] 请求在队列中超时或客户端断连
		if queueReq.Release != nil {
			queueReq.Release()
		}
		if errors.Is(reqCtx.Err(), context.DeadlineExceeded) {
			// 超时: 请求在队列中等待超过 queueTimeout → 504 Gateway Timeout
			klog.Errorf("[FairnessScheduling] request timed out in queue: reqID=%s sessionID=%s user=%s model=%s timeout=%v",
				requestID, sessionID, userId, modelName, r.queueTimeout)
			c.AbortWithStatusJSON(http.StatusGatewayTimeout, "Request processing timed out")
			return fmt.Errorf("request processing timed out in fairness queue")
		}
		// 客户端断连: 请求在等待时客户端已关闭连接 → 503 Service Unavailable
		klog.V(4).Infof("[FairnessScheduling] request cancelled (client disconnected): reqID=%s sessionID=%s user=%s model=%s",
			requestID, sessionID, userId, modelName)
		c.AbortWithStatusJSON(http.StatusServiceUnavailable, "Client disconnected while waiting in fairness queue")
		return fmt.Errorf("client disconnected while waiting in fairness queue")
	}
}
