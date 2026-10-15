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

package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"github.com/volcano-sh/kthena/pkg/autoscaler/util"
	autoscalerwebhook "github.com/volcano-sh/kthena/pkg/autoscaler/webhook"
	"github.com/volcano-sh/kthena/pkg/controller"
	modelboosterwebhook "github.com/volcano-sh/kthena/pkg/model-booster-controller/webhook"
	modelservingwebhook "github.com/volcano-sh/kthena/pkg/model-serving-controller/webhook"
	webhookcert "github.com/volcano-sh/kthena/pkg/webhook/cert"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

// =============================================================================
// kthena-controller-manager — kthena 控制面统一启动入口
// =============================================================================
//
// 【职责】一个进程同时跑三个控制器 + 一个 webhook server:
//   控制器: ModelServing(推理编排) / ModelBooster(一键部署,已废弃) / Autoscaler(弹性伸缩)
//   webhook: 校验+变更(ModelServing/ModelBooster/AutoscalingPolicy) 统一一个 mux, 8443 端口
//   实际组装逻辑在 pkg/controller.SetupController。
//
// 【关键 flag】
//   --controllers: 启用哪些控制器(默认 * 全开; 可 -modelserving 禁用某项)
//   --enable-webhook: 是否启 webhook(默认 true)
//   --leader-elect: 多副本选主(默认 false)
//   --workers: 每 controller worker 数(默认 5)
//   --debug-port: debug 端口(datastore cache dump), 0 禁用
//
// 【webhook 证书】三级优先级: Secret → 已有文件 → 自动生成自签证书(写回 Secret 并更新 webhook config CA)。
//   生成后 waitForCertsReady 等文件就位再启 TLS server。
//
// 【启动顺序】main: 解析 flag → 起 webhook goroutine → controller.SetupController(阻塞)。
//   信号处理: SIGINT/SIGTERM → cancel ctx → 优雅退出。
// =============================================================================

// webhookConfig webhook server 配置
type webhookConfig struct {
	tlsCertFile    string
	tlsPrivateKey  string
	port           int
	webhookTimeout int
	certSecretName string
	serviceName    string
	kubeAPIQPS     float32
	kubeAPIBurst   int
}

// main 进程入口: 解析 flag → 起 webhook goroutine → SetupController 阻塞运行。
func main() {
	var enableWebhook bool
	var wc webhookConfig
	var cc controller.Config
	var controllers []string
	// Initialize klog flags
	klog.InitFlags(nil)
	// Opt into fixed stderrthreshold behavior (kubernetes/klog#212).
	if err := flag.CommandLine.Set("legacy_stderr_threshold_behavior", "false"); err != nil {
		klog.Fatalf("Failed to set legacy_stderr_threshold_behavior: %v", err)
	}
	if err := flag.CommandLine.Set("stderrthreshold", "INFO"); err != nil {
		klog.Fatalf("Failed to set stderrthreshold: %v", err)
	}
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.StringVar(&cc.Kubeconfig, "kubeconfig", "", "kubeconfig file path")
	pflag.BoolVar(&enableWebhook, "enable-webhook", true, "If true, webhook will be used. Default is true")
	pflag.StringVar(&cc.MasterURL, "master", "", "The address of the Kubernetes API server. Overrides any value in kubeconfig. Only required if out-of-cluster.")
	pflag.StringVar(&wc.tlsCertFile, "tls-cert-file", "/etc/tls/tls.crt", "File containing the x509 Certificate for HTTPS")
	pflag.StringVar(&wc.tlsPrivateKey, "tls-private-key-file", "/etc/tls/tls.key", "File containing the x509 private key to --tls-cert-file")
	pflag.IntVar(&wc.port, "port", 8443, "Secure port that the webhook listens on")
	pflag.IntVar(&wc.webhookTimeout, "webhook-timeout", 30, "Timeout for webhook operations in seconds")
	pflag.StringVar(&wc.certSecretName, "cert-secret-name", "kthena-controller-manager-webhook-certs", "Name of the secret to store auto-generated certificates")
	pflag.StringVar(&wc.serviceName, "service-name", "kthena-controller-manager-webhook", "Service name for the webhook server")
	pflag.BoolVar(&cc.EnableLeaderElection, "leader-elect", false, "Enable leader election for controller. "+
		"Enabling this will ensure there is only one active controller. Default is false.")
	pflag.IntVar(&cc.Workers, "workers", 5, "number of workers to run. Default is 5")
	pflag.StringSliceVar(&controllers, "controllers", []string{"*"}, "A list of controllers to enable. '*' enables all controllers, 'foo' enables the controller "+
		"named 'foo', '-foo' disables the controller named 'foo'.\nIf both '+foo' and '-foo' are set simultaneously, then controller named 'foo' will be enabled.\nAll controllers: 'modelserving', 'modelbooster', 'autoscaler'")
	pflag.Float32Var(&cc.KubeAPIQPS, "kube-api-qps", 0, "QPS to use while talking with kubernetes apiserver. If 0, use default value.")
	pflag.IntVar(&cc.KubeAPIBurst, "kube-api-burst", 0, "Burst to use while talking with kubernetes apiserver. If 0, use default value.")
	pflag.IntVar(&cc.DebugPort, "debug-port", 0, "Port for debug server to dump internal cache. If 0, debug server is disabled.")
	pflag.IntVar(&cc.AutoscalingSyncPeriodSeconds, "autoscaling-sync-period-seconds", util.AutoscalingSyncPeriodSeconds,
		"Reconcile interval in seconds for the autoscaler. Smaller values react faster to traffic spikes but increase API server load. 0 falls back to the default (15).")
	pflag.Parse()

	cc.Controllers = parseControllers(controllers)

	pflag.CommandLine.VisitAll(func(f *pflag.Flag) {
		klog.Infof("Flag: %s, Value: %s", f.Name, f.Value.String())
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		klog.Info("Received termination, signaling shutdown")
		cancel()
	}()
	wc.kubeAPIQPS = cc.KubeAPIQPS
	wc.kubeAPIBurst = cc.KubeAPIBurst
	if enableWebhook {
		go func() {
			if err := setupWebhook(ctx, wc); err != nil {
				os.Exit(1)
			}
		}()
	}
	controller.SetupController(ctx, cc)
}

const validatingWebhookName = "kthena-controller-manager-validating-webhook"
const mutatingWebhookName = "kthena-controller-manager-mutating-webhook"

// ensureWebhookCertificate 自动生成自签证书写入 Secret, 返回 CA bundle。
// DNS 名含 <service>.<ns>.svc 和 .svc.cluster.local 两种(集群内访问)。
func ensureWebhookCertificate(ctx context.Context, kubeClient kubernetes.Interface, wc webhookConfig) ([]byte, error) {
	namespace := getNamespace()
	dnsNames := []string{
		fmt.Sprintf("%s.%s.svc", wc.serviceName, namespace),
		fmt.Sprintf("%s.%s.svc.cluster.local", wc.serviceName, namespace),
	}
	klog.Infof("Auto-generating certificate for webhook server (secret=%s service=%s)", wc.certSecretName, wc.serviceName)
	return webhookcert.EnsureCertificate(ctx, kubeClient, namespace, wc.certSecretName, dnsNames)
}

// setupWebhook 启动统一的 webhook server(校验+变更, 一个 mux 多路由):
//
// 流程:
//   1. InClusterConfig + 设 QPS/Burst, 建 kubeClient
//   2. CA bundle 三级优先级: Secret → 已有证书文件 → 自动生成(写回 Secret + 更新 webhook config CA)
//   3. 注册路由: /validate-workload-...-modelserving(ModelServing 校验)
//      /validate/modelbooster + /mutate/modelbooster(ModelBooster 校验/变更)
//      /validate/autoscalingpolicy + /mutate/autoscalingpolicy(AutoscalingPolicy 校验/变更)
//      /healthz(健康检查)
//   4. waitForCertsReady 等证书文件就位 → ListenAndServeTLS 启动 → 等 ctx 退出优雅关闭
//
// why 统一一个 mux: 三个 CRD 的 webhook 共用一个进程+端口, 减少部署复杂度。
func setupWebhook(ctx context.Context, wc webhookConfig) error {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		klog.Fatalf("build client config: %v", err)
		return err
	}
	// Set QPS and Burst if provided
	if wc.kubeAPIQPS > 0 {
		cfg.QPS = wc.kubeAPIQPS
	}
	if wc.kubeAPIBurst > 0 {
		cfg.Burst = wc.kubeAPIBurst
	}

	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("failed to create kubeClient: %v", err)
		return err
	}

	// Secret -> File -> Generate precedence for CA bundle selection
	namespace := getNamespace()
	var caBundle []byte

	// 1. Try secret first.
	if bundle, err := webhookcert.LoadCertBundleFromSecret(ctx, kubeClient, namespace, wc.certSecretName); err != nil {
		klog.Warningf("Error reading CA bundle from secret %s: %v", wc.certSecretName, err)
	} else if bundle != nil {
		klog.Infof("Loaded CA bundle from secret %s", wc.certSecretName)
		caBundle = bundle.CAPEM
	}

	// 2. If not from secret, try existing cert file.
	if caBundle == nil {
		if !fileExists(wc.tlsPrivateKey) || !fileExists(wc.tlsCertFile) {
			b, err := ensureWebhookCertificate(ctx, kubeClient, wc)
			if err != nil {
				klog.Fatalf("Failed to auto-generate webhook certificates: %v", err)
			}
			caBundle = b
		}
	}

	if caBundle != nil {
		// Always update both webhook configurations with the chosen CA bundle
		if err := webhookcert.UpdateValidatingWebhookCABundle(ctx, kubeClient, validatingWebhookName, caBundle); err != nil {
			klog.Warningf("Failed to update ValidatingWebhookConfiguration CA bundle: %v", err)
		}
		if err := webhookcert.UpdateMutatingWebhookCABundle(ctx, kubeClient, mutatingWebhookName, caBundle); err != nil {
			klog.Warningf("Failed to update MutatingWebhookConfiguration CA bundle: %v", err)
		}
	}

	mux := http.NewServeMux()

	modelServingValidator := modelservingwebhook.NewModelServingValidator()
	mux.HandleFunc("/validate-workload-ai-v1alpha1-modelserving", modelServingValidator.Handle)

	modelValidator := modelboosterwebhook.NewModelValidator()
	modelMutator := modelboosterwebhook.NewModelMutator()
	autoscalingPolicyValidator := autoscalerwebhook.NewAutoscalingPolicyValidator()
	autoscalingPolicyMutator := autoscalerwebhook.NewAutoscalingPolicyMutator()
	mux.HandleFunc("/validate/modelbooster", modelValidator.Handle)
	mux.HandleFunc("/mutate/modelbooster", modelMutator.Handle)
	mux.HandleFunc("/validate/autoscalingpolicy", autoscalingPolicyValidator.Handle)
	mux.HandleFunc("/mutate/autoscalingpolicy", autoscalingPolicyMutator.Handle)

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("ok")); err != nil {
			klog.Errorf("failed to write health check response: %v", err)
		}
	})

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", wc.port),
		Handler:      mux,
		ReadTimeout:  time.Duration(wc.webhookTimeout) * time.Second,
		WriteTimeout: time.Duration(wc.webhookTimeout) * time.Second,
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
	}

	// Wait for both cert and key files to exist (in case they are mounted by Kubernetes)
	ok := waitForCertsReady(wc.tlsPrivateKey, wc.tlsCertFile)
	if !ok {
		return fmt.Errorf("TLS cert/key files not found, webhook server cannot start")
	}

	go func() {
		klog.Infof("Starting webhook server on %s", server.Addr)
		if err := server.ListenAndServeTLS(wc.tlsCertFile, wc.tlsPrivateKey); err != nil && !errors.Is(err, http.ErrServerClosed) {
			klog.Fatalf("failed to start unified webhook server: %v", err)
		}
	}()
	<-ctx.Done()
	ctxTimeout, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(ctxTimeout)
	return nil
}

// getNamespace 返回当前 Pod namespace(读 POD_NAMESPACE env), 无则 "default"
func getNamespace() string {
	ns := os.Getenv("POD_NAMESPACE")
	if ns == "" {
		return "default"
	}
	return ns
}

// fileExists 文件是否存在
func fileExists(path string) bool {
	if path == "" {
		return false
	}
	_, err := os.Stat(path)
	return err == nil
}

// waitForCertsReady 等待 TLS 证书/密钥文件就位(最多 30s, 500ms 轮询)。
// why 等: 证书可能由 K8s 挂载, 异步就绪, 直接启会失败。
func waitForCertsReady(keyFile, CertFile string) bool {
	waitTimeout := 30 * time.Second
	waitInterval := 500 * time.Millisecond
	start := time.Now()
	for {
		if fileExists(CertFile) && fileExists(keyFile) {
			return true
		}
		if time.Since(start) > waitTimeout {
			klog.Warningf("timeout waiting for TLS cert/key files to appear at %s and %s", keyFile, CertFile)
			return false
		}
		time.Sleep(waitInterval)
	}
}

// parseControllers 把 --controllers flag 解析成"启用控制器"集合。
// 默认三个全开(modelserving/modelbooster/autoscaler); 若解析后一个没启用则回退默认(全开)。
func parseControllers(controllers []string) map[string]bool {
	// defaultControllers defines all available controllers as enabled
	defaultControllers := map[string]bool{
		controller.ModelServingController: true,
		controller.ModelBoosterController: true,
		controller.AutoscalerController:   true,
	}

	enableControllers := make(map[string]bool)

	for i := range controllers {
		controllers[i] = strings.TrimSpace(controllers[i])
		if controllers[i] == "" {
			continue
		}
	}

	for ctrlName := range defaultControllers {
		// Determine if the controller should be enabled based on user input
		if isControllerEnabled(ctrlName, controllers) {
			enableControllers[ctrlName] = true
		} else {
			klog.Infof("Controller <%s> is disabled", ctrlName)
		}
	}

	if len(enableControllers) == 0 {
		klog.Warning("No controllers are enabled")
		return defaultControllers
	}

	return enableControllers
}

// isControllerEnabled 判断某控制器是否启用。规则:
//   - +name 或 name: 显式启用
//   - -name: 显式禁用
//   - *: 通配启用全部(默认)
//   - 无显式包含/排除: 默认 true(全开)
func isControllerEnabled(name string, controllers []string) bool {
	// Because controllers are enabled by default, the default return value is true.
	hasStar := false
	// if no explicit inclusion or exclusion, enable all controllers by default
	if len(controllers) == 0 {
		return true
	}
	for _, ctrl := range controllers {
		// if we get here, there was an explicit inclusion
		if ctrl == name {
			return true
		}
		// if we get here, there was an explicit inclusion
		if ctrl == "+"+name {
			return true
		}
		// if we get here, there was an explicit exclusion
		if ctrl == "-"+name {
			return false
		}
		if ctrl == "*" {
			hasStar = true
		}
	}
	// if we get here, there was no explicit inclusion or exclusion
	return hasStar
}
