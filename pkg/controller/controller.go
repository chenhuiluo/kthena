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
	"net/http"
	"os"
	"time"

	clientset "github.com/volcano-sh/kthena/client-go/clientset/versioned"
	autoscaler "github.com/volcano-sh/kthena/pkg/autoscaler/controller"
	"github.com/volcano-sh/kthena/pkg/kube"
	modelbooster "github.com/volcano-sh/kthena/pkg/model-booster-controller/controller"
	"github.com/volcano-sh/kthena/pkg/model-booster-controller/utils"
	modelserving "github.com/volcano-sh/kthena/pkg/model-serving-controller/controller"
	apiextclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
	volcanoClientSet "volcano.sh/apis/pkg/client/clientset/versioned"
)

// =============================================================================
// 控制面组装入口 — 创建并启动 ModelServing/ModelBooster/LWS/Autoscaler 控制器
// =============================================================================
//
// 【职责】cmd/kthena-controller-manager/main.go 调本包 SetupController, 由它:
//   1. 建 kubeClient/kthenaClient/volcanoClient/apiextClient 四个客户端
//   2. 按 cc.Controllers 配置创建对应控制器实例(modelserving/modelbooster/autoscaler)
//   3. LWS 控制器随 ModelServing 一起初始化(CRD 不存在则跳过, 返回 nil)
//   4. startControllers: goroutine 启动各控制器 + debug server(可选)
//   5. 若开启 leader election, 只有当选 leader 才 startControllers(保证多副本只一个干活)
//
// 【为什么 ModelServing 和 LWS 绑在一起】LWS 是 ModelServing 的旁路入口(转 LWS→ModelServing),
//   逻辑上从属 ModelServing 控制器, 故随它一起初始化与启动。
//
// 【三个控制器名常量】modelserving / modelbooster / autoscaler, 供 --controllers flag 过滤。
// =============================================================================

const (
	defaultLeaseDuration = 15 * time.Second
	defaultRenewDeadline = 10 * time.Second
	defaultRetryPeriod   = 2 * time.Second
	leaderElectionId     = "kthena.controller-manager"
	leaseName            = "lease.kthena.controller-manager"

	// 三个控制器名, 供 --controllers flag 启用/禁用
	ModelServingController = "modelserving"
	ModelBoosterController = "modelbooster"
	AutoscalerController   = "autoscaler"
)

// SetupController 控制面组装主入口: 建客户端 → 按配置创建控制器 → 启动。
// 流程:
//   1. BuildConfig + 设 QPS/Burst, 建 kubeClient/kthenaClient/volcanoClient/apiextClient
//   2. 遍历 cc.Controllers, 创建启用的控制器实例(msc/mc/ac), LWS 随 ModelServing 初始化
//   3. startControllers 闭包: goroutine 启动各控制器 + debug server(只绑 localhost, 安全)
//   4. leader election 开则当选 leader 才 startControllers; 否则直接启动
func SetupController(ctx context.Context, cc Config) {
	config, err := kube.BuildConfig(cc.MasterURL, cc.Kubeconfig)
	if err != nil {
		klog.Fatalf("build client config: %v", err)
	}
	// Set QPS and Burst if provided
	if cc.KubeAPIQPS > 0 {
		config.QPS = cc.KubeAPIQPS
	}
	if cc.KubeAPIBurst > 0 {
		config.Burst = cc.KubeAPIBurst
	}
	kubeClient := kubernetes.NewForConfigOrDie(config)
	client := clientset.NewForConfigOrDie(config)
	volcanoClient, err := volcanoClientSet.NewForConfig(config)
	if err != nil {
		klog.Fatalf("failed to create volcano client: %v", err)
	}
	apiextClient, err := apiextclient.NewForConfig(config)
	if err != nil {
		klog.Fatalf("failed to create apiext client: %v", err)
	}

	var mc *modelbooster.ModelBoosterController
	var msc *modelserving.ModelServingController
	var lwsc *modelserving.LWSController
	var ac *autoscaler.AutoscaleController

	for ctrl, enable := range cc.Controllers {
		if enable {
			switch ctrl {
			case ModelBoosterController:
				mc = modelbooster.NewModelBoosterController(kubeClient, client)
			case ModelServingController:
				msc, err = modelserving.NewModelServingController(kubeClient, client, volcanoClient, apiextClient)
				if err != nil {
					klog.Fatalf("failed to create ModelServing controller: %v", err)
				}
				lwsc, err = modelserving.InitializeLWSController(config, kubeClient, client)
				if err != nil {
					klog.Errorf("Failed to initialize LWS controller: %v", err)
				} else if lwsc == nil {
					klog.Info("LeaderWorkerSet CRD not found, LWS support disabled")
				}
			case AutoscalerController:
				ac = autoscaler.NewAutoscaleController(kubeClient, client, cc.AutoscalingSyncPeriodSeconds)
			}
		}
	}

	// startControllers 启动闭包: goroutine 启动各控制器 + debug server(可选)。
	// why 用闭包而非直接执行: 配合 leader election — 只有当选 leader 才调用本闭包启动控制器,
	//   保证多副本部署时只有一个实例真正跑控制器逻辑, 其余待命。
	startControllers := func(ctx context.Context) {
		if mc != nil {
			go mc.Run(ctx, cc.Workers)
			klog.Info("ModelBooster controller started")
		}
		if msc != nil {
			go msc.Run(ctx, cc.Workers)
			klog.Info("ModelServing controller started")

			if lwsc != nil {
				go func() {
					if err = lwsc.Run(ctx, 1); err != nil {
						klog.Errorf("Error running LWS controller: %s", err.Error())
					}
				}()
				klog.Info("ModelServing lws controller started")
			}
		}
		if ac != nil {
			go ac.Run(ctx)
			klog.Info("Autoscaler controller started")
		}

		if cc.DebugPort > 0 {
			go func() {
				debugMux := http.ServeMux{}
				if msc != nil {
					msc.RegisterModelServingDebugEndpoints(&debugMux)
				}
				// Ensure the debug server is only accessible locally for security reasons
				debugAddr := fmt.Sprintf("localhost:%d", cc.DebugPort)
				klog.Infof("Starting debug server on %s", debugAddr)
				server := &http.Server{
					Addr:              debugAddr,
					Handler:           &debugMux,
					ReadHeaderTimeout: 5 * time.Second,
					ReadTimeout:       10 * time.Second,
					WriteTimeout:      10 * time.Second,
					IdleTimeout:       30 * time.Second,
				}
				go func() {
					<-ctx.Done()
					shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					_ = server.Shutdown(shutdownCtx)
				}()
				if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
					klog.Errorf("Debug server failed: %v", err)
				}
			}()
		}
	}

	if cc.EnableLeaderElection {
		startedLeading := func(ctx context.Context) {
			startControllers(ctx)
			klog.Info("Start as leader")
		}
		leaderElector, err := initLeaderElector(kubeClient, startedLeading)
		if err != nil {
			panic(err)
		}
		leaderElector.Run(ctx)
	} else {
		startControllers(ctx)
		klog.Info("Started controllers without leader election")
	}
	<-ctx.Done()
}

// initLeaderElector 初始化 leader elector(lease lock, 默认 15s lease/10s renew/2s retry)。
// why leader election: 多副本部署时只有一个当选 leader 跑控制器, 避免多实例同时调谐冲突。
func initLeaderElector(kubeClient kubernetes.Interface, startedLeading func(ctx context.Context)) (*leaderelection.LeaderElector, error) {
	resourceLock, err := newResourceLock(kubeClient)
	if err != nil {
		return nil, err
	}
	leaderElector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          resourceLock,
		LeaseDuration: defaultLeaseDuration,
		RenewDeadline: defaultRenewDeadline,
		RetryPeriod:   defaultRetryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: startedLeading,
			OnStoppedLeading: func() {
				klog.Error("leader election lost")
			},
		},
		ReleaseOnCancel: false,
		Name:            leaderElectionId,
	})
	if err != nil {
		return nil, err
	}
	return leaderElector, nil
}

// newResourceLock 返回 lease lock(用 coordination.k8s.io Lease 资源做选主)。
// leader id = hostname + 随机 UUID, 保证多副本唯一。
func newResourceLock(client kubernetes.Interface) (*resourcelock.LeaseLock, error) {
	namespace, err := utils.GetInClusterNameSpace()
	if err != nil {
		return nil, err
	}
	// Leader id, should be unique
	id, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	id = id + "_" + string(uuid.NewUUID())
	return &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      leaseName,
			Namespace: namespace,
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: id,
		},
	}, nil
}
