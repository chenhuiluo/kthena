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

package kube

import (
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// =============================================================================
// kubeconfig 构建 — 按 kubectl 语义加载 K8s rest.Config
// =============================================================================
// BuildConfig 是控制面建客户端的统一入口(pkg/controller.SetupController 调它)。
// 加载优先级: ① masterURL/kubeconfigPath 都空 → 优先 in-cluster config(Pod 内 ServiceAccount),
//   失败再走 kubeconfig 链(KUBECONFIG 环境变量路径列表 → ~/.kube/config);
//   ② 显式 kubeconfigPath → 只用该文件; ③ masterURL 覆盖任一来源的 server 地址。
// =============================================================================

// BuildConfig 返回 rest.Config(kubectl 加载语义)。
// 都不传优先 in-cluster config, 再回退 kubeconfig 链; 显式 kubeconfigPath 只用该文件;
// masterURL 覆盖 server 地址。
func BuildConfig(masterURL, kubeconfigPath string) (*rest.Config, error) {
	if masterURL == "" && kubeconfigPath == "" {
		if config, err := rest.InClusterConfig(); err == nil {
			return config, nil
		}
	}
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	loadingRules.ExplicitPath = kubeconfigPath
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules,
		&clientcmd.ConfigOverrides{ClusterInfo: clientcmdapi.Cluster{Server: masterURL}},
	).ClientConfig()
}
