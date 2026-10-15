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

// Config 控制面启动配置(由 cmd main.go 从 flag 解析填充, 传给 SetupController)。
type Config struct {
	EnableLeaderElection         bool          // 是否开启 leader election(多副本只一个干活)
	Workers                      int          // 每个 controller 的 worker 数(并发调谐数, 默认 5)
	Kubeconfig                   string          // kubeconfig 文件路径(集群外调试用)
	MasterURL                    string          // API server 地址(覆盖 kubeconfig 里的)
	Controllers                  map[string]bool // 启用的控制器集合(modelserving/modelbooster/autoscaler)
	KubeAPIQPS                   float32         // K8s API 限速 QPS, 0 用默认
	KubeAPIBurst                 int             // K8s API burst, 0 用默认
	DebugPort                    int             // debug 端口(导 datastore cache dump), 0 禁用
	AutoscalingSyncPeriodSeconds int             // autoscaler 调谐间隔秒, 0 用默认(15)
}
