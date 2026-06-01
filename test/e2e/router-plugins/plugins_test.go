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

package routerplugins

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/volcano-sh/kthena/pkg/kthena-router/scheduler/plugins"
	plugincontext "github.com/volcano-sh/kthena/test/e2e/router-plugins/context"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestSchedulerPluginPrefixCache verifies repeated prompts stick to one pod after warmup.
func TestSchedulerPluginPrefixCache(t *testing.T) {
	ctx := context.Background()
	chatURL, metricsURL, restoreCfg := applySchedulerConfig(t, testCtx.KubeClient, testCtx.KthenaClient, kthenaNamespace, testNamespace, schedulerOnlyPrefixCache)
	t.Cleanup(restoreCfg)

	route := deployModelRouteFromFile(t, ctx, testCtx.KthenaClient, testNamespace, "ModelRoute-plugins.yaml")
	prompt := "kthena-router-plugin-e2e-fixed-prompt-prefix-cache"

	sendRouterChatRequests(t, chatURL, route.Spec.ModelName, prompt, 30)
	time.Sleep(2 * time.Second)

	pods := listReadyMockPods(t, testCtx.KubeClient, testNamespace)
	const total = 100
	since := metav1.NewTime(time.Now())
	sendRouterChatRequests(t, chatURL, route.Spec.ModelName, prompt, total)
	time.Sleep(2 * time.Second)

	maxCount := 0
	routed := 0
	for _, pod := range pods {
		c := countSelectedPodInLogs(t, testCtx.KubeClient, kthenaNamespace, pod.Name, since)
		t.Logf("prefix-cache: pod %s selected %d/%d", pod.Name, c, total)
		routed += c
		if c > maxCount {
			maxCount = c
		}
	}
	t.Logf("prefix-cache: dominant pod %d/%d (of %d log lines)", maxCount, total, routed)
	require.GreaterOrEqual(t, routed, total/2, "expected access logs for routed requests")
	require.GreaterOrEqual(t, float64(maxCount)/float64(total), 0.7)

	waitForSchedulerPluginInMetrics(t, metricsURL, plugins.PrefixCachePluginName, "score")
}

// TestSchedulerPluginLeastRequest verifies the least-request plugin is active and routes successfully.
func TestSchedulerPluginLeastRequest(t *testing.T) {
	ctx := context.Background()
	restoreFastReplicas := scaleDeploymentReplicas(t, testCtx.KubeClient, testNamespace, plugincontext.DeploymentName, 1)
	t.Cleanup(restoreFastReplicas)

	chatURL, metricsURL, restoreCfg := applySchedulerConfig(t, testCtx.KubeClient, testCtx.KthenaClient, kthenaNamespace, testNamespace, schedulerOnlyLeastRequest)
	t.Cleanup(restoreCfg)

	// Build a two-backend route (fast vs slow). Then make the slow backend busy so least-request should avoid it.
	deploySlowLatencyMockStack(t, testCtx.KubeClient, testNamespace)
	_ = deployModelServerFromFile(t, ctx, testCtx.KthenaClient, testNamespace, "ModelServer-plugins-mixed.yaml")
	route := deployModelRouteFromFile(t, ctx, testCtx.KthenaClient, testNamespace, "ModelRoute-plugins-latency.yaml")
	model := route.Spec.ModelName

	fastPods := listReadyPodsByApp(t, testCtx.KubeClient, testNamespace, plugincontext.AppLabel)
	slowPods := listReadyPodsByApp(t, testCtx.KubeClient, testNamespace, plugincontext.SlowMockAppLabel)
	require.NotEmpty(t, fastPods, "fast mock pool")
	require.Len(t, slowPods, 1, "slow mock pool")

	// Saturate slow: direct load raises engine waiting (Filter), router load raises onFlight (Score).
	stopSlowLoad := startLongRequestsToPod(t, slowPods[0], model, "kthena-router-plugin-e2e-fixed-prompt-least-request-slow-load", 20, 512)
	t.Cleanup(stopSlowLoad)
	stopRouterLoad := startLongRequestsViaRouter(t, chatURL, model, "kthena-router-plugin-e2e-fixed-prompt-least-request-load", 30, 512)
	t.Cleanup(stopRouterLoad)
	time.Sleep(8 * time.Second)

	const total = 60
	since := metav1.NewTime(time.Now())
	sendRouterChatRequests(t, chatURL, model, "kthena-router-plugin-e2e-fixed-prompt-least-request-route", total)
	time.Sleep(2 * time.Second)

	fastCount := countSelectedPodsInLogs(t, testCtx.KubeClient, kthenaNamespace, since, fastPods)
	slowCount := countSelectedPodsInLogs(t, testCtx.KubeClient, kthenaNamespace, since, slowPods)
	routed := fastCount + slowCount
	t.Logf("least-request: fast pool %d, slow pool %d (of %d log lines)", fastCount, slowCount, routed)
	require.GreaterOrEqual(t, routed, total/2, "expected access logs for routed requests")
	require.Greater(t, fastCount, slowCount, "least-request should prefer the fast pool over the busy slow pool")
	require.GreaterOrEqual(t, float64(fastCount)/float64(routed), 0.7, "least-request should route at least 70%% to the fast pool")
	require.LessOrEqual(t, float64(slowCount)/float64(routed), 0.2, "least-request should route at most 20%% to the slow pool")

	waitForSchedulerPluginInMetrics(t, metricsURL, plugins.LeastRequestPluginName, "score")
	waitForSchedulerPluginInMetrics(t, metricsURL, plugins.LeastRequestPluginName, "filter")
}

// TestSchedulerPluginLeastLatency verifies least-latency prefers pods without prior latency samples on loaded pod.
func TestSchedulerPluginLeastLatency(t *testing.T) {
	ctx := context.Background()
	restoreFastReplicas := scaleDeploymentReplicas(t, testCtx.KubeClient, testNamespace, plugincontext.DeploymentName, 1)
	t.Cleanup(restoreFastReplicas)

	deploySlowLatencyMockStack(t, testCtx.KubeClient, testNamespace)
	_ = deployModelServerFromFile(t, ctx, testCtx.KthenaClient, testNamespace, "ModelServer-plugins-mixed.yaml")

	chatURL, metricsURL, restoreCfg := applySchedulerConfig(t, testCtx.KubeClient, testCtx.KthenaClient, kthenaNamespace, testNamespace, schedulerOnlyLeastLatency)
	t.Cleanup(restoreCfg)

	route := deployModelRouteFromFile(t, ctx, testCtx.KthenaClient, testNamespace, "ModelRoute-plugins-latency.yaml")
	model := route.Spec.ModelName

	fastPods := listReadyPodsByApp(t, testCtx.KubeClient, testNamespace, plugincontext.AppLabel)
	slowPods := listReadyPodsByApp(t, testCtx.KubeClient, testNamespace, plugincontext.SlowMockAppLabel)
	require.Len(t, fastPods, 1, "fast mock pool")
	require.Len(t, slowPods, 1, "slow mock pool")

	// Prime both pools so router can observe stable TTFT deltas.
	const primeRequests = 40
	directChatToPod(t, fastPods[0], model, "kthena-router-plugin-e2e-fixed-prompt-latency-fast-prime", primeRequests)
	directChatToPod(t, slowPods[0], model, "kthena-router-plugin-e2e-fixed-prompt-latency-slow-prime", primeRequests)
	time.Sleep(3 * time.Second)

	const total = 100
	since := metav1.NewTime(time.Now())
	sendRouterChatRequests(t, chatURL, model, "kthena-router-plugin-e2e-fixed-prompt-latency-route", total)
	time.Sleep(2 * time.Second)

	fastCount := countSelectedPodsInLogs(t, testCtx.KubeClient, kthenaNamespace, since, fastPods)
	slowCount := countSelectedPodsInLogs(t, testCtx.KubeClient, kthenaNamespace, since, slowPods)
	routed := fastCount + slowCount
	t.Logf("least-latency: fast pool %d, slow pool %d (of %d log lines)", fastCount, slowCount, routed)
	require.GreaterOrEqual(t, routed, total/2, "expected access logs for routed requests")
	require.GreaterOrEqual(t, float64(fastCount)/float64(routed), 0.7,
		"least-latency should route at least 70%% to the fast pool")
	require.LessOrEqual(t, float64(slowCount)/float64(routed), 0.3,
		"least-latency should route at most 30%% to the slow pool")

	waitForSchedulerPluginInMetrics(t, metricsURL, plugins.LeastLatencyPluginName, "score")
}

// TestSchedulerPluginLoraAffinity verifies lora-affinity filters to pods that list the adapter in /v1/models.
func TestSchedulerPluginLoraAffinity(t *testing.T) {
	ctx := context.Background()
	restoreReplicas := scaleDeploymentReplicas(t, testCtx.KubeClient, testNamespace, plugincontext.DeploymentName, 2)
	t.Cleanup(restoreReplicas)

	chatURL, metricsURL, restoreCfg := applySchedulerConfig(t, testCtx.KubeClient, testCtx.KthenaClient, kthenaNamespace, testNamespace, schedulerOnlyLoraAffinity)
	t.Cleanup(restoreCfg)

	_ = deployModelRouteFromFile(t, ctx, testCtx.KthenaClient, testNamespace, "ModelRoute-plugins-lora.yaml")
	pods := listReadyMockPods(t, testCtx.KubeClient, testNamespace)
	require.Len(t, pods, 2, "lora test needs exactly 2 mock pods")

	loadedPod := pods[0]
	loadLoraOnPod(t, loadedPod, "lora-A", "/models/lora-A")
	waitForLoRAAdapterRoutable(t, chatURL, "lora-A")
	time.Sleep(3 * time.Second)

	const total = 80
	since := metav1.NewTime(time.Now())
	sendRouterChatRequests(t, chatURL, "lora-A", "kthena-router-plugin-e2e-fixed-prompt-lora-affinity", total)
	time.Sleep(2 * time.Second)

	loadedCount := 0
	otherCount := 0
	for _, pod := range pods {
		c := countSelectedPodInLogs(t, testCtx.KubeClient, kthenaNamespace, pod.Name, since)
		t.Logf("lora-affinity: pod %s selected %d/%d", pod.Name, c, total)
		if pod.Name == loadedPod.Name {
			loadedCount = c
		} else {
			otherCount += c
		}
	}
	routed := loadedCount + otherCount
	t.Logf("lora-affinity: loaded pod %s %d, other pods %d (of %d log lines)", loadedPod.Name, loadedCount, otherCount, routed)
	require.GreaterOrEqual(t, routed, total/2, "expected access logs for routed requests")
	require.Equal(t, 0, otherCount, "lora-affinity filter should not route to pods without the adapter")
	require.GreaterOrEqual(t, float64(loadedCount)/float64(routed), 0.7,
		"lora-affinity should route at least 70%% to the pod that loaded the adapter")

	waitForSchedulerPluginInMetrics(t, metricsURL, plugins.LoraAffinityPluginName, "filter")
}

// TestSchedulerPluginRandom verifies random score plugin is active.
func TestSchedulerPluginRandom(t *testing.T) {
	ctx := context.Background()
	chatURL, metricsURL, restoreCfg := applySchedulerConfig(t, testCtx.KubeClient, testCtx.KthenaClient, kthenaNamespace, testNamespace, schedulerOnlyRandom)
	t.Cleanup(restoreCfg)

	route := deployModelRouteFromFile(t, ctx, testCtx.KthenaClient, testNamespace, "ModelRoute-plugins.yaml")
	model := route.Spec.ModelName
	pods := listReadyMockPods(t, testCtx.KubeClient, testNamespace)
	require.Len(t, pods, 3, "random test needs 3 mock pods")

	const total = 120
	since := metav1.NewTime(time.Now())
	sendRouterChatRequests(t, chatURL, model, "kthena-router-plugin-e2e-fixed-prompt-random", total)
	time.Sleep(2 * time.Second)

	routed := 0
	for _, pod := range pods {
		c := countSelectedPodInLogs(t, testCtx.KubeClient, kthenaNamespace, pod.Name, since)
		t.Logf("random: pod %s selected %d/%d", pod.Name, c, total)
		routed += c
		require.Less(t, float64(c)/float64(total), 0.5, "random should not let pod %s exceed 50%% of traffic", pod.Name)
	}
	require.GreaterOrEqual(t, routed, total/2, "expected access logs for routed requests")

	waitForSchedulerPluginInMetrics(t, metricsURL, plugins.RandomPluginName, "score")
}
