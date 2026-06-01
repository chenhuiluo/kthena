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
	"fmt"
	"os"
	"testing"

	"github.com/volcano-sh/kthena/test/e2e/framework"
	plugincontext "github.com/volcano-sh/kthena/test/e2e/router-plugins/context"
	"github.com/volcano-sh/kthena/test/e2e/utils"
)

var (
	testCtx         *plugincontext.PluginTestContext
	testNamespace   string
	kthenaNamespace string
)

// TestMain installs kthena, deploys plugin mock backends, and runs tests.
func TestMain(m *testing.M) {
	testNamespace = "kthena-e2e-router-plugins-" + utils.RandomString(5)

	config := framework.NewDefaultConfig()
	kthenaNamespace = config.Namespace
	config.NetworkingEnabled = true

	if err := framework.InstallKthena(config); err != nil {
		fmt.Printf("Failed to install kthena: %v\n", err)
		os.Exit(1)
	}

	var err error
	testCtx, err = plugincontext.NewPluginTestContext(testNamespace)
	if err != nil {
		fmt.Printf("Failed to create plugin test context: %v\n", err)
		_ = framework.UninstallKthena(config.Namespace)
		os.Exit(1)
	}

	if err := testCtx.CreateTestNamespace(); err != nil {
		fmt.Printf("Failed to create test namespace: %v\n", err)
		_ = framework.UninstallKthena(config.Namespace)
		os.Exit(1)
	}

	if err := testCtx.SetupPluginComponents(); err != nil {
		fmt.Printf("Failed to setup plugin components: %v\n", err)
		_ = testCtx.DeleteTestNamespace()
		_ = framework.UninstallKthena(config.Namespace)
		os.Exit(1)
	}

	code := m.Run()

	if err := testCtx.CleanupPluginComponents(); err != nil {
		fmt.Printf("Failed to cleanup plugin components: %v\n", err)
	}
	if err := testCtx.DeleteTestNamespace(); err != nil {
		fmt.Printf("Failed to delete test namespace: %v\n", err)
	}
	if err := framework.UninstallKthena(config.Namespace); err != nil {
		fmt.Printf("Failed to uninstall kthena: %v\n", err)
	}

	os.Exit(code)
}
