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

package util

const (
	DefaultSyncPeriodSeconds        = 15
	ScaleUpSyncPeriodSeconds        = 5
	ScaleDownSyncPeriodSeconds      = 30
	SloQuantileSlidingWindowSeconds = 60
	SloQuantileDataKeepSeconds      = 300
	SloQuantilePercentile           = 95
	AutoscaleCtxTimeoutSeconds      = 3
)

// AutoscalingSyncPeriodSeconds is kept as a deprecated alias for
// DefaultSyncPeriodSeconds to preserve backward compatibility for
// external consumers. New code should use DefaultSyncPeriodSeconds.
//
// Deprecated: Use DefaultSyncPeriodSeconds instead.
const AutoscalingSyncPeriodSeconds = DefaultSyncPeriodSeconds
