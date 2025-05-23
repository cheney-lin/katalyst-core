/*
Copyright 2022 The Katalyst Authors.

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

package general

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHeartbeatCheck(t *testing.T) {
	t.Parallel()

	testCheckName := "testHeartBeatCheck"
	RegisterHeartbeatCheck(testCheckName, 2*time.Second, HealthzCheckStateReady, 2*time.Second)

	results := GetRegisterReadinessCheckResult()
	status, ok := results[HealthzCheckName(testCheckName)]
	assert.True(t, ok)
	assert.True(t, status.Ready)

	// timeout
	time.Sleep(3 * time.Second)
	results = GetRegisterReadinessCheckResult()
	status, ok = results[HealthzCheckName(testCheckName)]
	assert.True(t, ok)
	assert.False(t, status.Ready)

	// updated with error
	err := UpdateHealthzStateByError(testCheckName, fmt.Errorf("error"))
	assert.NoError(t, err)
	results = GetRegisterReadinessCheckResult()
	status, ok = results[HealthzCheckName(testCheckName)]
	assert.True(t, ok)
	assert.True(t, status.Ready)

	// error no longer tolerable
	time.Sleep(3 * time.Second)
	results = GetRegisterReadinessCheckResult()
	status, ok = results[HealthzCheckName(testCheckName)]
	assert.True(t, ok)
	assert.False(t, status.Ready)

	// recover
	err = UpdateHealthzStateByError(testCheckName, nil)
	assert.NoError(t, err)
	results = GetRegisterReadinessCheckResult()
	status, ok = results[HealthzCheckName(testCheckName)]
	assert.True(t, ok)
}

func TestTemporaryHeartbeatCheck(t *testing.T) {
	t.Parallel()

	testCheckName := "testTemporaryHeartBeatCheck"
	RegisterTemporaryHeartbeatCheck(testCheckName, 2*time.Second, HealthzCheckStateReady, 2*time.Second)

	results := GetRegisterReadinessCheckResult()
	status, ok := results[HealthzCheckName(testCheckName)]
	assert.True(t, ok)
	assert.True(t, status.Ready)

	// timeout
	time.Sleep(3 * time.Second)
	results = GetRegisterReadinessCheckResult()
	status, ok = results[HealthzCheckName(testCheckName)]
	assert.True(t, ok)
	assert.False(t, status.Ready)

	// updated with error
	err := UpdateHealthzStateByError(testCheckName, fmt.Errorf("error"))
	assert.NoError(t, err)
	results = GetRegisterReadinessCheckResult()
	status, ok = results[HealthzCheckName(testCheckName)]
	assert.True(t, ok)
	assert.True(t, status.Ready)

	// error no longer tolerable
	time.Sleep(3 * time.Second)
	results = GetRegisterReadinessCheckResult()
	status, ok = results[HealthzCheckName(testCheckName)]
	assert.True(t, ok)
	assert.False(t, status.Ready)

	// recover
	err = UpdateHealthzStateByError(testCheckName, nil)
	assert.NoError(t, err)
	results = GetRegisterReadinessCheckResult()
	status, ok = results[HealthzCheckName(testCheckName)]
	assert.True(t, ok)

	// unregister
	UnregisterTemporaryHeartbeatCheck(testCheckName)
	results = GetRegisterReadinessCheckResult()
	status, ok = results[HealthzCheckName(testCheckName)]
	assert.False(t, ok)
}

func TestReportCheck(t *testing.T) {
	t.Parallel()

	testCheckName := "testReportCheck"
	RegisterReportCheck(testCheckName, 10*time.Millisecond, HealthzCheckStateNotReady)

	// assume first report is failed when UpdateHealthzStateByError is not invoked
	results := GetRegisterReadinessCheckResult()
	status, ok := results[HealthzCheckName(testCheckName)]
	assert.True(t, ok)
	assert.False(t, status.Ready)

	// update the status to ready
	err := UpdateHealthzStateByError(testCheckName, nil)
	assert.NoError(t, err)
	results = GetRegisterReadinessCheckResult()
	status, ok = results[HealthzCheckName(testCheckName)]
	assert.True(t, ok)
	assert.True(t, status.Ready)

	// timeout
	time.Sleep(20 * time.Millisecond)
	results = GetRegisterReadinessCheckResult()
	status, ok = results[HealthzCheckName(testCheckName)]
	assert.True(t, ok)
	assert.False(t, status.Ready)
	assert.Equal(t, "timeout", status.Message)

	// updated with error
	err = UpdateHealthzStateByError(testCheckName, errors.New("error"))
	assert.NoError(t, err)
	results = GetRegisterReadinessCheckResult()
	status, ok = results[HealthzCheckName(testCheckName)]
	assert.True(t, ok)
	assert.False(t, status.Ready)
	assert.Equal(t, "error", status.Message)
}
