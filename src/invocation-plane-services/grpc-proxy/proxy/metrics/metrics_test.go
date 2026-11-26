/*
SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0

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

package metrics

import (
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A sampler that started before SetNatsStatsConnections replaced its
// connections may finish after it. Its totals must not come back.
func TestRetiredSamplerCannotRestoreItsTotals(t *testing.T) {
	t.Cleanup(func() { SetNatsStatsConnections() })
	nc := &nats.Conn{}
	nc.OutMsgs = 7
	retired := &statsGeneration{conns: []*nats.Conn{nc}}
	currentStats.Store(retired)
	sampleNatsStats(retired)
	require.EqualValues(t, 7, loadNatsStats().OutMsgs)

	SetNatsStatsConnections()
	sampleNatsStats(retired)

	assert.Equal(t, nats.Statistics{}, loadNatsStats())
}
