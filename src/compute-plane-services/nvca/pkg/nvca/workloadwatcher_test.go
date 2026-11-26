/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

package nvca

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/icms-translate/translate/common"
	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/icms-translate/translate/function"
	"github.com/prometheus/client_golang/prometheus"
	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	apitypes "k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	listersv1 "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	nvcametrics "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/metrics"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/internal/util/k8sutil"
	nvcastorage "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/storage"
	nvcatypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/types"
)

func TestWorkerRestartScoreRecorder(t *testing.T) {
	newPod := func(labels map[string]string, restarts int32) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "fn-pod", Namespace: "nvcf-backend", Labels: labels},
			Spec: corev1.PodSpec{Containers: []corev1.Container{
				{Name: common.UtilsContainerName, Env: []corev1.EnvVar{{Name: k8sutil.InstanceIDEnvKey, Value: "instance-1"}}},
			}},
			Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
				{Name: common.UtilsContainerName, RestartCount: restarts},
			}},
		}
	}
	functionPod := map[string]string{nvcatypes.ICMSRequestIDKey: "req-1"}
	key := apitypes.NamespacedName{Namespace: "nvcf-backend", Name: "fn-pod"}
	newRecorder := func(t *testing.T, client *k8sfake.Clientset, cachedPods ...*corev1.Pod) *workerRestartScoreRecorder {
		indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
		for _, pod := range cachedPods {
			require.NoError(t, indexer.Add(pod))
		}
		r := newWorkerRestartScoreRecorder(client, listersv1.NewPodLister(indexer), (&k8sutil.TimeConfig{}).Complete())
		t.Cleanup(r.queue.ShutDown)
		return r
	}

	t.Run("pod update only queues a function pod whose worker restarted", func(t *testing.T) {
		tests := []struct {
			name      string
			pod       *corev1.Pod
			wantQueue int
		}{
			{name: "worker restart is queued", pod: newPod(functionPod, 2), wantQueue: 1},
			{name: "worker without restarts is not queued", pod: newPod(functionPod, 0)},
			{name: "non-function pod is not queued", pod: newPod(nil, 2)},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				client := k8sfake.NewClientset(tt.pod)
				r := newRecorder(t, client, tt.pod)
				r.onPodUpdate(context.Background(), tt.pod, tt.pod)
				assert.Equal(t, tt.wantQueue, r.queue.Len())
				assert.Empty(t, client.Actions(), "the pod update must not call the API server")
			})
		}
	})

	t.Run("queued pod is patched with its score", func(t *testing.T) {
		ctx := context.Background()
		pod := newPod(functionPod, 2)
		client := k8sfake.NewClientset(pod)
		r := newRecorder(t, client, pod)
		r.onPodUpdate(ctx, pod, pod)
		require.True(t, r.processNext(ctx))

		got, err := client.CoreV1().Pods(key.Namespace).Get(ctx, key.Name, metav1.GetOptions{})
		require.NoError(t, err)
		var score k8sutil.WorkerRestartScore
		require.NoError(t, json.Unmarshal([]byte(got.Annotations[k8sutil.WorkerRestartScoreAnnotationKey]), &score))
		assert.InDelta(t, 2, score.Score, 0.001)
		assert.Equal(t, int32(2), score.RestartCount)
		assert.Zero(t, r.queue.NumRequeues(key))
	})

	t.Run("deleted pod is dropped", func(t *testing.T) {
		ctx := context.Background()
		client := k8sfake.NewClientset()
		r := newRecorder(t, client)
		r.queue.Add(key)
		require.True(t, r.processNext(ctx))
		assert.Empty(t, client.Actions())
		assert.Zero(t, r.queue.NumRequeues(key))
	})

	t.Run("failed patch is requeued", func(t *testing.T) {
		ctx := context.Background()
		pod := newPod(functionPod, 2)
		client := k8sfake.NewClientset(pod)
		client.PrependReactor("patch", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("api server unavailable")
		})
		r := newRecorder(t, client, pod)
		r.queue.Add(key)
		require.True(t, r.processNext(ctx))
		assert.Equal(t, 1, r.queue.NumRequeues(key))
	})

	t.Run("run stops when the context is done", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		r := newRecorder(t, k8sfake.NewClientset())
		done := make(chan struct{})
		go func() {
			r.run(ctx)
			close(done)
		}()
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("run did not stop after the context was done")
		}
	})
}

func TestGetDefaultWorkloadContainerNamesToWatch(t *testing.T) {
	tests := []struct {
		name string
		want []string
	}{
		{
			name: "default",
			want: []string{
				"init",
				"ess",
				"ess-init",
				common.UtilsContainerName,
				nvcastorage.SMBServerContainerName,
				function.LLMWorkerContainerName,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetDefaultWorkloadContainerNamesToWatch()
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestReportPodCrashAndRestartMetrics(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	ctx = nvcametrics.WithDefaultMetrics(ctx,
		"my-nca-id", "my-cluster", "my-cluster", "1.0.0",
		nvcametrics.WithEventErrorTotalDefaultEvents(append(getAgentEvents(), getNVCAMetricEvents()...)),
		nvcametrics.WithContainerCrashAndRestartTotalDefaultContainerNames(GetDefaultWorkloadContainerNamesToWatch()),
		nvcametrics.WithRegisterer(reg))
	metrics := nvcametrics.FromContext(ctx)

	oldContainerStatuses := []corev1.ContainerStatus{
		{
			Name:         common.UtilsContainerName,
			RestartCount: 1,
			LastTerminationState: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					ExitCode: 0,
				},
			},
		},
	}

	newContainerStatuses := []corev1.ContainerStatus{
		{
			Name:         common.UtilsContainerName,
			RestartCount: 3,
			LastTerminationState: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{
					ExitCode: 1,
				},
			},
		},
	}

	podNamespacedName := "namespace/pod"

	t.Run("Test container restart and crash", func(t *testing.T) {
		metrics.ContainerCrashTotal.Reset()
		metrics.ContainerRestartTotal.Reset()
		reportPodCrashAndRestartMetrics(ctx, oldContainerStatuses, newContainerStatuses, podNamespacedName)
		assert.Equal(t, 1, promtestutil.CollectAndCount(metrics.ContainerCrashTotal))
		assert.Equal(t, 1, promtestutil.CollectAndCount(metrics.ContainerRestartTotal))
	})

	t.Run("Test no restart or crash", func(t *testing.T) {
		metrics.ContainerCrashTotal.Reset()
		metrics.ContainerRestartTotal.Reset()
		newContainerStatuses[0].RestartCount = 1
		newContainerStatuses[0].LastTerminationState.Terminated.ExitCode = 0
		reportPodCrashAndRestartMetrics(ctx, oldContainerStatuses, newContainerStatuses, podNamespacedName)
		assert.Equal(t, 0, promtestutil.CollectAndCount(metrics.ContainerCrashTotal))
		assert.Equal(t, 0, promtestutil.CollectAndCount(metrics.ContainerRestartTotal))
	})
}
