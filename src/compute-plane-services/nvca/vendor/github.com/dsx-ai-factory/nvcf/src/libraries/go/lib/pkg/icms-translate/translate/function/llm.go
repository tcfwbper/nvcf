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

package function

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"

	"github.com/dsx-ai-factory/nvcf/src/libraries/go/lib/pkg/icms-translate/translate/common"
)

const (
	LLMWorkerContainerName = "llm-worker"
	// llmMetricsPort is the TCP port exposed by Pylon for Prometheus metrics.
	llmMetricsPort int32 = 9089

	//nolint:gosec
	llmCredentialManagerImageEnv = "LLM_CREDENTIAL_MANAGER_IMAGE"
	//nolint:gosec
	llmCredentialManagerImageDefault = "nvcr.io/0651155215864979/ncp-dev/nvcf_worker_llm_credentials:2.109.0"
	llmRouterClientImageEnv          = "LLM_ROUTER_CLIENT_IMAGE"
	llmRouterClientImageDefault      = "nvcr.io/0651155215864979/ncp-dev/stargate-client:0.4.0"
	llmRequestRouterAddressEnv       = "LLM_REQUEST_ROUTER_ADDRESS"
	legacyStargateAddressEnv         = "STARGATE_ADDRESS"

	llmDirMountPath    = "/var/run/llm"
	llmWorkerTokenPath = llmDirMountPath + "/worker-token"

	essAssertionTokenPathEnv = "ESS_ASSERTION_TOKEN_PATH"
	essAssertionTokenPath    = common.EssConfigDir + "/jwt.token"

	// Kubernetes signals the inference server and Pylon at the same time on
	// pod deletion. The inference server's preStop pause keeps it serving
	// while Pylon stops. A Pylon that reads llmWorkerDrainTimeoutEnv drains
	// for up to that timeout and then shuts down within
	// llmWorkerShutdownAllowance, so it is done before the pause ends. The
	// pod's grace period then leaves the inference server
	// llmInferenceExitAllowance to exit.
	llmWorkerDrainTimeoutEnv     = "PYLON_SHUTDOWN_DRAIN_TIMEOUT_MS"
	defaultLLMInferenceStopPause = 60 * time.Second
	llmWorkerShutdownAllowance   = 5 * time.Second
	llmInferenceExitAllowance    = 60 * time.Second

	// Set per cluster through functionEnvOverrides. A Pylon without /ready
	// answers 404, so its worker never becomes Ready.
	llmReadinessGateEnv = "PYLON_READINESS_GATE"
	llmReadinessPath    = "/ready"
)

// llmShutdown is the shutdown timing of an LLM container function pod, all
// derived from the inference stop pause.
type llmShutdown struct {
	pause time.Duration
	drain time.Duration
}

func newLLMShutdown(configuredPause time.Duration) llmShutdown {
	pause := configuredPause.Truncate(time.Second)
	if pause <= 0 {
		pause = defaultLLMInferenceStopPause
	}
	return llmShutdown{
		pause: pause,
		drain: max(pause-llmWorkerShutdownAllowance, 0),
	}
}

// minGraceSeconds is the shortest pod grace period that lets the inference
// server finish the pause and then exit. It adds whole seconds, which cannot
// overflow for any pause a time.Duration can hold.
func (s llmShutdown) minGraceSeconds() int64 {
	return int64(s.pause/time.Second) + int64(llmInferenceExitAllowance/time.Second)
}

// inferenceLifecycle delays the inference server's stop signal so it keeps
// serving requests that Pylon is still draining.
func (s llmShutdown) inferenceLifecycle() *corev1.Lifecycle {
	return &corev1.Lifecycle{
		PreStop: &corev1.LifecycleHandler{
			Exec: &corev1.ExecAction{
				Command: []string{"sleep", strconv.Itoa(int(s.pause / time.Second))},
			},
		},
	}
}

// holdTelemetrySidecars delays the stop signal of the BYOO collector and
// fluent-bit by the same pause, so they keep shipping the inference server's
// logs and telemetry while it serves. Their images have no sleep binary, so
// this uses the kubelet's sleep action (stable since Kubernetes 1.34).
func (s llmShutdown) holdTelemetrySidecars(pod *corev1.Pod) {
	for i := range pod.Spec.Containers {
		switch pod.Spec.Containers[i].Name {
		case common.ByooOTelCollectorPodNameBase, common.FluentbitContainerName:
			pod.Spec.Containers[i].Lifecycle = &corev1.Lifecycle{
				PreStop: &corev1.LifecycleHandler{
					Sleep: &corev1.SleepAction{Seconds: int64(s.pause / time.Second)},
				},
			}
		}
	}
}

func normalizeLLMRequestRouterAddressEnvAliases(envSet map[string]string) {
	llmRequestRouterAddress := envSet[llmRequestRouterAddressEnv]
	if llmRequestRouterAddress == "" {
		llmRequestRouterAddress = envSet[legacyStargateAddressEnv]
	}
	if llmRequestRouterAddress == "" {
		return
	}
	if envSet[llmRequestRouterAddressEnv] == "" {
		envSet[llmRequestRouterAddressEnv] = llmRequestRouterAddress
	}
	if envSet[legacyStargateAddressEnv] == "" {
		envSet[legacyStargateAddressEnv] = llmRequestRouterAddress
	}
}

// pylon probes the upstream over HTTP on the inference port, so the function's
// declared health endpoint only carries over when the function keeps both
// aligned. Otherwise pylon falls back to its own candidate paths.
func upstreamHealthPath(allEnvSet map[string]string) string {
	path := strings.TrimSpace(allEnvSet["INFERENCE_HEALTH_ENDPOINT"])
	if path == "" {
		return ""
	}
	if strings.EqualFold(strings.TrimSpace(allEnvSet["INFERENCE_HEALTH_PROTOCOL"]), "grpc") {
		return ""
	}
	healthPort, err := strconv.Atoi(strings.TrimSpace(allEnvSet["INFERENCE_HEALTH_PORT"]))
	if err == nil && healthPort > 0 && strconv.Itoa(healthPort) != strings.TrimSpace(allEnvSet["INFERENCE_PORT"]) {
		return ""
	}
	return path
}

// llmWorkerReadinessProbe gates the llm-worker container, and so the function
// status, on Pylon's /ready. Pylon has no gRPC prober, so a gRPC health check
// gets no probe.
func llmWorkerReadinessProbe(allEnvSet map[string]string) *corev1.Probe {
	// Pylon accepts only "true" or "false" and exits on any other value, so
	// only the exact value Pylon reads as on adds the probe.
	if allEnvSet[llmReadinessGateEnv] != "true" {
		return nil
	}
	if strings.EqualFold(strings.TrimSpace(allEnvSet["INFERENCE_HEALTH_PROTOCOL"]), "grpc") {
		return nil
	}
	// Same timings as the utils container probe.
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: llmReadinessPath,
				Port: intstr.FromString(common.WorkerMetricsPortName),
			},
		},
		InitialDelaySeconds: 0,
		PeriodSeconds:       10,
		FailureThreshold:    2,
		SuccessThreshold:    1,
		TimeoutSeconds:      6,
	}
}

// newLLMRouterClientContainer builds the Pylon sidecar for an LLM worker.
func newLLMRouterClientContainer(
	ls *LaunchSpecification,
	allEnvSet map[string]string,
	tcfg TranslateConfig,
	instanceID string,
	isHelm bool,
) (corev1.Container, error) {
	llmRouterClientImage := allEnvSet[llmRouterClientImageEnv]
	if llmRouterClientImage == "" {
		llmRouterClientImage = llmRouterClientImageDefault
		// return corev1.Container{}, fmt.Errorf("LLM router client image is not set")
	}
	llmRequestRouterAddress := allEnvSet[llmRequestRouterAddressEnv]
	if llmRequestRouterAddress == "" {
		llmRequestRouterAddress = allEnvSet[legacyStargateAddressEnv]
	}
	if llmRequestRouterAddress == "" {
		return corev1.Container{}, fmt.Errorf(
			"LLM request router address is not set (%s env or %s legacy env)",
			llmRequestRouterAddressEnv,
			legacyStargateAddressEnv,
		)
	}

	llmEnvSet := make(map[string]string, len(allEnvSet)+2)
	for k, v := range allEnvSet {
		llmEnvSet[k] = v
	}
	if llmEnvSet[llmRequestRouterAddressEnv] == "" && llmEnvSet[legacyStargateAddressEnv] == "" {
		llmEnvSet[llmRequestRouterAddressEnv] = llmRequestRouterAddress
	}
	normalizeLLMRequestRouterAddressEnvAliases(llmEnvSet)
	// The drain must end before the inference preStop pause does, so the
	// platform value replaces any value from the function's environment. Helm
	// functions run Pylon in a separate pod with no pause, so they keep
	// Pylon's default drain behavior.
	if !isHelm {
		drain := newLLMShutdown(tcfg.LLMInferenceStopPause.Duration).drain
		llmEnvSet[llmWorkerDrainTimeoutEnv] = strconv.FormatInt(drain.Milliseconds(), 10)
	}

	envs := common.MapToEnv(llmEnvSet)
	envs = append(envs,
		corev1.EnvVar{
			Name:  "INSTANCE_ID",
			Value: instanceID,
		},
		corev1.EnvVar{
			Name:  "SHARED_CONFIG_DIR",
			Value: ConfigDirPath,
		},
		corev1.EnvVar{
			Name:  "WORKER_TOKEN_PATH",
			Value: llmWorkerTokenPath,
		},
	)

	var upstreamHttpBaseUrl string
	svcPort := allEnvSet["INFERENCE_PORT"]
	if isHelm {
		svcName := allEnvSet["HELM_CHART_INFERENCE_SERVICE_NAME"]
		if tcfg.Namespace == "" {
			upstreamHttpBaseUrl = fmt.Sprintf("http://%s:%s", svcName, svcPort)
		} else {
			upstreamHttpBaseUrl = fmt.Sprintf("http://%s.%s.svc.cluster.local:%s", svcName, tcfg.Namespace, svcPort)
		}
	} else {
		upstreamHttpBaseUrl = fmt.Sprintf("http://127.0.0.1:%s", svcPort)
	}

	args := []string{
		fmt.Sprintf("--upstream-http-base-url=%s", upstreamHttpBaseUrl),
		fmt.Sprintf("--stargate-address=%s", llmRequestRouterAddress),
		fmt.Sprintf("--inference-server-id=%s", instanceID),
		fmt.Sprintf("--auth-token-file=%s", llmWorkerTokenPath),
		"--backend-connectivity=reverse",
		"--initial-input-tps=100",
	}
	// NVCF stores 1 when a deployment leaves maxRequestConcurrency unset, so a
	// value of 1 cannot be told apart from the default. As an engine limit it
	// would admit one request at a time, so only a larger value is passed.
	if ls.MaxRequestConcurrency > 1 {
		args = append(args, fmt.Sprintf("--max-engine-concurrency=%d", ls.MaxRequestConcurrency))
	}
	if healthPath := upstreamHealthPath(allEnvSet); healthPath != "" {
		args = append(args, fmt.Sprintf("--upstream-health-path=%s", healthPath))
	}
	if tcfg.StargateQUICInsecure {
		args = append(args, "--quic-insecure")
	}
	readinessProbe := llmWorkerReadinessProbe(allEnvSet)
	if readinessProbe != nil {
		// A Pylon without /ready rejects the flag and exits at startup, instead
		// of staying NotReady until INFERENCE_READY_TIMEOUT.
		args = append(args, "--readiness-gate")
	}

	for _, model := range ls.Models {
		// Only include a model name if the LLM model config is present.
		if model.LLMModelConfig != nil {
			args = append(args, fmt.Sprintf("--model-name=%s", model.Name))
		}
	}

	c := corev1.Container{
		Name:            LLMWorkerContainerName,
		Image:           llmRouterClientImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Ports: []corev1.ContainerPort{{
			Name:          common.WorkerMetricsPortName,
			ContainerPort: llmMetricsPort,
			Protocol:      corev1.ProtocolTCP,
		}},
		Args:           args,
		Env:            common.SortEnvs(envs),
		ReadinessProbe: readinessProbe,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    *resource.NewMilliQuantity(500, resource.DecimalSI),
				corev1.ResourceMemory: *resource.NewQuantity(512*1<<20, resource.BinarySI),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    *resource.NewMilliQuantity(1000, resource.DecimalSI),
				corev1.ResourceMemory: *resource.NewQuantity(1024*1<<20, resource.BinarySI),
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      "llm",
				MountPath: llmDirMountPath,
			},
			// config-data backs SHARED_CONFIG_DIR, shared with the credential
			// manager. Mount it so the nvcf client does not fail creating
			// ConfigDirPath at startup.
			{
				Name:      "config-data",
				MountPath: ConfigDirPath,
			},
		},
	}

	return c, nil
}

func newLLMCredentialManagerContainer(allEnvSet map[string]string, _ TranslateConfig) (corev1.Container, error) {
	llmCredentialManagerImage := allEnvSet[llmCredentialManagerImageEnv]
	if llmCredentialManagerImage == "" {
		llmCredentialManagerImage = llmCredentialManagerImageDefault
		// return corev1.Container{}, fmt.Errorf("LLM credential manager image is not set")
	}

	envs := common.MakeWorkloadEnvVars(common.FunctionCreationAction)
	envs = append(envs,
		corev1.EnvVar{
			Name:  "NVCF_WORKER_TOKEN",
			Value: allEnvSet["NVCF_WORKER_TOKEN"],
		},
		corev1.EnvVar{
			Name:  "NVCF_FQDN_GRPC",
			Value: allEnvSet["NVCF_FQDN_GRPC"],
		},
		corev1.EnvVar{
			Name:  "FUNCTION_ID",
			Value: allEnvSet["FUNCTION_ID"],
		},
		corev1.EnvVar{
			Name:  "FUNCTION_VERSION_ID",
			Value: allEnvSet["FUNCTION_VERSION_ID"],
		},
		corev1.EnvVar{
			Name:  "NCA_ID",
			Value: allEnvSet["NCA_ID"],
		},
		corev1.EnvVar{
			Name:  "SHARED_CONFIG_DIR",
			Value: ConfigDirPath,
		},
		corev1.EnvVar{
			Name:  "WORKER_TOKEN_PATH",
			Value: llmWorkerTokenPath,
		},
	)
	volumeMounts := []corev1.VolumeMount{
		{
			Name:      "llm",
			MountPath: llmDirMountPath,
		},
		// config-data backs SHARED_CONFIG_DIR. The credential manager
		// creates and caches the worker token here, so it must be mounted.
		{
			Name:      "config-data",
			MountPath: ConfigDirPath,
		},
	}
	if allEnvSet[common.SecretsAssertionTokenEnv] != "" {
		envs = append(envs, corev1.EnvVar{
			Name:  essAssertionTokenPathEnv,
			Value: essAssertionTokenPath,
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      common.EssDataVolumeName,
			MountPath: common.EssConfigDir,
		})
	}

	c := corev1.Container{
		Name:            "llm-credential-manager",
		Image:           llmCredentialManagerImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Env:             common.SortEnvs(envs),
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    *resource.NewMilliQuantity(125, resource.DecimalSI),
				corev1.ResourceMemory: *resource.NewQuantity(64*1<<20, resource.BinarySI),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    *resource.NewMilliQuantity(250, resource.DecimalSI),
				corev1.ResourceMemory: *resource.NewQuantity(128*1<<20, resource.BinarySI),
			},
		},
		VolumeMounts: volumeMounts,
	}
	return c, nil
}

func newLLMClientVolumes() []corev1.Volume {
	return []corev1.Volume{
		{
			Name: "llm",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}
}
