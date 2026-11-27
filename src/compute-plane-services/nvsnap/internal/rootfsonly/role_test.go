// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package rootfsonly

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func rolePod(command, args []string) *corev1.Pod {
	return &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Command: command, Args: args}}}}
}

func TestPodRole(t *testing.T) {
	for _, tc := range []struct {
		name          string
		command, args []string
		want          string
	}{
		{"tokens", []string{"python3", "-m", "dynamo.vllm"}, []string{"--model", "m", "--disaggregation-mode", "prefill"}, "prefill"},
		{"equals", []string{"python3", "-m", "dynamo.sglang", "--disaggregation-mode=decode"}, nil, "decode"},
		{"shell string", []string{"/bin/bash", "-c"}, []string{"export X=1\nexec python3 -m dynamo.vllm --model m --disaggregation-mode decode --kv-transfer-config '{\"kv_role\":\"kv_both\"}'"}, "decode"},
		{"quoted", []string{"sh", "-c", `exec x --disaggregation-mode "prefill"`}, nil, "prefill"},
		{"legacy prefill flag", []string{"python3", "-m", "dynamo.vllm", "--is-prefill-worker"}, nil, "prefill"},
		{"legacy decode flag", nil, []string{"--model", "m", "--is-decode-worker"}, "decode"},
		{"none", []string{"vllm", "serve", "m"}, []string{"--kv-transfer-config", `{"kv_role":"kv_both"}`}, ""},
		// Rank flags are not a role: every rank of an instance shares it.
		{"rank only", []string{"vllm", "serve", "m", "--node-rank", "1", "--headless"}, nil, ""},
		{"role and rank", []string{"vllm", "serve", "m", "--disaggregation-mode", "decode", "--data-parallel-rank", "3"}, nil, "decode"},
	} {
		if got := PodRole(rolePod(tc.command, tc.args)); got != tc.want {
			t.Errorf("%s: PodRole = %q, want %q", tc.name, got, tc.want)
		}
	}
	if PodRole(nil) != "" {
		t.Error("nil pod has a role")
	}
}

// The cache identity leaves the role out (one download for both roles).
func TestRoleOutOfCacheIdentity(t *testing.T) {
	c := &HashInputComposer{CUDADriverMajor: 580}
	p := rolePod([]string{"python3", "-m", "dynamo.vllm", "--disaggregation-mode", "prefill"}, nil)
	d := rolePod([]string{"python3", "-m", "dynamo.vllm", "--disaggregation-mode", "decode"}, nil)
	if a, b := c.Compose(p, 0), c.Compose(d, 0); len(a.EngineCompatFlags) != len(b.EngineCompatFlags) {
		t.Fatalf("roles differ in cache identity: %v vs %v", a.EngineCompatFlags, b.EngineCompatFlags)
	}
}
