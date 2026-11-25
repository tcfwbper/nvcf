/*
SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
SPDX-License-Identifier: Apache-2.0
*/

package agent

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// A captured instance's pods are deleted together: no rank's delete waits
// for another's, so no rank is recreated (and admitted beside a running
// pod of the instance, which no group restore admits) while another still
// runs. Each delete is retried on its own; pods of other instances stay.
func TestDeleteInstance_RanksTogether(t *testing.T) {
	c, kc, leader := groupFixture(t)
	r1, err := kc.CoreV1().Pods("fn").Get(context.Background(), "kimi-1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	started := 0
	together := true
	prev := deleteMember
	deleteMember = func(a *Agent, ctx context.Context, ns, name string, uid types.UID) error {
		mu.Lock()
		started++
		mu.Unlock()
		// Each delete waits for the other one to have been issued.
		for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			mu.Lock()
			n := started
			mu.Unlock()
			if n >= 2 {
				return prev(a, ctx, ns, name, uid)
			}
		}
		mu.Lock()
		together = false
		mu.Unlock()
		return prev(a, ctx, ns, name, uid)
	}
	t.Cleanup(func() { deleteMember = prev })

	c.deleteInstance(context.Background(), "k1", "", []*corev1.Pod{leader, r1}, c.a.log.WithField("t", 1))

	if !together {
		t.Error("one rank's delete waited for another's before both were issued")
	}
	if left := remainingPods(t, kc); !slices.Equal(left, []string{"other-0"}) {
		t.Errorf("pods left %v, want only the other instance's other-0", left)
	}
}
