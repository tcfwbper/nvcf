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

package operator

import (
	"bytes"
	"context"
	"crypto/x509"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	v1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/util/retry"

	nvidiaiov1 "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/apis/nvcf/v1"
	"github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/operator/internal/kubeclients"
	nvcaoptypes "github.com/dsx-ai-factory/nvcf/src/compute-plane-services/nvca/pkg/operator/types"
)

// createGate holds back the first n creates of one Secret until all n callers have
// arrived, so every caller has read the Secret as absent before any create lands.
// The fake clientset serializes reactors under a single lock, so the gate has to sit
// in front of the client instead.
type createGate struct {
	secretName string
	n          int
	mu         sync.Mutex
	arrived    int
	release    chan struct{}
}

func newCreateGate(secretName string, n int) *createGate {
	return &createGate{secretName: secretName, n: n, release: make(chan struct{})}
}

func (g *createGate) wait(name string) {
	if name != g.secretName {
		return
	}
	g.mu.Lock()
	g.arrived++
	gated := g.arrived <= g.n
	if g.arrived == g.n {
		close(g.release)
	}
	g.mu.Unlock()
	if gated {
		select {
		case <-g.release:
		case <-time.After(30 * time.Second):
		}
	}
}

type gatedClientset struct {
	kubernetes.Interface
	gate *createGate
}

func (c gatedClientset) CoreV1() corev1client.CoreV1Interface {
	return gatedCoreV1{CoreV1Interface: c.Interface.CoreV1(), gate: c.gate}
}

type gatedCoreV1 struct {
	corev1client.CoreV1Interface
	gate *createGate
}

func (c gatedCoreV1) Secrets(namespace string) corev1client.SecretInterface {
	return gatedSecrets{SecretInterface: c.CoreV1Interface.Secrets(namespace), gate: c.gate}
}

type gatedSecrets struct {
	corev1client.SecretInterface
	gate *createGate
}

func (s gatedSecrets) Create(ctx context.Context, sec *v1.Secret, opts metav1.CreateOptions) (*v1.Secret, error) {
	s.gate.wait(sec.Name)
	return s.SecretInterface.Create(ctx, sec, opts)
}

// Two cold-start reconciles of the same backend that both read the certificate as
// absent must converge on one CA: the stored serving cert has to verify against the
// CA Secret and every published caBundle, and neither reconcile may fail.
func TestConcurrentColdStartWebhookCertsConverge(t *testing.T) {
	const runs = 10

	for run := 0; run < runs; run++ {
		ctx := newTestContext()
		_, nb := newWebhookCertTestCache()
		bc := &BackendK8sCache{clients: &kubeclients.KubeClients{K8s: gatedClientset{
			Interface: fake.NewSimpleClientset(),
			gate:      newCreateGate(NVCAWebhookTLSCertSecretName, 2),
		}}}
		now := time.Now().UTC()

		errs := make([]error, 2)
		var wg sync.WaitGroup
		for i := range errs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				errs[i] = publishWebhookCert(ctx, bc, nb, now)
			}()
		}
		wg.Wait()

		for i, err := range errs {
			require.NoError(t, err, "run %d, reconcile %d", run, i)
		}
		require.NoError(t, webhookCAConsistency(ctx, bc, nb, now), "run %d", run)
	}
}

// publishWebhookCert runs the certificate part of setupNVCAAgentInfra.
func publishWebhookCert(ctx context.Context, bc *BackendK8sCache, nb *nvidiaiov1.NVCFBackend, now time.Time) error {
	cert, err := bc.ensureWebhookCert(ctx, nb, now)
	if err != nil {
		return err
	}
	if err := bc.setupWebhookSecrets(ctx, nb, cert); err != nil {
		return err
	}
	if err := bc.setupMiniServiceValidatingWebhook(ctx, nb, cert); err != nil {
		return err
	}
	return bc.setupNVCAMutatingWebhookConfiguration(ctx, nb, cert)
}

// webhookCAConsistency returns an error unless the stored serving cert verifies
// against the CA Secret and every caBundle published for the NVCA webhooks.
func webhookCAConsistency(ctx context.Context, bc *BackendK8sCache, nb *nvidiaiov1.NVCFBackend, now time.Time) error {
	ns := getSystemNamespace(nb)
	k8s := bc.clients.K8s

	tlsSecret, err := k8s.CoreV1().Secrets(ns).Get(ctx, NVCAWebhookTLSCertSecretName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	servingCert, err := parseCertPEM(tlsSecret.Data[TLSCertName])
	if err != nil {
		return err
	}
	caSecret, err := k8s.CoreV1().Secrets(ns).Get(ctx, NVCAWebhookTLSCASecretName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	bundles := map[string][]byte{"secret " + NVCAWebhookTLSCASecretName: caSecret.Data[TLSCAName]}
	vwc, err := k8s.AdmissionregistrationV1().ValidatingWebhookConfigurations().Get(ctx, nvcaoptypes.NVCAModuleName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	for _, w := range vwc.Webhooks {
		bundles["validating webhook "+w.Name] = w.ClientConfig.CABundle
	}
	mwc, err := k8s.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(ctx, nvcaoptypes.NVCAModuleName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	for _, w := range mwc.Webhooks {
		bundles["mutating webhook "+w.Name] = w.ClientConfig.CABundle
	}

	var problems []string
	for source, pemBytes := range bundles {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pemBytes) {
			problems = append(problems, source+": no parseable CA")
			continue
		}
		if err := verifyWebhookServingCert(servingCert, roots, nb, now); err != nil {
			problems = append(problems, fmt.Sprintf("%s: %v", source, err))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("serving cert does not verify: %s", strings.Join(problems, "; "))
	}
	return nil
}

// When another writer replaces the stored certificate between this reconcile's read
// and its write, the preconditioned update conflicts and the reconcile must adopt the
// other writer's certificate rather than overwrite it.
func TestEnsureWebhookCert_AdoptsCertOnUpdateConflict(t *testing.T) {
	ctx := newTestContext()
	bc, nb := newWebhookCertTestCache()
	now := time.Now().UTC()

	expired, err := generateWebhookCerts(nb, now.AddDate(-2, 0, 0))
	require.NoError(t, err)
	require.NoError(t, bc.setupWebhookSecrets(ctx, nb, expired))
	winner, err := generateWebhookCerts(nb, now)
	require.NoError(t, err)

	cs := bc.clients.K8s.(*fake.Clientset)
	var conflicted atomic.Bool
	cs.PrependReactor("update", "secrets", func(a k8stesting.Action) (bool, runtime.Object, error) {
		sec := a.(k8stesting.UpdateAction).GetObject().(*v1.Secret)
		if sec.Name != NVCAWebhookTLSCertSecretName || conflicted.Swap(true) {
			return false, nil, nil
		}
		require.NoError(t, cs.Tracker().Update(v1.SchemeGroupVersion.WithResource("secrets"),
			newTLSCertSecret(nb, winner), sec.Namespace))
		return true, nil, k8serrors.NewConflict(schema.GroupResource{Resource: "secrets"}, sec.Name,
			fmt.Errorf("the object has been modified"))
	})

	got, err := bc.ensureWebhookCert(ctx, nb, now)
	require.NoError(t, err)
	assert.True(t, conflicted.Load())
	assert.Equal(t, winner, got)
}

// hideFirstGet makes the first get of resource report NotFound, as if another writer
// created the object right after this reconcile read it.
func hideFirstGet(cs *fake.Clientset, resource, name string) {
	var hidden atomic.Bool
	cs.PrependReactor("get", resource, func(a k8stesting.Action) (bool, runtime.Object, error) {
		if a.(k8stesting.GetAction).GetName() != name || hidden.Swap(true) {
			return false, nil, nil
		}
		return true, nil, k8serrors.NewNotFound(schema.GroupResource{Resource: resource}, name)
	})
}

// conflictFirstUpdate makes the first update of resource fail with Conflict, as if
// another writer changed the object right after this reconcile read it.
func conflictFirstUpdate(cs *fake.Clientset, resource, name string) *atomic.Bool {
	var conflicted atomic.Bool
	cs.PrependReactor("update", resource, func(a k8stesting.Action) (bool, runtime.Object, error) {
		obj := a.(k8stesting.UpdateAction).GetObject().(metav1.Object)
		if obj.GetName() != name || conflicted.Swap(true) {
			return false, nil, nil
		}
		return true, nil, k8serrors.NewConflict(schema.GroupResource{Resource: resource}, name,
			fmt.Errorf("the object has been modified"))
	})
	return &conflicted
}

// updateWebhookCABundles sets caBundle on every webhook in the NVCA validating and
// mutating webhook configurations, as a manual edit or an earlier operator could.
func (bc *BackendK8sCache) updateWebhookCABundles(ctx context.Context, caBundle []byte) error {
	admission := bc.clients.K8s.AdmissionregistrationV1()
	name := nvcaoptypes.NVCAModuleName

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		vwc, err := admission.ValidatingWebhookConfigurations().Get(ctx, name, metav1.GetOptions{})
		if k8serrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		for i := range vwc.Webhooks {
			vwc.Webhooks[i].ClientConfig.CABundle = caBundle
		}
		_, err = admission.ValidatingWebhookConfigurations().Update(ctx, vwc, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return fmt.Errorf("update %v ValidatingWebhookConfiguration: %w", name, err)
	}

	err = retry.RetryOnConflict(retry.DefaultRetry, func() error {
		mwc, err := admission.MutatingWebhookConfigurations().Get(ctx, name, metav1.GetOptions{})
		if k8serrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		for i := range mwc.Webhooks {
			mwc.Webhooks[i].ClientConfig.CABundle = caBundle
		}
		_, err = admission.MutatingWebhookConfigurations().Update(ctx, mwc, metav1.UpdateOptions{})
		return err
	})
	if err != nil {
		return fmt.Errorf("update %v MutatingWebhookConfiguration: %w", name, err)
	}
	return nil
}

func assertWebhookCABundles(ctx context.Context, t *testing.T, bc *BackendK8sCache, want []byte) {
	t.Helper()
	admission := bc.clients.K8s.AdmissionregistrationV1()
	vwc, err := admission.ValidatingWebhookConfigurations().Get(ctx, nvcaoptypes.NVCAModuleName, metav1.GetOptions{})
	require.NoError(t, err)
	for _, w := range vwc.Webhooks {
		assert.Equal(t, want, w.ClientConfig.CABundle, w.Name)
	}
	mwc, err := admission.MutatingWebhookConfigurations().Get(ctx, nvcaoptypes.NVCAModuleName, metav1.GetOptions{})
	require.NoError(t, err)
	for _, w := range mwc.Webhooks {
		assert.Equal(t, want, w.ClientConfig.CABundle, w.Name)
	}
}

// A Secret that appears between the read and the create must be updated to the
// desired content rather than reported as an AlreadyExists error.
func TestCreateOrUpdateSecret_RecoversFromAlreadyExists(t *testing.T) {
	ctx := newTestContext()
	bc, nb := newWebhookCertTestCache()
	now := time.Now().UTC()
	cs := bc.clients.K8s.(*fake.Clientset)

	stale, err := generateWebhookCerts(nb, now)
	require.NoError(t, err)
	desired, err := generateWebhookCerts(nb, now)
	require.NoError(t, err)
	require.NoError(t, bc.setupTLSCASecret(ctx, nb, stale))

	hideFirstGet(cs, "secrets", NVCAWebhookTLSCASecretName)
	require.NoError(t, bc.setupTLSCASecret(ctx, nb, desired))

	caSecret, err := cs.CoreV1().Secrets(getSystemNamespace(nb)).Get(ctx, NVCAWebhookTLSCASecretName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, desired.CACertBytes, caSecret.Data[TLSCAName])
}

// A webhook configuration that appears between the read and the create must be
// updated to the desired caBundle rather than left stale behind an AlreadyExists error.
func TestCreateOrUpdateWebhookConfiguration_RecoversFromAlreadyExists(t *testing.T) {
	ctx := newTestContext()
	bc, nb := newWebhookCertTestCache()
	now := time.Now().UTC()
	cs := bc.clients.K8s.(*fake.Clientset)

	stale, err := generateWebhookCerts(nb, now)
	require.NoError(t, err)
	desired, err := generateWebhookCerts(nb, now)
	require.NoError(t, err)
	require.NoError(t, bc.setupMiniServiceValidatingWebhook(ctx, nb, stale))
	require.NoError(t, bc.setupNVCAMutatingWebhookConfiguration(ctx, nb, stale))

	hideFirstGet(cs, "validatingwebhookconfigurations", nvcaoptypes.NVCAModuleName)
	hideFirstGet(cs, "mutatingwebhookconfigurations", nvcaoptypes.NVCAModuleName)
	require.NoError(t, bc.setupMiniServiceValidatingWebhook(ctx, nb, desired))
	require.NoError(t, bc.setupNVCAMutatingWebhookConfiguration(ctx, nb, desired))

	assertWebhookCABundles(ctx, t, bc, desired.CACertBytes)
}

// An update that conflicts with another writer must re-read the live object and
// apply the desired caBundle rather than fail.
func TestCreateOrUpdateWebhookConfiguration_RetriesOnConflict(t *testing.T) {
	ctx := newTestContext()
	bc, nb := newWebhookCertTestCache()
	now := time.Now().UTC()
	cs := bc.clients.K8s.(*fake.Clientset)

	stale, err := generateWebhookCerts(nb, now)
	require.NoError(t, err)
	desired, err := generateWebhookCerts(nb, now)
	require.NoError(t, err)
	require.NoError(t, bc.setupMiniServiceValidatingWebhook(ctx, nb, stale))
	require.NoError(t, bc.setupNVCAMutatingWebhookConfiguration(ctx, nb, stale))

	vConflicted := conflictFirstUpdate(cs, "validatingwebhookconfigurations", nvcaoptypes.NVCAModuleName)
	mConflicted := conflictFirstUpdate(cs, "mutatingwebhookconfigurations", nvcaoptypes.NVCAModuleName)
	require.NoError(t, bc.setupMiniServiceValidatingWebhook(ctx, nb, desired))
	require.NoError(t, bc.setupNVCAMutatingWebhookConfiguration(ctx, nb, desired))

	assert.True(t, vConflicted.Load())
	assert.True(t, mConflicted.Load())
	assertWebhookCABundles(ctx, t, bc, desired.CACertBytes)
}

// Rotating a certificate that is about to expire must publish the new CA to the
// webhook caBundles, not only to the Secrets, and leave the rest of each webhook
// configuration untouched.
func TestRotateTLSCert_UpdatesWebhookCABundles(t *testing.T) {
	ctx := newTestContext()
	bc, nb := newWebhookCertTestCache()
	now := time.Now().UTC()
	issuedAt := now.AddDate(-1, 0, 10)
	require.NoError(t, publishWebhookCert(ctx, bc, nb, issuedAt))

	admission := bc.clients.K8s.AdmissionregistrationV1()
	vwcBefore, err := admission.ValidatingWebhookConfigurations().Get(ctx, nvcaoptypes.NVCAModuleName, metav1.GetOptions{})
	require.NoError(t, err)
	mwcBefore, err := admission.MutatingWebhookConfigurations().Get(ctx, nvcaoptypes.NVCAModuleName, metav1.GetOptions{})
	require.NoError(t, err)
	oldCA := vwcBefore.Webhooks[0].ClientConfig.CABundle

	require.NoError(t, bc.rotateTLSCert(ctx, nb, now))
	require.NoError(t, webhookCAConsistency(ctx, bc, nb, now))

	vwcAfter, err := admission.ValidatingWebhookConfigurations().Get(ctx, nvcaoptypes.NVCAModuleName, metav1.GetOptions{})
	require.NoError(t, err)
	mwcAfter, err := admission.MutatingWebhookConfigurations().Get(ctx, nvcaoptypes.NVCAModuleName, metav1.GetOptions{})
	require.NoError(t, err)
	newCA := vwcAfter.Webhooks[0].ClientConfig.CABundle
	assert.NotEqual(t, oldCA, newCA)

	require.Len(t, vwcAfter.Webhooks, len(vwcBefore.Webhooks))
	for i := range vwcBefore.Webhooks {
		vwcBefore.Webhooks[i].ClientConfig.CABundle = newCA
		assert.Equal(t, vwcBefore.Webhooks[i], vwcAfter.Webhooks[i])
	}
	require.Len(t, mwcAfter.Webhooks, len(mwcBefore.Webhooks))
	for i := range mwcBefore.Webhooks {
		mwcBefore.Webhooks[i].ClientConfig.CABundle = newCA
		assert.Equal(t, mwcBefore.Webhooks[i], mwcAfter.Webhooks[i])
	}
}

// A rotation that loses the claim on the serving cert Secret must publish the
// certificate the other writer stored rather than overwrite it with its own.
func TestRotateTLSCert_AdoptsCertOnUpdateConflict(t *testing.T) {
	ctx := newTestContext()
	bc, nb := newWebhookCertTestCache()
	now := time.Now().UTC()
	require.NoError(t, publishWebhookCert(ctx, bc, nb, now.AddDate(-1, 0, 10)))
	winner, err := generateWebhookCerts(nb, now)
	require.NoError(t, err)

	cs := bc.clients.K8s.(*fake.Clientset)
	var conflicted atomic.Bool
	cs.PrependReactor("update", "secrets", func(a k8stesting.Action) (bool, runtime.Object, error) {
		sec := a.(k8stesting.UpdateAction).GetObject().(*v1.Secret)
		if sec.Name != NVCAWebhookTLSCertSecretName || conflicted.Swap(true) {
			return false, nil, nil
		}
		require.NoError(t, cs.Tracker().Update(v1.SchemeGroupVersion.WithResource("secrets"),
			newTLSCertSecret(nb, winner), sec.Namespace))
		return true, nil, k8serrors.NewConflict(schema.GroupResource{Resource: "secrets"}, sec.Name,
			fmt.Errorf("the object has been modified"))
	})

	require.NoError(t, bc.rotateTLSCert(ctx, nb, now))
	assert.True(t, conflicted.Load())
	require.NoError(t, webhookCAConsistency(ctx, bc, nb, now))
	assertWebhookCABundles(ctx, t, bc, winner.CACertBytes)
}

// A cluster left with webhooks trusting a different CA than the one that signed
// the stored serving cert must have the stored CA republished, while the stored
// cert itself is kept.
func TestRepairWebhookCAMismatch_RepublishesStoredCA(t *testing.T) {
	ctx := newTestContext()
	bc, nb := newWebhookCertTestCache()
	now := time.Now().UTC()
	require.NoError(t, publishWebhookCert(ctx, bc, nb, now))
	other, err := generateWebhookCerts(nb, now)
	require.NoError(t, err)
	require.NoError(t, bc.updateWebhookCABundles(ctx, other.CACertBytes))
	require.Error(t, webhookCAConsistency(ctx, bc, nb, now))

	secrets := bc.clients.K8s.CoreV1().Secrets(getSystemNamespace(nb))
	before, err := secrets.Get(ctx, NVCAWebhookTLSCertSecretName, metav1.GetOptions{})
	require.NoError(t, err)

	repaired, err := bc.repairWebhookCAMismatch(ctx, nb, now)
	require.NoError(t, err)
	assert.True(t, repaired)
	require.NoError(t, webhookCAConsistency(ctx, bc, nb, now))
	assertWebhookCABundles(ctx, t, bc, before.Data[TLSCAName])

	after, err := secrets.Get(ctx, NVCAWebhookTLSCertSecretName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, before.Data, after.Data)
}

// A caBundle edited by hand on a single webhook must be detected and repaired.
func TestRepairWebhookCAMismatch_RepairsHandEditedCABundle(t *testing.T) {
	ctx := newTestContext()
	bc, nb := newWebhookCertTestCache()
	now := time.Now().UTC()
	require.NoError(t, publishWebhookCert(ctx, bc, nb, now))
	other, err := generateWebhookCerts(nb, now)
	require.NoError(t, err)

	mwcs := bc.clients.K8s.AdmissionregistrationV1().MutatingWebhookConfigurations()
	mwc, err := mwcs.Get(ctx, nvcaoptypes.NVCAModuleName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Greater(t, len(mwc.Webhooks), 1)
	last := len(mwc.Webhooks) - 1
	mwc.Webhooks[last].ClientConfig.CABundle = other.CACertBytes
	_, err = mwcs.Update(ctx, mwc, metav1.UpdateOptions{})
	require.NoError(t, err)

	repaired, err := bc.repairWebhookCAMismatch(ctx, nb, now)
	require.NoError(t, err)
	assert.True(t, repaired)
	require.NoError(t, webhookCAConsistency(ctx, bc, nb, now))
}

// Consistent certs and caBundles must be left alone, without any writes.
func TestRepairWebhookCAMismatch_NoOpWhenConsistent(t *testing.T) {
	ctx := newTestContext()
	bc, nb := newWebhookCertTestCache()
	now := time.Now().UTC()
	require.NoError(t, publishWebhookCert(ctx, bc, nb, now))
	cs := bc.clients.K8s.(*fake.Clientset)
	cs.ClearActions()

	repaired, err := bc.repairWebhookCAMismatch(ctx, nb, now)
	require.NoError(t, err)
	assert.False(t, repaired)
	for _, a := range cs.Actions() {
		assert.Equal(t, "get", a.GetVerb(), "unexpected %s of %s", a.GetVerb(), a.GetResource().Resource)
	}
}

// When the stored cert is itself unusable, the repair must replace it and publish
// the replacement's CA, rather than republish a CA for a cert that can't be served.
func TestRepairWebhookCAMismatch_ReplacesUnusableCert(t *testing.T) {
	ctx := newTestContext()
	bc, nb := newWebhookCertTestCache()
	now := time.Now().UTC()
	require.NoError(t, publishWebhookCert(ctx, bc, nb, now.AddDate(-2, 0, 0)))

	repaired, err := bc.repairWebhookCAMismatch(ctx, nb, now)
	require.NoError(t, err)
	assert.True(t, repaired)
	require.NoError(t, webhookCAConsistency(ctx, bc, nb, now))
}

// Before setup has published any webhook configuration there is nothing to repair,
// and the repair must not create cert material on its own.
func TestRepairWebhookCAMismatch_SkipsBeforeWebhooksArePublished(t *testing.T) {
	ctx := newTestContext()
	bc, nb := newWebhookCertTestCache()

	repaired, err := bc.repairWebhookCAMismatch(ctx, nb, time.Now().UTC())
	require.NoError(t, err)
	assert.False(t, repaired)
	_, err = bc.clients.K8s.CoreV1().Secrets(getSystemNamespace(nb)).
		Get(ctx, NVCAWebhookTLSCertSecretName, metav1.GetOptions{})
	assert.True(t, k8serrors.IsNotFound(err))
}

// rotateDuringFirstUpdate simulates a rotation that stores a new serving cert and
// publishes its CA right after the repair read resource, so the repair's first
// update of it conflicts. It returns whether the rotation ran and whether any update
// that landed afterwards still carried staleCA.
func rotateDuringFirstUpdate(t *testing.T, cs *fake.Clientset, nb *nvidiaiov1.NVCFBackend, rotated WebhookCert,
	resource string, staleCA []byte) (rotatedDuringRepair, staleAfterRotation *atomic.Bool) {
	rotatedDuringRepair, staleAfterRotation = &atomic.Bool{}, &atomic.Bool{}
	cs.PrependReactor("update", "*", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if !rotatedDuringRepair.Load() {
			return false, nil, nil
		}
		switch obj := a.(k8stesting.UpdateAction).GetObject().(type) {
		case *v1.Secret:
			if obj.Name == NVCAWebhookTLSCASecretName && bytes.Equal(obj.Data[TLSCAName], staleCA) {
				staleAfterRotation.Store(true)
			}
		case *admissionregistrationv1.ValidatingWebhookConfiguration:
			for _, w := range obj.Webhooks {
				if bytes.Equal(w.ClientConfig.CABundle, staleCA) {
					staleAfterRotation.Store(true)
				}
			}
		case *admissionregistrationv1.MutatingWebhookConfiguration:
			for _, w := range obj.Webhooks {
				if bytes.Equal(w.ClientConfig.CABundle, staleCA) {
					staleAfterRotation.Store(true)
				}
			}
		}
		return false, nil, nil
	})
	cs.PrependReactor("update", resource, func(a k8stesting.Action) (bool, runtime.Object, error) {
		if rotatedDuringRepair.Swap(true) {
			return false, nil, nil
		}
		ns := getSystemNamespace(nb)
		secrets := v1.SchemeGroupVersion.WithResource("secrets")
		require.NoError(t, cs.Tracker().Update(secrets, newTLSCertSecret(nb, rotated), ns))
		require.NoError(t, cs.Tracker().Update(secrets, newTLSCASecret(nb, rotated.CACertBytes), ns))
		vwcRes := admissionregistrationv1.SchemeGroupVersion.WithResource("validatingwebhookconfigurations")
		vwcObj, err := cs.Tracker().Get(vwcRes, "", nvcaoptypes.NVCAModuleName)
		require.NoError(t, err)
		vwc := vwcObj.(*admissionregistrationv1.ValidatingWebhookConfiguration).DeepCopy()
		for i := range vwc.Webhooks {
			vwc.Webhooks[i].ClientConfig.CABundle = rotated.CACertBytes
		}
		require.NoError(t, cs.Tracker().Update(vwcRes, vwc, ""))
		mwcRes := admissionregistrationv1.SchemeGroupVersion.WithResource("mutatingwebhookconfigurations")
		mwcObj, err := cs.Tracker().Get(mwcRes, "", nvcaoptypes.NVCAModuleName)
		require.NoError(t, err)
		mwc := mwcObj.(*admissionregistrationv1.MutatingWebhookConfiguration).DeepCopy()
		for i := range mwc.Webhooks {
			mwc.Webhooks[i].ClientConfig.CABundle = rotated.CACertBytes
		}
		require.NoError(t, cs.Tracker().Update(mwcRes, mwc, ""))
		name := a.(k8stesting.UpdateAction).GetObject().(metav1.Object).GetName()
		return true, nil, k8serrors.NewConflict(schema.GroupResource{Resource: resource}, name,
			fmt.Errorf("the object has been modified"))
	})
	return rotatedDuringRepair, staleAfterRotation
}

// A rotation that publishes a new CA while the repair is republishing the previous
// one must never be overwritten by the previous CA, so the caBundles are correct as
// soon as the repair returns, with no later pass needed.
func TestRepairWebhookCAMismatch_NeverOverwritesConcurrentRotation(t *testing.T) {
	ctx := newTestContext()
	bc, nb := newWebhookCertTestCache()
	now := time.Now().UTC()
	require.NoError(t, publishWebhookCert(ctx, bc, nb, now))
	stored, err := bc.clients.K8s.CoreV1().Secrets(getSystemNamespace(nb)).
		Get(ctx, NVCAWebhookTLSCertSecretName, metav1.GetOptions{})
	require.NoError(t, err)
	other, err := generateWebhookCerts(nb, now)
	require.NoError(t, err)
	require.NoError(t, bc.updateWebhookCABundles(ctx, other.CACertBytes))
	rotated, err := generateWebhookCerts(nb, now)
	require.NoError(t, err)

	cs := bc.clients.K8s.(*fake.Clientset)
	rotatedDuringRepair, staleAfterRotation := rotateDuringFirstUpdate(t, cs, nb, rotated,
		"validatingwebhookconfigurations", stored.Data[TLSCAName])

	repaired, err := bc.repairWebhookCAMismatch(ctx, nb, now)
	require.NoError(t, err)
	assert.True(t, repaired)
	assert.True(t, rotatedDuringRepair.Load())
	assert.False(t, staleAfterRotation.Load(), "the previous CA was published after the rotation")
	require.NoError(t, webhookCAConsistency(ctx, bc, nb, now))
	assertWebhookCABundles(ctx, t, bc, rotated.CACertBytes)
}

// The same holds for the CA Secret: a rotation that stores a new CA while the repair
// is rewriting a stale CA Secret must not be overwritten by the previous CA.
func TestRepairWebhookCAMismatch_NeverOverwritesConcurrentRotationOfCASecret(t *testing.T) {
	ctx := newTestContext()
	bc, nb := newWebhookCertTestCache()
	now := time.Now().UTC()
	require.NoError(t, publishWebhookCert(ctx, bc, nb, now))
	stored, err := bc.clients.K8s.CoreV1().Secrets(getSystemNamespace(nb)).
		Get(ctx, NVCAWebhookTLSCertSecretName, metav1.GetOptions{})
	require.NoError(t, err)
	other, err := generateWebhookCerts(nb, now)
	require.NoError(t, err)
	require.NoError(t, bc.setupTLSCASecret(ctx, nb, other))
	rotated, err := generateWebhookCerts(nb, now)
	require.NoError(t, err)

	cs := bc.clients.K8s.(*fake.Clientset)
	rotatedDuringRepair, staleAfterRotation := rotateDuringFirstUpdate(t, cs, nb, rotated,
		"secrets", stored.Data[TLSCAName])

	repaired, err := bc.repairWebhookCAMismatch(ctx, nb, now)
	require.NoError(t, err)
	assert.True(t, repaired)
	assert.True(t, rotatedDuringRepair.Load())
	assert.False(t, staleAfterRotation.Load(), "the previous CA was published after the rotation")
	require.NoError(t, webhookCAConsistency(ctx, bc, nb, now))
}

// storeCertInTracker writes cert to both webhook cert Secrets directly through the
// tracker, as another writer would, so it can be called from a reactor.
func storeCertInTracker(t *testing.T, cs *fake.Clientset, nb *nvidiaiov1.NVCFBackend, cert WebhookCert) {
	ns := getSystemNamespace(nb)
	secrets := v1.SchemeGroupVersion.WithResource("secrets")
	require.NoError(t, cs.Tracker().Update(secrets, newTLSCertSecret(nb, cert), ns))
	require.NoError(t, cs.Tracker().Update(secrets, newTLSCASecret(nb, cert.CACertBytes), ns))
}

// A rotation that stored a new cert and stopped before publishing its CA leaves the
// caBundles on the previous CA; the repair must publish the new one.
func TestRepairWebhookCAMismatch_RepairsInterruptedRotation(t *testing.T) {
	ctx := newTestContext()
	bc, nb := newWebhookCertTestCache()
	now := time.Now().UTC()
	require.NoError(t, publishWebhookCert(ctx, bc, nb, now))
	rotated, err := generateWebhookCerts(nb, now)
	require.NoError(t, err)
	require.NoError(t, bc.setupWebhookSecrets(ctx, nb, rotated))
	require.Error(t, webhookCAConsistency(ctx, bc, nb, now))

	repaired, err := bc.repairWebhookCAMismatch(ctx, nb, now)
	require.NoError(t, err)
	assert.True(t, repaired)
	require.NoError(t, webhookCAConsistency(ctx, bc, nb, now))
	assertWebhookCABundles(ctx, t, bc, rotated.CACertBytes)
}

// When the serving cert rotates after the repair found only the CA Secret stale, the
// repair must still publish the new CA to the caBundles.
func TestRepairWebhookCAMismatch_PublishesBundlesWhenCertRotatesAfterCheck(t *testing.T) {
	ctx := newTestContext()
	bc, nb := newWebhookCertTestCache()
	now := time.Now().UTC()
	require.NoError(t, publishWebhookCert(ctx, bc, nb, now))
	other, err := generateWebhookCerts(nb, now)
	require.NoError(t, err)
	require.NoError(t, bc.setupTLSCASecret(ctx, nb, other))
	rotated, err := generateWebhookCerts(nb, now)
	require.NoError(t, err)

	cs := bc.clients.K8s.(*fake.Clientset)
	var rotatedDuringRepair atomic.Bool
	cs.PrependReactor("update", "secrets", func(a k8stesting.Action) (bool, runtime.Object, error) {
		sec := a.(k8stesting.UpdateAction).GetObject().(*v1.Secret)
		if sec.Name != NVCAWebhookTLSCASecretName || rotatedDuringRepair.Swap(true) {
			return false, nil, nil
		}
		storeCertInTracker(t, cs, nb, rotated)
		return true, nil, k8serrors.NewConflict(schema.GroupResource{Resource: "secrets"}, sec.Name,
			fmt.Errorf("the object has been modified"))
	})

	repaired, err := bc.repairWebhookCAMismatch(ctx, nb, now)
	require.NoError(t, err)
	assert.True(t, repaired)
	assert.True(t, rotatedDuringRepair.Load())
	require.NoError(t, webhookCAConsistency(ctx, bc, nb, now))
	assertWebhookCABundles(ctx, t, bc, rotated.CACertBytes)
}

// Two rotations that both found the cert expiring can overlap on different
// replicas. A rotation that stored its cert and was delayed while another stored and
// published a newer one must not then publish its own, older CA.
func TestRotateTLSCert_DelayedRotationDoesNotOverwriteNewerCA(t *testing.T) {
	ctx := newTestContext()
	bc, nb := newWebhookCertTestCache()
	now := time.Now().UTC()
	require.NoError(t, publishWebhookCert(ctx, bc, nb, now.AddDate(-1, 0, 10)))
	newer, err := generateWebhookCerts(nb, now)
	require.NoError(t, err)

	cs := bc.clients.K8s.(*fake.Clientset)
	var overlapped atomic.Bool
	cs.PrependReactor("get", "validatingwebhookconfigurations", func(a k8stesting.Action) (bool, runtime.Object, error) {
		if overlapped.Swap(true) {
			return false, nil, nil
		}
		storeCertInTracker(t, cs, nb, newer)
		res := admissionregistrationv1.SchemeGroupVersion.WithResource("validatingwebhookconfigurations")
		obj, err := cs.Tracker().Get(res, "", nvcaoptypes.NVCAModuleName)
		require.NoError(t, err)
		vwc := obj.(*admissionregistrationv1.ValidatingWebhookConfiguration).DeepCopy()
		for i := range vwc.Webhooks {
			vwc.Webhooks[i].ClientConfig.CABundle = newer.CACertBytes
		}
		require.NoError(t, cs.Tracker().Update(res, vwc, ""))
		res = admissionregistrationv1.SchemeGroupVersion.WithResource("mutatingwebhookconfigurations")
		obj, err = cs.Tracker().Get(res, "", nvcaoptypes.NVCAModuleName)
		require.NoError(t, err)
		mwc := obj.(*admissionregistrationv1.MutatingWebhookConfiguration).DeepCopy()
		for i := range mwc.Webhooks {
			mwc.Webhooks[i].ClientConfig.CABundle = newer.CACertBytes
		}
		require.NoError(t, cs.Tracker().Update(res, mwc, ""))
		return false, nil, nil
	})

	require.NoError(t, bc.rotateTLSCert(ctx, nb, now))
	assert.True(t, overlapped.Load())
	require.NoError(t, webhookCAConsistency(ctx, bc, nb, now))
	assertWebhookCABundles(ctx, t, bc, newer.CACertBytes)
}

// A CA Secret that no longer holds the CA of a usable serving cert must be rewritten
// even when every caBundle already matches, and the serving cert must be kept.
func TestRepairWebhookCAMismatch_RewritesStaleCASecret(t *testing.T) {
	ctx := newTestContext()
	bc, nb := newWebhookCertTestCache()
	now := time.Now().UTC()
	require.NoError(t, publishWebhookCert(ctx, bc, nb, now))
	other, err := generateWebhookCerts(nb, now)
	require.NoError(t, err)
	require.NoError(t, bc.setupTLSCASecret(ctx, nb, other))
	require.Error(t, webhookCAConsistency(ctx, bc, nb, now))

	secrets := bc.clients.K8s.CoreV1().Secrets(getSystemNamespace(nb))
	before, err := secrets.Get(ctx, NVCAWebhookTLSCertSecretName, metav1.GetOptions{})
	require.NoError(t, err)

	repaired, err := bc.repairWebhookCAMismatch(ctx, nb, now)
	require.NoError(t, err)
	assert.True(t, repaired)
	require.NoError(t, webhookCAConsistency(ctx, bc, nb, now))
	after, err := secrets.Get(ctx, NVCAWebhookTLSCertSecretName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, before.Data, after.Data)
}

// A replacement that fails to write the CA Secret must report the error and leave
// the mismatch detectable, so the next sync finishes the repair.
func TestRepairWebhookCAMismatch_RetriesAfterFailedCASecretWrite(t *testing.T) {
	ctx := newTestContext()
	bc, nb := newWebhookCertTestCache()
	now := time.Now().UTC()
	require.NoError(t, publishWebhookCert(ctx, bc, nb, now.AddDate(-2, 0, 0)))

	cs := bc.clients.K8s.(*fake.Clientset)
	var failed atomic.Bool
	cs.PrependReactor("update", "secrets", func(a k8stesting.Action) (bool, runtime.Object, error) {
		sec := a.(k8stesting.UpdateAction).GetObject().(*v1.Secret)
		if sec.Name != NVCAWebhookTLSCASecretName || failed.Swap(true) {
			return false, nil, nil
		}
		return true, nil, k8serrors.NewInternalError(fmt.Errorf("etcd unavailable"))
	})

	_, err := bc.repairWebhookCAMismatch(ctx, nb, now)
	require.Error(t, err)
	require.True(t, failed.Load())
	require.Error(t, webhookCAConsistency(ctx, bc, nb, now))

	repaired, err := bc.repairWebhookCAMismatch(ctx, nb, now)
	require.NoError(t, err)
	assert.True(t, repaired)
	require.NoError(t, webhookCAConsistency(ctx, bc, nb, now))
}

// A newly minted certificate is stored together with its CA in the serving cert
// Secret, so the pair can never be written half by one reconcile and half by another.
func TestEnsureWebhookCert_StoresCAWithServingCert(t *testing.T) {
	ctx := newTestContext()
	bc, nb := newWebhookCertTestCache()
	now := time.Now().UTC()

	got, err := bc.ensureWebhookCert(ctx, nb, now)
	require.NoError(t, err)

	tlsSecret, err := bc.clients.K8s.CoreV1().Secrets(getSystemNamespace(nb)).
		Get(ctx, NVCAWebhookTLSCertSecretName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, got.TLSCert, tlsSecret.Data[TLSCertName])
	assert.Equal(t, got.TLSKey, tlsSecret.Data[TLSKeyName])
	assert.Equal(t, got.CACertBytes, tlsSecret.Data[TLSCAName])
}

// Secrets written before the CA was stored alongside the serving cert carry it only
// in the CA Secret; they must still be reused, and are upgraded on the next publish.
func TestEnsureWebhookCert_ReusesLegacySecretsWithoutCAKey(t *testing.T) {
	ctx := newTestContext()
	bc, nb := newWebhookCertTestCache()
	now := time.Now().UTC()
	ns := getSystemNamespace(nb)

	stored, err := generateWebhookCerts(nb, now)
	require.NoError(t, err)
	_, err = bc.clients.K8s.CoreV1().Secrets(ns).Create(ctx, &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: NVCAWebhookTLSCertSecretName, Namespace: ns},
		Data:       map[string][]byte{TLSCertName: stored.TLSCert, TLSKeyName: stored.TLSKey},
		Type:       v1.SecretTypeTLS,
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, bc.setupTLSCASecret(ctx, nb, stored))

	got, err := bc.ensureWebhookCert(ctx, nb, now)
	require.NoError(t, err)
	assert.Equal(t, stored, got)

	require.NoError(t, publishWebhookCert(ctx, bc, nb, now))
	tlsSecret, err := bc.clients.K8s.CoreV1().Secrets(ns).Get(ctx, NVCAWebhookTLSCertSecretName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, stored.CACertBytes, tlsSecret.Data[TLSCAName])
	require.NoError(t, webhookCAConsistency(ctx, bc, nb, now))
}
