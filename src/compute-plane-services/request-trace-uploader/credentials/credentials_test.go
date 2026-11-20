// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package credentials

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testIDKey     = "client_id"
	testSecretKey = "client_secret"
	testID        = "id-6b1f0c"
	testSecret    = "s3cr3t-value-9d2e"
)

func writeSecrets(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write secrets file: %v", err)
	}
}

func secretsPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "secrets.json")
}

func validSecrets() string {
	return `{"` + testIDKey + `":"` + testID + `","` + testSecretKey + `":"` + testSecret + `","unrelated":{"nested":true}}`
}

func TestDefaultConfigIsValid(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.File != DefaultFile {
		t.Fatalf("File = %q, want %q", cfg.File, DefaultFile)
	}
	if cfg.RefreshInterval != DefaultRefreshInterval {
		t.Fatalf("RefreshInterval = %v, want %v", cfg.RefreshInterval, DefaultRefreshInterval)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
}

func TestConfigValidateRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{"empty file", Config{File: "", RefreshInterval: time.Minute}},
		{"relative file", Config{File: "secrets.json", RefreshInterval: time.Minute}},
		{"zero interval", Config{File: "/var/secrets/secrets.json"}},
		{"interval below minimum", Config{File: "/var/secrets/secrets.json", RefreshInterval: MinRefreshInterval - time.Millisecond}},
		{"interval above maximum", Config{File: "/var/secrets/secrets.json", RefreshInterval: MaxRefreshInterval + time.Second}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cfg.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want error")
			}
		})
	}
}

func TestOpenReadsNamedKeysFromFile(t *testing.T) {
	path := secretsPath(t)
	writeSecrets(t, path, validSecrets())

	loader, err := Open(Config{File: path, RefreshInterval: time.Minute}, testIDKey, testSecretKey)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	for key, want := range map[string]string{testIDKey: testID, testSecretKey: testSecret} {
		got, ok := loader.Get(key)
		if !ok {
			t.Fatalf("Get(%q) ok = false, want true", key)
		}
		if got.Reveal() != want {
			t.Fatalf("Get(%q) = %q, want %q", key, got.Reveal(), want)
		}
	}
	if got, ok := loader.Get("unrelated"); ok || got.Reveal() != "" {
		t.Fatalf("Get(unrequested key) = %q, %v; want empty and false", got.Reveal(), ok)
	}
}

func TestOpenFailsWithoutUsableFile(t *testing.T) {
	tests := []struct {
		name    string
		content *string
		wantErr string
	}{
		{name: "missing file", content: nil, wantErr: "read secrets file"},
		{name: "missing key", content: ptr(`{"` + testIDKey + `":"` + testID + `"}`), wantErr: testSecretKey},
		{name: "empty value", content: ptr(`{"` + testIDKey + `":"` + testID + `","` + testSecretKey + `":""}`), wantErr: testSecretKey},
		{name: "non-string value", content: ptr(`{"` + testIDKey + `":"` + testID + `","` + testSecretKey + `":42}`), wantErr: testSecretKey},
		{name: "not an object", content: ptr(`["` + testSecret + `"]`), wantErr: "parse secrets file"},
		{name: "truncated", content: ptr(`{"` + testIDKey + `":"` + testID + `","` + testSecretKey + `":"` + testSecret), wantErr: "parse secrets file"},
		{name: "empty file", content: ptr(""), wantErr: "parse secrets file"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := secretsPath(t)
			if tt.content != nil {
				writeSecrets(t, path, *tt.content)
			}
			_, err := Open(Config{File: path, RefreshInterval: time.Minute}, testIDKey, testSecretKey)
			if err == nil {
				t.Fatal("Open() error = nil, want error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Open() error = %v, want it to mention %q", err, tt.wantErr)
			}
		})
	}
}

func TestOpenIgnoresEnvironment(t *testing.T) {
	t.Setenv(testIDKey, testID)
	t.Setenv(testSecretKey, testSecret)

	_, err := Open(Config{File: secretsPath(t), RefreshInterval: time.Minute}, testIDKey, testSecretKey)
	if err == nil {
		t.Fatal("Open() error = nil with only env values set, want missing-file error")
	}
}

func TestOpenRejectsBadKeyLists(t *testing.T) {
	path := secretsPath(t)
	writeSecrets(t, path, validSecrets())
	tests := []struct {
		name string
		keys []string
	}{
		{"no keys", nil},
		{"empty key", []string{testIDKey, ""}},
		{"duplicate key", []string{testIDKey, testIDKey}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Open(Config{File: path, RefreshInterval: time.Minute}, tt.keys...); err == nil {
				t.Fatal("Open() error = nil, want error")
			}
		})
	}
}

func TestOpenRejectsInvalidConfig(t *testing.T) {
	if _, err := Open(Config{File: "secrets.json", RefreshInterval: time.Minute}, testIDKey); err == nil {
		t.Fatal("Open() error = nil for a relative file, want error")
	}
}

func ptr(s string) *string { return &s }

func openValid(t *testing.T) (*Loader, string) {
	t.Helper()
	path := secretsPath(t)
	writeSecrets(t, path, validSecrets())
	loader, err := Open(Config{File: path, RefreshInterval: time.Minute}, testIDKey, testSecretKey)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return loader, path
}

func mustGet(t *testing.T, loader *Loader, key string) string {
	t.Helper()
	value, ok := loader.Get(key)
	if !ok {
		t.Fatalf("Get(%q) ok = false", key)
	}
	return value.Reveal()
}

func TestReloadPicksUpRotatedSecret(t *testing.T) {
	loader, path := openValid(t)
	rotated := "rotated-secret-41a7"
	writeSecrets(t, path, `{"`+testIDKey+`":"`+testID+`","`+testSecretKey+`":"`+rotated+`"}`)

	if err := loader.Reload(); err != nil {
		t.Fatalf("Reload() error = %v", err)
	}
	if got := mustGet(t, loader, testSecretKey); got != rotated {
		t.Fatalf("secret after Reload = %q, want %q", got, rotated)
	}
}

func TestReloadKeepsLastGoodValuesOnBadRead(t *testing.T) {
	tests := []struct {
		name    string
		content *string
	}{
		{"file removed", nil},
		{"half written", ptr(`{"` + testIDKey + `":"new-id","` + testSecretKey + `":"new-sec`)},
		{"empty", ptr("")},
		{"key missing", ptr(`{"` + testIDKey + `":"new-id"}`)},
		{"key emptied", ptr(`{"` + testIDKey + `":"new-id","` + testSecretKey + `":""}`)},
		// A damaged byte would be decoded as U+FFFD, giving a wrong credential.
		{"invalid UTF-8 in a value", ptr(`{"` + testIDKey + `":"new-id","` + testSecretKey + "\":\"new-sec\xffret\"}")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loader, path := openValid(t)
			if tt.content == nil {
				if err := os.Remove(path); err != nil {
					t.Fatalf("remove secrets file: %v", err)
				}
			} else {
				writeSecrets(t, path, *tt.content)
			}

			if err := loader.Reload(); err == nil {
				t.Fatal("Reload() error = nil, want error")
			}
			if got := mustGet(t, loader, testIDKey); got != testID {
				t.Fatalf("id after failed Reload = %q, want last good %q", got, testID)
			}
			if got := mustGet(t, loader, testSecretKey); got != testSecret {
				t.Fatalf("secret after failed Reload = %q, want last good %q", got, testSecret)
			}
		})
	}
}

// A reload that read the file before a rotation must not commit after a
// reload that read it afterwards, or the older credentials come back.
func TestReloadNeverLetsAnOlderReadReplaceANewerOne(t *testing.T) {
	loader, path := openValid(t)
	parked, resume := make(chan struct{}), make(chan struct{})
	var parkedOnce atomic.Bool
	loader.afterRead = func() {
		if parkedOnce.CompareAndSwap(false, true) {
			close(parked)
			<-resume
		}
	}

	var olderErr, newerErr error
	olderDone, newerDone := make(chan struct{}), make(chan struct{})
	go func() { olderErr = loader.Reload(); close(olderDone) }()
	<-parked
	got := make(chan struct{})
	go func() { loader.Get(testSecretKey); close(got) }()
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("Get waited for a reload in progress")
	}
	rotated := "rotated-secret-5e08"
	writeSecrets(t, path, `{"`+testIDKey+`":"`+testID+`","`+testSecretKey+`":"`+rotated+`"}`)
	go func() { newerErr = loader.Reload(); close(newerDone) }()
	// Unserialized, the newer reload commits while the older one is parked.
	// Serialized, it waits, so let the older one go after a short wait.
	select {
	case <-newerDone:
	case <-time.After(100 * time.Millisecond):
	}
	close(resume)
	<-olderDone
	<-newerDone

	if olderErr != nil || newerErr != nil {
		t.Fatalf("Reload() errors = %v, %v", olderErr, newerErr)
	}
	if got := mustGet(t, loader, testSecretKey); got != rotated {
		t.Fatalf("secret after both reloads = %q, want the newer %q", got, rotated)
	}
}

// Run logs Reload errors, and logs carry no file paths, so the error keeps
// only its cause.
func TestReloadErrorsKeepTheCauseWithoutThePath(t *testing.T) {
	tests := []struct {
		name   string
		breaks func(t *testing.T, path string)
		wantIs error
	}{
		{name: "file removed", breaks: removeFile, wantIs: fs.ErrNotExist},
		{name: "path is a directory", breaks: func(t *testing.T, path string) {
			removeFile(t, path)
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loader, path := openValid(t)
			tt.breaks(t, path)

			err := loader.Reload()
			if err == nil {
				t.Fatal("Reload() error = nil, want error")
			}
			if strings.Contains(err.Error(), path) {
				t.Errorf("Reload() error %q names the secrets file", err)
			}
			if !strings.Contains(err.Error(), "read secrets file") {
				t.Errorf("Reload() error %q, want it to say the read failed", err)
			}
			if tt.wantIs != nil && !errors.Is(err, tt.wantIs) {
				t.Errorf("Reload() error %q, want errors.Is %v", err, tt.wantIs)
			}
		})
	}
}

func removeFile(t *testing.T, path string) {
	t.Helper()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
}

// A backend that starts before its secrets file is mounted, or whose file is
// missing, opens with no values and picks the file up on the next Reload,
// so a missing secret is an authentication failure at runtime, not a
// process that cannot start.
func TestOpenWithAllowMissingFileStartsEmptyAndLoadsTheFileLater(t *testing.T) {
	path := secretsPath(t)
	loader, err := Open(Config{File: path, RefreshInterval: time.Minute, AllowMissingFile: true}, testIDKey, testSecretKey)
	if err != nil {
		t.Fatalf("Open() without the file error = %v, want none", err)
	}
	if got, ok := loader.Get(testIDKey); ok || got.Reveal() != "" {
		t.Fatalf("Get before the file exists = %q, %v; want empty and false", got.Reveal(), ok)
	}
	if err := loader.Reload(); err == nil {
		t.Fatal("Reload() without the file error = nil, want the read error so the caller counts an authentication failure")
	}
	writeSecrets(t, path, validSecrets())
	if err := loader.Reload(); err != nil {
		t.Fatalf("Reload() after the file appeared error = %v", err)
	}
	if got, ok := loader.Get(testSecretKey); !ok || got.Reveal() != testSecret {
		t.Fatalf("Get after the file appeared = %q, %v; want the secret", got.Reveal(), ok)
	}
}

// AllowMissingFile excuses a file that cannot be read, never one that is
// read but lacks a required key: that is still refused at Open.
func TestOpenWithAllowMissingFileStillRefusesABadFile(t *testing.T) {
	path := secretsPath(t)
	writeSecrets(t, path, `{"`+testIDKey+`":"`+testID+`"}`)
	if _, err := Open(Config{File: path, RefreshInterval: time.Minute, AllowMissingFile: true}, testIDKey, testSecretKey); err == nil || !strings.Contains(err.Error(), testSecretKey) {
		t.Fatalf("Open() error = %v, want one naming the missing key %s", err, testSecretKey)
	}
}

// A file the process cannot read yet, such as one mounted with the wrong
// permissions, is treated as missing: the fix may come without a restart.
func TestOpenWithAllowMissingFileStartsEmptyWhenTheFileCannotBeRead(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root reads a file whatever its mode")
	}
	path := secretsPath(t)
	writeSecrets(t, path, validSecrets())
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	loader, err := Open(Config{File: path, RefreshInterval: time.Minute, AllowMissingFile: true}, testIDKey, testSecretKey)
	if err != nil {
		t.Fatalf("Open() on an unreadable file error = %v, want none", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := loader.Reload(); err != nil {
		t.Fatalf("Reload() once readable error = %v", err)
	}
	if got, ok := loader.Get(testIDKey); !ok || got.Reveal() != testID {
		t.Fatalf("Get after the file became readable = %q, %v; want the id", got.Reveal(), ok)
	}
}
