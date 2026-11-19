// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"strings"
)

// Image values convention. An indexed path names an image object whose
// source fields are registry and repository and whose version fields are tag
// and digest. It renders <registry>/<repository>[:<tag>][@<digest>]:
//
//	{registry: nvcr.io, repository: nvidia/x, tag: 1.0} -> nvcr.io/nvidia/x:1.0
var imageSourceFields = []string{"registry", "repository"}

// imageValueException records how one upstream chart's image values differ
// from the convention. Anything not listed here follows the convention.
type imageValueException struct {
	name string
	// imageSourceFields are extra fields of the image object that select a
	// source. They must be empty in chart values, and overrides clear them.
	imageSourceFields []string
	// chartSourceFields are dotted paths under the dependency values root
	// that select a source for every image of the dependency. They must be
	// empty in chart values.
	chartSourceFields []string
	// digestReplacesTag: a digest renders <repository>@<digest> and drops the tag.
	digestReplacesTag bool
	// appVersionTag: an empty tag and digest render the dependency appVersion.
	appVersionTag bool
}

// imageValueExceptions is keyed by dependency chart name, the name field of
// a wrapper Chart.yaml dependency. An index path starts with the values root
// of that dependency: its alias when it has one, otherwise its name.
//
// Tech debt: upstream charts spell images differently, and NVCF follows each
// upstream community's convention instead of reshaping values. The key is the
// dependency rather than the image identity because every image of one
// upstream chart shares a shape, while one identity can be referenced by
// charts with different shapes. Remove a row when its upstream chart converges
// on the convention.
var imageValueExceptions = map[string]imageValueException{
	// NATS (nats-io/k8s chart): fullImageName replaces registry and
	// repository, a digest replaces the tag, and global.image.registry is the
	// registry of every image whose own registry is empty.
	//   {registry: r.example, repository: nats, tag: 2.10, digest: sha256:aa..} -> r.example/nats@sha256:aa..
	"nats": {
		name:              "nats",
		imageSourceFields: []string{"fullImageName"},
		chartSourceFields: []string{"global.image.registry"},
		digestReplacesTag: true,
	},
	// cert-manager (jetstack "image" helper): imageRegistry and imageNamespace
	// prefix image.name when image.repository is empty, and an empty tag and
	// digest inherit the vendored chart's appVersion.
	//   imageRegistry: quay.io, imageNamespace: jetstack, image: {name: controller} -> quay.io/jetstack/controller:<appVersion>
	"cert-manager": {
		name:              "cert-manager",
		chartSourceFields: []string{"imageRegistry", "imageNamespace"},
		appVersionTag:     true,
	},
}

// imageExceptionFor returns the exception for an index path, or the zero
// value for the convention. dependencies maps each declared values root (an
// alias, or the name when there is no alias) to its chart name.
//
//	cm.image, {cm: cert-manager}       -> cert-manager exception
//	nats.image, {nats: other}          -> convention: the root is another chart
//	nats.image, {messaging: nats}      -> error: values under nats never reach
//	                                      the dependency, which reads messaging
//
// A table key used as a root that the chart does not declare fails, so a
// renamed or newly aliased dependency cannot silently fall back to the
// convention.
func imageExceptionFor(path string, dependencies map[string]string) (imageValueException, error) {
	root, _, _ := strings.Cut(path, ".")
	if chart, declared := dependencies[root]; declared {
		if exception, ok := imageValueExceptions[chart]; ok {
			return exception, nil
		}
		return imageValueException{name: "convention"}, nil
	}
	if exception, ok := imageValueExceptions[root]; ok {
		return imageValueException{}, &imageExceptionKeyError{exception: exception.name, root: root}
	}
	return imageValueException{name: "convention"}, nil
}

// imageExceptionKeyError is an index root named after an exception chart that
// the chart does not declare as a values root. It is a configuration error,
// never an expected pending-fix finding.
type imageExceptionKeyError struct {
	exception, root string
}

func (e *imageExceptionKeyError) Error() string {
	return fmt.Sprintf("image value exception %q applies only to a declared %s dependency; an aliased dependency reads its values under the alias", e.exception, e.root)
}

// resolve returns the reference the chart renders for the image object node
// of a dependency with the given appVersion. Exception source fields are not
// read: they must be empty in chart values, and a scenario that sets one
// renders a reference the catalog does not declare, which fails loudly.
func (e imageValueException) resolve(node map[string]any, appVersion string) string {
	reference := joinImageRepository(imageCatalogString(node, "registry"), imageCatalogString(node, "repository"))
	tag, digest := imageCatalogString(node, "tag"), imageCatalogString(node, "digest")
	if e.digestReplacesTag && digest != "" {
		tag = ""
	}
	if e.appVersionTag && tag == "" && digest == "" {
		tag = appVersion
	}
	return withImageVersion(reference, tag, digest)
}

// override returns the fields an ordinary values file writes to render
// <registry>/<repository>:<version>@<digest>. The digest goes into the tag and
// the digest field and exception source fields are cleared, because Helm keeps
// fields an override omits: a kept digest would render "<tag>@<new>@<old>",
// and a kept fullImageName would win over the new source.
func (e imageValueException) override(registry, repository, version, digest string) map[string]any {
	fields := map[string]any{"registry": registry, "repository": repository, "tag": version + "@" + digest, "digest": ""}
	for _, field := range e.imageSourceFields {
		fields[field] = ""
	}
	return fields
}

// imageReferenceHost returns the registry host that starts a repository or
// image reference: a first path segment that contains "." or ":", or is
// "localhost". A single segment is never a host.
//
//	docker.io/library/nats -> docker.io (a registry hidden in a repository)
//	localhost:5000/x       -> localhost:5000
//	library/nats, nats     -> no host
func imageReferenceHost(reference string) (string, bool) {
	first, rest, found := strings.Cut(reference, "/")
	if !found || rest == "" {
		return "", false
	}
	if strings.ContainsAny(first, ".:") || first == "localhost" {
		return first, true
	}
	return "", false
}

func joinImageRepository(prefix, repository string) string {
	if prefix == "" {
		return repository
	}
	return prefix + "/" + repository
}

func withImageVersion(repository, tag, digest string) string {
	if tag != "" {
		repository += ":" + tag
	}
	if digest != "" {
		repository += "@" + digest
	}
	return repository
}
