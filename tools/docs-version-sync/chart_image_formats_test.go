// SPDX-FileCopyrightText: Copyright (c) NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"strings"
)

// imageValueFormat is how one upstream chart spells an image in its values:
// how to read the effective reference, which fields the wrapper values.yaml
// must write so the default is visible, and which fields an ordinary override
// writes to move the image.
type imageValueFormat struct {
	name string
	// resolve returns the reference the chart renders for the image object
	// node. parent is the dependency's values root and appVersion its
	// Chart.yaml appVersion.
	resolve func(parent, node map[string]any, appVersion string) string
	// visible reports a missing wrapper default for the image object.
	visible func(parent, node map[string]any) error
	// override returns the fields that render
	// <registry>/<repository>:<version>@<digest>.
	override func(registry, repository, version, digest string) map[string]any
}

// imageValueFormats selects the format by chart: the key is the first segment
// of an index path, which must be a dependency name or alias declared in the
// wrapper Chart.yaml. Any other root uses standardImageFormat.
//
// Tech debt: upstream charts spell images differently, and NVCF follows each
// upstream community's convention instead of reshaping values. Per-field shape
// detection was rejected because it hides precedence (a stray fullImageName or
// imageRegistry would silently change the rule). A format declared in the
// index was rejected because one image identity can span charts with
// different formats. Remove an entry when its chart converges on the standard
// registry/repository/tag/digest shape.
var imageValueFormats = map[string]imageValueFormat{
	// NATS: fullImageName wins, then digest replaces the tag.
	//   {registry: docker.io, repository: library/nats, tag: 2.10, fullImageName: m.example/nats:1}
	//     -> m.example/nats:1
	//   {registry: docker.io, repository: library/nats, tag: 2.10, digest: sha256:aa..}
	//     -> docker.io/library/nats@sha256:aa..
	"nats": {
		name: "nats",
		resolve: func(_, node map[string]any, _ string) string {
			if full := imageCatalogString(node, "fullImageName"); full != "" {
				return full
			}
			reference := joinImageRepository(imageCatalogString(node, "registry"), imageCatalogString(node, "repository"))
			if digest := imageCatalogString(node, "digest"); digest != "" {
				return reference + "@" + digest
			}
			return withImageVersion(reference, imageCatalogString(node, "tag"), "")
		},
		visible: func(_, node map[string]any) error {
			if imageCatalogString(node, "fullImageName") != "" {
				return nil
			}
			return requireImageFields(node, "registry", "repository")
		},
		// fullImageName and digest are cleared: Helm keeps default fields an
		// override omits, and either would still take precedence over the tag.
		override: func(registry, repository, version, digest string) map[string]any {
			fields := splitImageOverride(registry, repository, version, digest)
			fields["fullImageName"] = ""
			return fields
		},
	},
	// cert-manager (jetstack "image" helper): an explicit repository, prefixed
	// by image.registry when set, else imageRegistry/imageNamespace/name. Tag
	// and digest may both apply; with neither, the tag inherits appVersion.
	//   imageRegistry: quay.io, imageNamespace: jetstack, image: {name: cert-manager-controller}
	//     -> quay.io/jetstack/cert-manager-controller:<appVersion>
	//   image: {repository: m.example/cm, tag: v1, digest: sha256:aa..}
	//     -> m.example/cm:v1@sha256:aa..
	"cert-manager": {
		name: "cert-manager",
		resolve: func(parent, node map[string]any, appVersion string) string {
			repository := imageCatalogString(node, "repository")
			if repository == "" {
				repository = joinImageRepository(imageCatalogString(parent, "imageRegistry"),
					joinImageRepository(imageCatalogString(parent, "imageNamespace"), imageCatalogString(node, "name")))
			}
			repository = joinImageRepository(imageCatalogString(node, "registry"), repository)
			tag, digest := imageCatalogString(node, "tag"), imageCatalogString(node, "digest")
			if tag == "" && digest == "" {
				tag = appVersion
			}
			return withImageVersion(repository, tag, digest)
		},
		visible: func(parent, node map[string]any) error {
			if imageCatalogString(node, "repository") != "" {
				return nil
			}
			if err := requireImageFields(parent, "imageRegistry"); err != nil {
				return err
			}
			return requireImageFields(node, "name")
		},
		// registry is cleared so the backwards-compatible prefix cannot double
		// the registry in the full repository.
		override: func(registry, repository, version, digest string) map[string]any {
			return map[string]any{"registry": "", "repository": registry + "/" + repository, "tag": version, "digest": digest}
		},
	},
}

// standardImageFormat is registry/repository/tag/digest, with digest appended
// after the tag.
//
//	{registry: nvcr.io, repository: nvidia/x, tag: 1.0} -> nvcr.io/nvidia/x:1.0
var standardImageFormat = imageValueFormat{
	name: "standard",
	resolve: func(_, node map[string]any, _ string) string {
		repository := joinImageRepository(imageCatalogString(node, "registry"), imageCatalogString(node, "repository"))
		return withImageVersion(repository, imageCatalogString(node, "tag"), imageCatalogString(node, "digest"))
	},
	visible: func(_, node map[string]any) error {
		return requireImageFields(node, "registry", "repository")
	},
	override: splitImageOverride,
}

// imageFormatFor returns the format for an index path. A table key that the
// chart does not declare as a dependency fails, so a renamed dependency cannot
// silently fall back to the standard format.
func imageFormatFor(path string, dependencies map[string]bool) (imageValueFormat, error) {
	root, _, _ := strings.Cut(path, ".")
	format, ok := imageValueFormats[root]
	if !ok {
		return standardImageFormat, nil
	}
	if !dependencies[root] {
		return imageValueFormat{}, &imageFormatKeyError{format: format.name, root: root}
	}
	return format, nil
}

// imageFormatKeyError is a format table key the chart does not declare. It is
// a configuration error, never an expected pending-fix coverage failure.
type imageFormatKeyError struct {
	format, root string
}

func (e *imageFormatKeyError) Error() string {
	return fmt.Sprintf("image format %q applies only to a declared %s dependency or alias in Chart.yaml", e.format, e.root)
}

// splitImageOverride writes a separate registry and a tag@digest tag. digest
// is cleared because Helm keeps default fields an override omits: a default
// digest would otherwise render "<tag>@<new>@<old>".
func splitImageOverride(registry, repository, version, digest string) map[string]any {
	return map[string]any{"registry": registry, "repository": repository, "tag": version + "@" + digest, "digest": ""}
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

func requireImageFields(node map[string]any, keys ...string) error {
	for _, key := range keys {
		if imageCatalogString(node, key) == "" {
			return fmt.Errorf("%s must be written in the wrapper values.yaml", key)
		}
	}
	return nil
}
