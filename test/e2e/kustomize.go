//go:build e2e

package e2e

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	capiyaml "sigs.k8s.io/cluster-api/util/yaml"
	"sigs.k8s.io/kustomize/api/krusty"
	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"
	"sigs.k8s.io/yaml"
)

func kustomizeYAML(input []byte, k *types.Kustomization) ([]unstructured.Unstructured, error) {
	kustomizeFiles := filesys.MakeFsInMemory()
	if err := kustomizeFiles.WriteFile("/resources.yaml", input); err != nil {
		return nil, err
	}
	if k.Resources == nil {
		k.Resources = []string{"/resources.yaml"}
	}

	return runKustomize(kustomizeFiles, k)
}

func kustomizeFile(path string, k *types.Kustomization) ([]unstructured.Unstructured, error) {
	kustomizeFiles := filesys.MakeFsInMemory()

	manifestData, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	err = kustomizeFiles.WriteFile("/resources.yaml", manifestData)
	if err != nil {
		return nil, fmt.Errorf("write %s to in-memory filesystem: %w", path, err)
	}
	k.Resources = append(k.Resources, "/resources.yaml")

	return runKustomize(kustomizeFiles, k)
}

func kustomizeDirectory(dirPath string, k *types.Kustomization) ([]unstructured.Unstructured, error) {
	// Copy the files on disk into an in-memory filesystem to avoid writing a
	// kustomization to disk.
	kustomizeFiles := filesys.MakeFsInMemory()

	addManifest := func(manifest os.DirEntry) error {
		path := filepath.Join(dirPath, manifest.Name())
		manifestFile, err := os.Open(path)
		if err != nil {
			return err
		}
		defer manifestFile.Close()

		manifestData, err := io.ReadAll(manifestFile)
		if err != nil {
			return fmt.Errorf("read %s: %w", path, err)
		}

		err = kustomizeFiles.WriteFile("/"+manifest.Name(), manifestData)
		if err != nil {
			return fmt.Errorf("write %s to in-memory filesystem: %w", manifest.Name(), err)
		}
		k.Resources = append(k.Resources, "/"+manifest.Name())
		return nil
	}

	manifests, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}
	for _, manifest := range manifests {
		if err := addManifest(manifest); err != nil {
			return nil, err
		}
	}

	return runKustomize(kustomizeFiles, k)
}

func runKustomize(files filesys.FileSystem, k *types.Kustomization) ([]unstructured.Unstructured, error) {
	kyaml, err := yaml.Marshal(k)
	if err != nil {
		return nil, err
	}
	if err := files.WriteFile("/kustomization.yaml", kyaml); err != nil {
		return nil, err
	}

	kustomizer := krusty.MakeKustomizer(krusty.MakeDefaultOptions())
	resMap, err := kustomizer.Run(files, "/")
	if err != nil {
		return nil, err
	}
	resYAML, err := resMap.AsYaml()
	if err != nil {
		return nil, err
	}
	u, err := capiyaml.ToUnstructured(resYAML)
	return u, err
}
