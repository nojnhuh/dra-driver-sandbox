//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"sigs.k8s.io/kind/pkg/log"
	"sigs.k8s.io/yaml"
)

func collectClusterDiagnostics(ctx context.Context, t *testing.T, client kubernetes.Interface) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Minute)
	defer cancel()

	dir := filepath.Join(t.ArtifactDir(), "diagnostics")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Error("Error creating diagnostics directory:", err)
		return
	}
	writeYAML := func(name string, obj any) {
		data, err := yaml.Marshal(obj)
		if err != nil {
			t.Errorf("Error marshaling %s: %v", name, err)
			return
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0644); err != nil {
			t.Errorf("Error writing %s: %v", name, err)
		}
	}

	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Error("Error collecting Nodes:", err)
	} else {
		writeYAML("nodes.yaml", nodes)
	}
	events, err := client.CoreV1().Events("").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Error("Error collecting Events:", err)
	} else {
		writeYAML("events.yaml", events)
	}
	pods, err := client.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Error("Error collecting Pods:", err)
		return
	}
	writeYAML("pods.yaml", pods)
	for _, pod := range pods.Items {
		podDir := filepath.Join(dir, "pods", pod.Namespace, pod.Name)
		if err := os.MkdirAll(podDir, 0755); err != nil {
			t.Errorf("Error creating log directory for Pod %s/%s: %v", pod.Namespace, pod.Name, err)
			continue
		}
		for _, container := range append(pod.Status.InitContainerStatuses, pod.Status.ContainerStatuses...) {
			for _, previous := range []bool{false, true} {
				if previous && container.RestartCount == 0 {
					continue
				}
				if !previous && container.State.Running == nil && container.State.Terminated == nil {
					continue
				}
				name := container.Name + ".log"
				if previous {
					name = container.Name + ".previous.log"
				}
				data, err := client.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, &corev1.PodLogOptions{
					Container: container.Name,
					Previous:  previous,
				}).DoRaw(ctx)
				if err != nil {
					t.Errorf("Error collecting Pod %s/%s log %s: %v", pod.Namespace, pod.Name, name, err)
					continue
				}
				if err := os.WriteFile(filepath.Join(podDir, name), data, 0644); err != nil {
					t.Errorf("Error writing Pod %s/%s log %s: %v", pod.Namespace, pod.Name, name, err)
				}
			}
		}
	}
}

type kindLogAdapter struct {
	klog.Logger
}

// Error implements [log.Logger].
func (k kindLogAdapter) Error(message string) {
	helper, logger := k.WithCallStackHelper()
	helper()
	logger.Error(nil, strings.TrimSpace(message))
}

// Errorf implements [log.Logger].
func (k kindLogAdapter) Errorf(format string, args ...any) {
	helper, logger := k.WithCallStackHelper()
	helper()
	logger.Error(nil, fmt.Sprintf(strings.TrimSpace(format), args...))
}

// Warn implements [log.Logger].
func (k kindLogAdapter) Warn(message string) {
	helper, logger := k.WithCallStackHelper()
	helper()
	logger.Info(strings.TrimSpace(message))
}

// Warnf implements [log.Logger].
func (k kindLogAdapter) Warnf(format string, args ...any) {
	helper, logger := k.WithCallStackHelper()
	helper()
	logger.Info(fmt.Sprintf(strings.TrimSpace(format), args...))
}

// V implements [log.Logger].
func (k kindLogAdapter) V(v log.Level) log.InfoLogger {
	return kindInfoLogAdapter{k.Logger.V(int(v))}
}

type kindInfoLogAdapter struct {
	klog.Logger
}

// Info implements [log.InfoLogger].
func (k kindInfoLogAdapter) Info(message string) {
	helper, logger := k.WithCallStackHelper()
	helper()
	logger.Info(strings.TrimSpace(message))
}

// Infof implements [log.InfoLogger].
func (k kindInfoLogAdapter) Infof(format string, args ...any) {
	helper, logger := k.WithCallStackHelper()
	helper()
	logger.Info(fmt.Sprintf(strings.TrimSpace(format), args...))
}
