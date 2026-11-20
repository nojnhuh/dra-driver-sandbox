//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/yaml"
)

func TestCollectClusterDiagnostics(t *testing.T) {
	pods := corev1.PodList{
		Items: []corev1.Pod{
			{
				Status: corev1.PodStatus{
					Phase: corev1.PodPending,
					InitContainerStatuses: []corev1.ContainerStatus{
						{Name: "init", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{}}},
					},
					ContainerStatuses: []corev1.ContainerStatus{
						{Name: "running", RestartCount: 1, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
						{Name: "waiting", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff"}}},
						{Name: "crashing", RestartCount: 1, State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "CrashLoopBackOff"}}},
					},
				},
			},
		},
	}
	pods.APIVersion, pods.Kind = "v1", "PodList"
	pods.Items[0].Name, pods.Items[0].Namespace = "operator", "tigera-operator"
	var nodes corev1.NodeList
	nodes.APIVersion, nodes.Kind = "v1", "NodeList"
	var events corev1.EventList
	events.APIVersion, events.Kind = "v1", "EventList"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		var obj any
		switch r.URL.Path {
		case "/api/v1/nodes":
			obj = &nodes
		case "/api/v1/events":
			obj = &events
		case "/api/v1/pods":
			obj = &pods
		case "/api/v1/namespaces/tigera-operator/pods/operator/log":
			if r.URL.Query().Get("follow") == "true" || r.URL.Query().Get("container") == "waiting" {
				t.Error("Diagnostics must not follow logs or request logs for unstarted containers")
				http.Error(w, "unexpected log request", http.StatusBadRequest)
				return
			}
			_, _ = fmt.Fprintf(w, "%s previous=%s", r.URL.Query().Get("container"), r.URL.Query().Get("previous"))
			return
		default:
			t.Errorf("Unexpected request: %s", r.URL)
			http.NotFound(w, r)
			return
		}
		if err := json.NewEncoder(w).Encode(obj); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	client, err := kubernetes.NewForConfig(&rest.Config{Host: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	collectClusterDiagnostics(ctx, t, client)

	dir := filepath.Join(t.ArtifactDir(), "diagnostics")
	for _, name := range []string{"nodes.yaml", "events.yaml", "pods.yaml"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(filepath.Join(dir, "pods.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var collected corev1.PodList
	if err := yaml.Unmarshal(data, &collected); err != nil {
		t.Fatal(err)
	}
	if len(collected.Items) != 1 || collected.Items[0].Status.Phase != corev1.PodPending {
		t.Fatalf("Pending pod missing from diagnostics: %+v", collected.Items)
	}
	podDir := filepath.Join(dir, "pods", "tigera-operator", "operator")
	for name, want := range map[string]string{
		"init.log":              "init previous=",
		"running.log":           "running previous=",
		"running.previous.log":  "running previous=true",
		"crashing.previous.log": "crashing previous=true",
	} {
		data, err := os.ReadFile(filepath.Join(podDir, name))
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != want {
			t.Errorf("%s = %q, want %q", name, data, want)
		}
	}
	for _, name := range []string{"waiting.log", "crashing.log"} {
		if _, err := os.Stat(filepath.Join(podDir, name)); !os.IsNotExist(err) {
			t.Errorf("Unexpected log for waiting container %s: %v", name, err)
		}
	}
}
