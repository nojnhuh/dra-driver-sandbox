package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Checkpoint contains data about devices prepared for each ResourceClaim the
// driver is responsible for.
//
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type Checkpoint struct {
	metav1.TypeMeta `json:",inline"`
}
