package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Checkpoint contains data about devices prepared for each ResourceClaim the
// driver is responsible for. It is serialized to a versioned JSON file that can
// be read by the driver to recover intermediate state.
//
// +kubebuilder:object:root=true
type Checkpoint struct {
	metav1.TypeMeta

	PreparedClaims []PreparedClaim
}

type PreparedClaim struct {
	UID types.UID

	Jon struct{}
}
