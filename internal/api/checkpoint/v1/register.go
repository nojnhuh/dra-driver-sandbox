package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName is the group name for checkpoints.
const GroupName = "checkpoint.internal.sandbox.example.com"

// SchemeGroupVersion is the group version for v1 types.
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

var (
	// localSchemeBuilder is used to add types and conversion functions to the scheme.
	// The generated conversion code references this variable by name.
	localSchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme applies all the stored functions to the scheme.
	AddToScheme = localSchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Checkpoint{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
