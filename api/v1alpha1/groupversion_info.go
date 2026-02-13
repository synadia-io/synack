// Package v1alpha1 contains API Schema definitions for synack v1alpha1.
// +kubebuilder:object:generate=true
// +groupName=synack.synadia.io
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

var (
	// GroupVersion is group version used to register these objects.
	GroupVersion = schema.GroupVersion{Group: "synack.synadia.io", Version: "v1alpha1"}

	// SchemeBuilder is used to add go types to the GroupVersionKind scheme.
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}

	// AddToScheme adds all registered types to the scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)
