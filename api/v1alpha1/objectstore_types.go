package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ObjectStoreSpec captures desired Object Store behavior in Control Plane terms.
type ObjectStoreSpec struct {
	AccountSelector `json:",inline"`

	// ObjectStoreID optionally pins this resource to an existing Control Plane object store ID.
	ObjectStoreID string `json:"ObjectStoreId,omitempty"`

	// Bucket is the object store bucket name.
	// +kubebuilder:validation:Required
	Bucket string `json:"bucket"`

	// Description is a human readable description.
	Description string `json:"description,omitempty"`

	// TTL is a Go duration string controlling max age, for example "24h".
	TTL string `json:"ttl,omitempty"`

	// MaxBytes limits total bytes for the bucket.
	MaxBytes int `json:"maxBytes,omitempty"`

	// Storage controls storage backend: file or memory.
	Storage string `json:"storage,omitempty"`

	// Replicas controls replica count.
	Replicas int `json:"replicas,omitempty"`

	// Compression enables S2 compression when true.
	Compression bool `json:"compression,omitempty"`

	// Placement controls cluster and tag placement requirements.
	Placement *Placement `json:"placement,omitempty"`

	// Metadata stores arbitrary bucket metadata.
	Metadata map[string]string `json:"metadata,omitempty"`
}

// ObjectStoreStatus reflects observed state from reconciliation.
type ObjectStoreStatus struct {
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	ObjectStoreID      string `json:"ObjectStoreId,omitempty"`
	LastSynced         string `json:"lastSynced,omitempty"`
	Message            string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=objectstore
// +kubebuilder:printcolumn:name="Bucket",type="string",JSONPath=".spec.bucket"
// +kubebuilder:printcolumn:name="ObjectStoreID",type="string",JSONPath=".status.ObjectStoreId"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// ObjectStore is the Schema for object store resources.
type ObjectStore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ObjectStoreSpec   `json:"spec,omitempty"`
	Status ObjectStoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ObjectStoreList contains a list of ObjectStore.
type ObjectStoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ObjectStore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ObjectStore{}, &ObjectStoreList{})
}
