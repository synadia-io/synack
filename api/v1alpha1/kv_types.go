package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// KeyValueSpec captures desired KeyValue behavior in Control Plane terms.
type KeyValueSpec struct {
	AccountSelector `json:",inline"`

	// KeyValueID optionally pins this resource to an existing Control Plane KV bucket ID.
	KeyValueID string `json:"keyValueId,omitempty"`

	// Bucket is the KV bucket name.
	// +kubebuilder:validation:Required
	Bucket string `json:"bucket"`

	// Description is a human readable description.
	Description string `json:"description,omitempty"`

	// History controls number of historical values per key.
	History int `json:"history,omitempty"`

	// TTL is a Go duration string controlling max age, for example "24h".
	TTL string `json:"ttl,omitempty"`

	// MaxBytes limits total bytes for the bucket.
	MaxBytes int `json:"maxBytes,omitempty"`

	// MaxValueSize limits value size in bytes.
	MaxValueSize int `json:"maxValueSize,omitempty"`

	// Storage controls storage backend: file or memory.
	Storage string `json:"storage,omitempty"`

	// Replicas controls replica count.
	Replicas int `json:"replicas,omitempty"`

	// Compression enables S2 compression when true.
	Compression bool `json:"compression,omitempty"`

	// Placement controls cluster and tag placement requirements.
	Placement *Placement `json:"placement,omitempty"`

	// RePublish configures republish behavior.
	RePublish *RePublish `json:"republish,omitempty"`

	// Mirror configures a mirror source.
	Mirror *StreamSource `json:"mirror,omitempty"`

	// Sources defines stream sources for the KV bucket.
	Sources []StreamSource `json:"sources,omitempty"`
}

// KeyValueStatus reflects observed state from reconciliation.
type KeyValueStatus struct {
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	KeyValueID         string `json:"keyValueId,omitempty"`
	LastSynced         string `json:"lastSynced,omitempty"`
	Message            string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=kv
// +kubebuilder:printcolumn:name="Bucket",type="string",JSONPath=".spec.bucket"
// +kubebuilder:printcolumn:name="KeyValueID",type="string",JSONPath=".status.keyValueId"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// KeyValue is the Schema for KeyValue resources.
type KeyValue struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KeyValueSpec   `json:"spec,omitempty"`
	Status KeyValueStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// KeyValueList contains a list of KeyValue.
type KeyValueList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []KeyValue `json:"items"`
}

func init() {
	SchemeBuilder.Register(&KeyValue{}, &KeyValueList{})
}
