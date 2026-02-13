package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StreamSpec captures desired Stream behavior in Control Plane terms.
type StreamSpec struct {
	// AccountID identifies the target Control Plane account ID.
	AccountID string `json:"accountId,omitempty"`

	// AccountPublicNKey identifies the target account by public NKEY.
	// When this is set, SystemID must also be set so lookup can be scoped.
	AccountPublicNKey string `json:"accountPublicNKey,omitempty"`

	// SystemID scopes AccountPublicNKey resolution to a specific system.
	SystemID string `json:"systemId,omitempty"`

	// Account is a legacy alias for AccountID.
	// Deprecated: prefer accountId.
	Account string `json:"account,omitempty"`

	// StreamID optionally pins this resource to an existing Control Plane stream ID.
	// When set, reconciliation will try this ID first before name-based lookup.
	StreamID string `json:"streamId,omitempty"`

	// Name is the JetStream stream name.
	Name string `json:"name"`

	// Subjects defines subjects associated with this stream.
	Subjects []string `json:"subjects,omitempty"`
}

// StreamStatus reflects observed state from reconciliation.
type StreamStatus struct {
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	StreamID           string `json:"streamId,omitempty"`
	LastSyncedAt       string `json:"lastSyncedAt,omitempty"`
	Message            string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=nsstream
// +kubebuilder:printcolumn:name="AccountID",type="string",JSONPath=".spec.accountId"
// +kubebuilder:printcolumn:name="AccountNKey",type="string",JSONPath=".spec.accountPublicNKey"
// +kubebuilder:printcolumn:name="Name",type="string",JSONPath=".spec.name"
// +kubebuilder:printcolumn:name="StreamID",type="string",JSONPath=".status.streamId"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// Stream is the Schema for stream resources.
type Stream struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   StreamSpec   `json:"spec,omitempty"`
	Status StreamStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// StreamList contains a list of Stream.
type StreamList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Stream `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Stream{}, &StreamList{})
}
