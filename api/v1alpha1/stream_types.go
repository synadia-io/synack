package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StreamSpec captures desired Stream behavior in Control Plane terms.
type StreamSpec struct {
	AccountSelector `json:",inline"`

	// StreamID optionally pins this resource to an existing Control Plane stream ID.
	// When set, reconciliation will try this ID first before name-based lookup.
	StreamID string `json:"streamId,omitempty"`

	// Name is the JetStream stream name.
	Name string `json:"name"`

	// Description is a human readable stream description.
	Description string `json:"description,omitempty"`

	// Subjects defines subjects associated with this stream.
	Subjects []string `json:"subjects,omitempty"`

	// Retention controls stream retention policy: limits, interest, or workqueue.
	Retention string `json:"retention,omitempty"`

	// MaxConsumers limits the number of consumers on this stream.
	MaxConsumers int `json:"maxConsumers,omitempty"`

	// MaxMsgsPerSubject limits messages retained per subject.
	MaxMsgsPerSubject int `json:"maxMsgsPerSubject,omitempty"`

	// MaxMsgs limits total number of messages retained.
	MaxMsgs int `json:"maxMsgs,omitempty"`

	// MaxBytes limits total bytes retained.
	MaxBytes int `json:"maxBytes,omitempty"`

	// MaxAge is a Go duration string, for example "24h".
	MaxAge string `json:"maxAge,omitempty"`

	// MaxMsgSize limits message size in bytes.
	MaxMsgSize int `json:"maxMsgSize,omitempty"`

	// Storage controls stream storage backend: file or memory.
	Storage string `json:"storage,omitempty"`

	// Discard controls overflow behavior: old or new.
	Discard string `json:"discard,omitempty"`

	// Replicas controls stream replica count.
	Replicas int `json:"replicas,omitempty"`

	// NoAck disables acknowledgements for consumers.
	NoAck bool `json:"noAck,omitempty"`

	// DuplicateWindow is a Go duration string used for duplicate suppression.
	DuplicateWindow string `json:"duplicateWindow,omitempty"`

	// Placement controls cluster and tag placement requirements.
	Placement *Placement `json:"placement,omitempty"`

	// Sources defines stream sources.
	Sources []StreamSource `json:"sources,omitempty"`

	// Compression controls stream compression. Currently "s2" is supported.
	Compression string `json:"compression,omitempty"`

	// SubjectTransform configures subject transforms for incoming messages.
	SubjectTransform *SubjectTransform `json:"subjectTransform,omitempty"`

	// RePublish configures republish behavior.
	RePublish *RePublish `json:"republish,omitempty"`

	// Sealed prevents further writes when enabled.
	Sealed bool `json:"sealed,omitempty"`

	// DenyDelete blocks message delete APIs.
	DenyDelete bool `json:"denyDelete,omitempty"`

	// DenyPurge blocks stream purge APIs.
	DenyPurge bool `json:"denyPurge,omitempty"`

	// AllowDirect enables direct get APIs.
	AllowDirect bool `json:"allowDirect,omitempty"`

	// AllowRollup enables rollup headers.
	AllowRollup bool `json:"allowRollup,omitempty"`

	// DiscardPerSubject enables discard-new-per-subject behavior.
	DiscardPerSubject bool `json:"discardPerSubject,omitempty"`

	// FirstSequence sets initial sequence for new streams.
	FirstSequence uint64 `json:"firstSequence,omitempty"`

	// Metadata stores arbitrary stream metadata.
	Metadata map[string]string `json:"metadata,omitempty"`
}

// SubjectTransform describes a source/destination subject transform.
type SubjectTransform struct {
	Source string `json:"source"`
	Dest   string `json:"dest"`
}

// StreamSource defines a source stream configuration.
type StreamSource struct {
	Name          string `json:"name"`
	OptStartSeq   int    `json:"optStartSeq,omitempty"`
	OptStartTime  string `json:"optStartTime,omitempty"`
	FilterSubject string `json:"filterSubject,omitempty"`

	ExternalAPIPrefix     string `json:"externalApiPrefix,omitempty"`
	ExternalDeliverPrefix string `json:"externalDeliverPrefix,omitempty"`

	SubjectTransforms []SubjectTransform `json:"subjectTransforms,omitempty"`
}

// RePublish configures stream republish behavior.
type RePublish struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
	HeadersOnly bool   `json:"headers_only,omitempty"`
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
