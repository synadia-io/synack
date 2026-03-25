package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ConsumerStreamRef references a Stream resource in the same namespace.
type ConsumerStreamRef struct {
	// Name is the referenced Stream resource name in the same namespace.
	Name string `json:"name"`
}

// ConsumerSpec captures desired Consumer behavior in Control Plane terms.
type ConsumerSpec struct {
	// StreamRef references a Stream CR in the same namespace.
	// Mutually exclusive with StreamID.
	StreamRef *ConsumerStreamRef `json:"streamRef,omitempty"`

	// StreamID is the direct Control Plane stream ID. Mutually exclusive with StreamRef.
	StreamID string `json:"streamId,omitempty"`

	// ConsumerID optionally pins this resource to an existing Control Plane consumer ID.
	ConsumerID string `json:"consumerId,omitempty"`

	// Name is the consumer name.
	Name string `json:"name"`

	// Description is a human readable consumer description.
	Description string `json:"description,omitempty"`

	// AckPolicy controls acknowledgement policy: explicit, all, or none.
	AckPolicy string `json:"ackPolicy,omitempty"`

	// AckWait is a Go duration string for acknowledgement wait time.
	AckWait string `json:"ackWait,omitempty"`

	// DeliverPolicy controls delivery policy: all, last, last_per_subject, new, by_start_sequence, by_start_time.
	DeliverPolicy string `json:"deliverPolicy,omitempty"`

	// DurableName sets the consumer's durable name.
	DurableName string `json:"durableName,omitempty"`

	// FilterSubjects filters delivered messages by subjects.
	FilterSubjects []string `json:"filterSubjects,omitempty"`

	// InactiveThreshold is a Go duration string for consumer inactivity threshold.
	InactiveThreshold string `json:"inactiveThreshold,omitempty"`

	// MaxAckPending limits outstanding un-acked messages.
	MaxAckPending int `json:"maxAckPending,omitempty"`

	// MaxDeliver limits redelivery attempts.
	MaxDeliver int `json:"maxDeliver,omitempty"`

	// MemStorage enables in-memory storage.
	MemStorage bool `json:"memStorage,omitempty"`

	// Replicas controls consumer replica count.
	Replicas int `json:"replicas,omitempty"`

	// OptStartSeq sets the start sequence for by_start_sequence delivery.
	OptStartSeq uint64 `json:"optStartSeq,omitempty"`

	// OptStartTime is an RFC3339 timestamp for by_start_time delivery.
	OptStartTime string `json:"optStartTime,omitempty"`

	// ReplayPolicy controls replay speed: instant or original.
	ReplayPolicy string `json:"replayPolicy,omitempty"`

	// SampleFreq sets the sampling frequency.
	SampleFreq string `json:"sampleFreq,omitempty"`

	// Backoff is a list of Go duration strings for redelivery backoff.
	Backoff []string `json:"backoff,omitempty"`

	// Direct enables direct get for consumers.
	Direct bool `json:"direct,omitempty"`

	// Metadata stores arbitrary consumer metadata.
	Metadata map[string]string `json:"metadata,omitempty"`

	// --- Pull-only fields ---

	// MaxRequestBatch limits pull request batch size.
	MaxRequestBatch int `json:"maxRequestBatch,omitempty"`

	// MaxRequestMaxBytes limits pull request batch byte size.
	MaxRequestMaxBytes int `json:"maxRequestMaxBytes,omitempty"`

	// MaxRequestExpires is a Go duration string for pull request expiry.
	MaxRequestExpires string `json:"maxRequestExpires,omitempty"`

	// MaxWaiting limits concurrent pull requests.
	MaxWaiting int `json:"maxWaiting,omitempty"`

	// --- Push-only fields (deliverSubject triggers push mode) ---

	// DeliverSubject sets the push deliver subject. When set, enables push mode.
	DeliverSubject string `json:"deliverSubject,omitempty"`

	// DeliverGroup sets the push queue group.
	DeliverGroup string `json:"deliverGroup,omitempty"`

	// FlowControl enables push flow control.
	FlowControl bool `json:"flowControl,omitempty"`

	// HeadersOnly delivers only message headers in push mode.
	HeadersOnly bool `json:"headersOnly,omitempty"`

	// HeartbeatInterval is a Go duration string for push idle heartbeats.
	HeartbeatInterval string `json:"heartbeatInterval,omitempty"`

	// RateLimitBps limits push delivery rate in bits per second.
	RateLimitBps uint64 `json:"rateLimitBps,omitempty"`
}

// ConsumerStatus reflects observed state from reconciliation.
type ConsumerStatus struct {
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	ConsumerID         string `json:"consumerId,omitempty"`
	StreamID           string `json:"streamId,omitempty"`
	LastSynced         string `json:"lastSynced,omitempty"`
	Message            string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=consumer
// +kubebuilder:printcolumn:name="StreamID",type="string",JSONPath=".status.streamId"
// +kubebuilder:printcolumn:name="Name",type="string",JSONPath=".spec.name"
// +kubebuilder:printcolumn:name="ConsumerID",type="string",JSONPath=".status.consumerId"
// +kubebuilder:printcolumn:name="Push",type="boolean",JSONPath=".status.isPush"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// Consumer is the Schema for consumer resources.
type Consumer struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ConsumerSpec   `json:"spec,omitempty"`
	Status ConsumerStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ConsumerList contains a list of Consumer.
type ConsumerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Consumer `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Consumer{}, &ConsumerList{})
}
