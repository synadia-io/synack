package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// NatsUserSpec captures desired NATS User behavior in Control Plane terms.
type NatsUserSpec struct {
	// Account selection (inline AccountSelector).
	AccountSelector `json:",inline"`

	// NatsUserID optionally pins this resource to an existing Control Plane user ID.
	NatsUserID string `json:"natsUserId,omitempty"`

	// Name is the NATS user name.
	Name string `json:"name"`

	// SigningKeyGroupID is the signing key group to use for this user.
	SigningKeyGroupID string `json:"signingKeyGroupId"`

	// JwtExpiresInSecs sets the JWT expiration in seconds.
	JwtExpiresInSecs *int64 `json:"jwtExpiresInSecs,omitempty"`

	// BearerToken enables bearer token mode.
	BearerToken *bool `json:"bearerToken,omitempty"`

	// Data sets the data limit (bytes, -1 unlimited).
	Data *int64 `json:"data,omitempty"`

	// Payload sets the max message payload size (bytes, -1 unlimited).
	Payload *int64 `json:"payload,omitempty"`

	// Subs sets the max subscriptions (-1 unlimited).
	Subs *int64 `json:"subs,omitempty"`

	// AllowedConnectionTypes restricts connection types for the user.
	AllowedConnectionTypes []string `json:"allowedConnectionTypes,omitempty"`

	// Tags are user-defined tags.
	Tags []string `json:"tags,omitempty"`
}

// NatsUserStatus reflects observed state from reconciliation.
type NatsUserStatus struct {
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	NatsUserID         string `json:"natsUserId,omitempty"`
	AccountID          string `json:"accountId,omitempty"`
	UserPublicKey      string `json:"userPublicKey,omitempty"`
	LastSynced         string `json:"lastSynced,omitempty"`
	Message            string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=user
// +kubebuilder:printcolumn:name="AccountID",type="string",JSONPath=".status.accountId"
// +kubebuilder:printcolumn:name="Name",type="string",JSONPath=".spec.name"
// +kubebuilder:printcolumn:name="UserID",type="string",JSONPath=".status.natsUserId"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// NatsUser is the Schema for NATS user resources.
type NatsUser struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NatsUserSpec   `json:"spec,omitempty"`
	Status NatsUserStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NatsUserList contains a list of NatsUser.
type NatsUserList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NatsUser `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NatsUser{}, &NatsUserList{})
}
