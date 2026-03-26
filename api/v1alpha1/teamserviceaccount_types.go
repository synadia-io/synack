package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TeamRef references a Team resource in the same namespace.
type TeamRef struct {
	// Name is the referenced Team resource name in the same namespace.
	Name string `json:"name"`
}

// TeamServiceAccountSpec captures desired state for a team-scoped service account.
type TeamServiceAccountSpec struct {
	// ServiceAccountID is the Control Plane service account ID.
	// If omitted on create the controller will create the service account and backfill the ID in status.
	ServiceAccountID string `json:"serviceAccountId,omitempty"`

	// TeamRef references a Team CR in the same namespace.
	// When set, the resource waits for the Team status.id and uses that ID.
	TeamRef *TeamRef `json:"teamRef,omitempty"`

	// TeamID identifies the target Control Plane team ID directly. Mutually exclusive with TeamRef.
	TeamID string `json:"teamId,omitempty"`

	// Name is the display name for the service account.
	// +kubebuilder:validation:Required
	Name string `json:"name"`

	// TeamRoleID is an optional baseline team-level role for this service account.
	TeamRoleID string `json:"teamRoleId,omitempty"`
}

// TeamServiceAccountStatus reflects observed state from reconciliation.
type TeamServiceAccountStatus struct {
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	ID                 string `json:"id,omitempty"`
	TeamAppUserID      string `json:"teamAppUserId,omitempty"`
	TeamID             string `json:"teamId,omitempty"`
	LastSynced         string `json:"lastSynced,omitempty"`
	Message            string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=tsa
// +kubebuilder:printcolumn:name="Name",type="string",JSONPath=".spec.name"
// +kubebuilder:printcolumn:name="ServiceAccountID",type="string",JSONPath=".status.id"
// +kubebuilder:printcolumn:name="TeamAppUserID",type="string",JSONPath=".status.teamAppUserId"
// +kubebuilder:printcolumn:name="TeamID",type="string",JSONPath=".status.teamId"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// TeamServiceAccount is the Schema for team service account resources.
type TeamServiceAccount struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TeamServiceAccountSpec   `json:"spec,omitempty"`
	Status TeamServiceAccountStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TeamServiceAccountList contains a list of TeamServiceAccount.
type TeamServiceAccountList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []TeamServiceAccount `json:"items"`
}

func init() {
	SchemeBuilder.Register(&TeamServiceAccount{}, &TeamServiceAccountList{})
}
