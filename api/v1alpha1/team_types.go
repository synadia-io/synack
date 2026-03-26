package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TeamSpec captures desired Team behavior in Control Plane terms.
type TeamSpec struct {
	// TeamID is the Control Plane team ID.
	// If omitted on create the controller will create the team and backfill the ID in status.
	TeamID string `json:"teamId,omitempty"`

	// Name is the human-readable team name.
	// +kubebuilder:validation:Required
	Name string `json:"name"`
}

// TeamStatus reflects observed state from reconciliation.
type TeamStatus struct {
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	ID                 string `json:"id,omitempty"`
	ObservedName       string `json:"observedName,omitempty"`
	LastSynced         string `json:"lastSynced,omitempty"`
	Message            string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=team
// +kubebuilder:printcolumn:name="Name",type="string",JSONPath=".spec.name"
// +kubebuilder:printcolumn:name="TeamID",type="string",JSONPath=".status.id"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// Team is the Schema for team resources.
type Team struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   TeamSpec   `json:"spec,omitempty"`
	Status TeamStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// TeamList contains a list of Team.
type TeamList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Team `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Team{}, &TeamList{})
}
