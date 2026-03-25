package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AccountSpec captures desired Account behavior in Control Plane terms.
type AccountSpec struct {
	// SystemID identifies the Control Plane system this account belongs to.
	SystemID string `json:"systemId"`

	// Name is the human-readable account name.
	Name string `json:"name"`
}

// AccountStatus reflects observed state from reconciliation.
type AccountStatus struct {
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	AccountID          string `json:"accountId,omitempty"`
	LastSynced         string `json:"lastSynced,omitempty"`
	Message            string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=account
// +kubebuilder:printcolumn:name="System",type="string",JSONPath=".spec.systemId"
// +kubebuilder:printcolumn:name="Name",type="string",JSONPath=".spec.name"
// +kubebuilder:printcolumn:name="AccountID",type="string",JSONPath=".status.accountId"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// Account is the Schema for account resources.
type Account struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AccountSpec   `json:"spec,omitempty"`
	Status AccountStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AccountList contains a list of Account.
type AccountList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Account `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Account{}, &AccountList{})
}
