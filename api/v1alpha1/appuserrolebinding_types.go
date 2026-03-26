package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AppUserRoleBindingScope identifies the target resource type for a role assignment.
// +kubebuilder:validation:Enum=Team;System;Account;NatsUser
type AppUserRoleBindingScope string

const (
	ScopeTeam     AppUserRoleBindingScope = "Team"
	ScopeSystem   AppUserRoleBindingScope = "System"
	ScopeAccount  AppUserRoleBindingScope = "Account"
	ScopeNatsUser AppUserRoleBindingScope = "NatsUser"
)

// AppUserRoleBindingSubjectRef references a TeamServiceAccount in the same namespace.
type AppUserRoleBindingSubjectRef struct {
	// Name is the referenced TeamServiceAccount resource name in the same namespace.
	Name string `json:"name"`
}

// AppUserRoleBindingTargetRef references a resource CR in the same namespace.
type AppUserRoleBindingTargetRef struct {
	// Name is the referenced resource name in the same namespace.
	Name string `json:"name"`
}

// AppUserRoleBindingSpec captures desired state for a scoped role assignment.
type AppUserRoleBindingSpec struct {
	// SubjectRef references a TeamServiceAccount CR in the same namespace.
	// When set, the resource waits for the TeamServiceAccount status and uses its teamAppUserId.
	SubjectRef *AppUserRoleBindingSubjectRef `json:"subjectRef,omitempty"`

	// TeamAppUserID identifies the team app user directly. Mutually exclusive with SubjectRef.
	// Use this for pre-existing service accounts or human users.
	TeamAppUserID string `json:"teamAppUserId,omitempty"`

	// Scope identifies the target resource type.
	// +kubebuilder:validation:Required
	Scope AppUserRoleBindingScope `json:"scope"`

	// TargetRef references a resource CR in the same namespace whose status ID will be used.
	// For Team scope, this references a Team CR. For Account scope, this references an Account CR.
	TargetRef *AppUserRoleBindingTargetRef `json:"targetRef,omitempty"`

	// TargetID identifies the target resource directly by Control Plane ID.
	// Mutually exclusive with TargetRef.
	TargetID string `json:"targetId,omitempty"`

	// RoleID is the Control Plane role to assign.
	// +kubebuilder:validation:Required
	RoleID string `json:"roleId"`
}

// AppUserRoleBindingStatus reflects observed state from reconciliation.
type AppUserRoleBindingStatus struct {
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	Bound              bool   `json:"bound,omitempty"`
	TeamAppUserID      string `json:"teamAppUserId,omitempty"`
	TargetID           string `json:"targetId,omitempty"`
	LastAppliedRoleID  string `json:"lastAppliedRoleId,omitempty"`
	LastSynced         string `json:"lastSynced,omitempty"`
	Message            string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=aurb
// +kubebuilder:printcolumn:name="Scope",type="string",JSONPath=".spec.scope"
// +kubebuilder:printcolumn:name="TargetID",type="string",JSONPath=".status.targetId"
// +kubebuilder:printcolumn:name="RoleID",type="string",JSONPath=".spec.roleId"
// +kubebuilder:printcolumn:name="Bound",type="boolean",JSONPath=".status.bound"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// AppUserRoleBinding is the Schema for app user role binding resources.
type AppUserRoleBinding struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AppUserRoleBindingSpec   `json:"spec,omitempty"`
	Status AppUserRoleBindingStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// AppUserRoleBindingList contains a list of AppUserRoleBinding.
type AppUserRoleBindingList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AppUserRoleBinding `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AppUserRoleBinding{}, &AppUserRoleBindingList{})
}
