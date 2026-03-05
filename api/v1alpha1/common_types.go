package v1alpha1

// AccountRef references an Account resource in the same namespace.
type AccountRef struct {
	// Name is the referenced Account resource name in the same namespace.
	Name string `json:"name"`
}

// AccountSelector groups the fields used by any resource that targets a Control Plane account.
type AccountSelector struct {
	// AccountRef references an Account CR in the same namespace.
	// When set, the resource waits for the Account status.accountId and uses that ID.
	AccountRef *AccountRef `json:"accountRef,omitempty"`

	// AccountID identifies the target Control Plane account ID.
	AccountID string `json:"accountId,omitempty"`

	// AccountPublicNKey identifies the target account by public NKEY.
	// When this is set, SystemID must also be set so lookup can be scoped.
	AccountPublicNKey string `json:"accountPublicNKey,omitempty"`

	// SystemID scopes AccountPublicNKey resolution to a specific system.
	SystemID string `json:"systemId,omitempty"`
}

// Placement describes cluster and tag placement requirements.
type Placement struct {
	Cluster string   `json:"cluster"`
	Tags    []string `json:"tags,omitempty"`
}
