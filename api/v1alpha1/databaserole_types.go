/*
Copyright 2026 Tran Dinh Loc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// DatabaseRoleSpec defines the desired state of DatabaseRole
type DatabaseRoleSpec struct {
	// RoleName is the name of the database role
	// +required
	RoleName string `json:"roleName"`
	// Password is the password of the database role
	// +required
	Password corev1.SecretKeySelector `json:"password"`
	// database cluster reference
	// +required
	DatabaseClusterRef DatabaseClusterRef `json:"databaseClusterRef"`

	// permissions of the database role
	// +optional
	Permissions []DatabasePermission `json:"permissions,omitempty"`
}

type DatabasePermission struct {
	// Database is the name of the database
	// +required
	Database string `json:"database"`
	// schema permissions of the database role on the database
	// +optional
	Schemas []SchemaPermission `json:"schemas,omitempty"`
}

type SchemaPermission struct {
	Name string `json:"name"`
	// +kubebuilder:validation:Optional

	Privileges []SchemaPrivilege `json:"privileges,omitempty"` // e.g., ["CREATE", "USAGE"]
	// +kubebuilder:validation:Optional
	Tables []TablePermission `json:"tables,omitempty"`
}

type TablePermission struct {
	Name string `json:"name"`
	// +kubebuilder:validation:Optional
	Privileges []TablePrivilege `json:"privileges,omitempty"` // e.g., ["SELECT", "INSERT", "UPDATE", "DELETE"]
}

// +kubebuilder:validation:Enum=CREATE;USAGE
type SchemaPrivilege = string

const (
	SchemaPrivilegeCreate SchemaPrivilege = "CREATE"
	SchemaPrivilegeUsage  SchemaPrivilege = "USAGE"
)

// +kubebuilder:validation:Enum=SELECT;INSERT;UPDATE;DELETE;TRUNCATE;REFERENCES;TRIGGER;CREATE;TEMPORARY;EXECUTE;USAGE;ALL
type TablePrivilege = string

const (
	TablePrivilegeSelect     TablePrivilege = "SELECT"
	TablePrivilegeInsert     TablePrivilege = "INSERT"
	TablePrivilegeUpdate     TablePrivilege = "UPDATE"
	TablePrivilegeDelete     TablePrivilege = "DELETE"
	TablePrivilegeTruncate   TablePrivilege = "TRUNCATE"
	TablePrivilegeReferences TablePrivilege = "REFERENCES"
	TablePrivilegeTrigger    TablePrivilege = "TRIGGER"
	TablePrivilegeCreate     TablePrivilege = "CREATE"
	TablePrivilegeTemporary  TablePrivilege = "TEMPORARY"
	TablePrivilegeExecute    TablePrivilege = "EXECUTE"
	TablePrivilegeUsage      TablePrivilege = "USAGE"
	TablePrivilegeAll        TablePrivilege = "ALL"
)

// DatabaseRoleStatus defines the observed state of DatabaseRole.
type DatabaseRoleStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the DatabaseRole resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// DatabaseRole is the Schema for the databaseroles API
type DatabaseRole struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of DatabaseRole
	// +required
	Spec DatabaseRoleSpec `json:"spec"`

	// status defines the observed state of DatabaseRole
	// +optional
	Status DatabaseRoleStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// DatabaseRoleList contains a list of DatabaseRole
type DatabaseRoleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []DatabaseRole `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DatabaseRole{}, &DatabaseRoleList{})
}
