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

package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	postgresv1alpha1 "github.com/dinhlockt02/postgres-operator/api/v1alpha1"
)

// DatabaseRoleReconciler reconciles a DatabaseRole object
type DatabaseRoleReconciler struct {
	client.Client
	Scheme              *runtime.Scheme
	Recorder            record.EventRecorder
	PgConnectionFactory PgConnectionFactory
}

// +kubebuilder:rbac:groups=postgres.databases.dinhloc.dev,resources=databaseroles,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=postgres.databases.dinhloc.dev,resources=databaseroles/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=postgres.databases.dinhloc.dev,resources=databaseroles/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the DatabaseRole object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.22.4/pkg/reconcile
func (r *DatabaseRoleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx)

	stages := []func(ctx context.Context, req ctrl.Request) (ctrl.Result, error){
		r.ensureDatabaseRole,
		r.ensurePermissionsConfigured,
	}

	for _, stage := range stages {
		if result, err := stage(ctx, req); err != nil {
			return result, err
		}
	}

	readyCondition := metav1.Condition{
		Type:    conditionReady,
		Status:  metav1.ConditionTrue,
		Reason:  "Ready",
		Message: "Database is ready",
	}

	availableCondition := metav1.Condition{
		Type:    conditionAvailable,
		Status:  metav1.ConditionTrue,
		Reason:  "Available",
		Message: "Database is available",
	}

	degradedCondition := metav1.Condition{
		Type:    conditionDegraded,
		Status:  metav1.ConditionFalse,
		Reason:  "NotDegraded",
		Message: "Database is not degraded",
	}

	var database postgresv1alpha1.DatabaseRole
	if err := r.Get(ctx, req.NamespacedName, &database); err != nil {
		return ctrl.Result{}, err
	}

	changed := false

	changed = changed || meta.SetStatusCondition(&database.Status.Conditions, readyCondition)
	changed = changed || meta.SetStatusCondition(&database.Status.Conditions, availableCondition)
	changed = changed || meta.SetStatusCondition(&database.Status.Conditions, degradedCondition)

	if changed {
		logger.Info("Updating Database status", "NamespacedName", req.Name)
		if err := r.Status().Update(ctx, &database); err != nil {
			return ctrl.Result{}, err
		}
	}

	logger.Info("Reconciled successful", "NamespacedName", req.Name)
	return ctrl.Result{}, nil
}

func (r *DatabaseRoleReconciler) ensureDatabaseRole(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx)
	// 0. Initialize the Database variables
	var (
		databaseCluster postgresv1alpha1.DatabaseCluster
		databaseRole    postgresv1alpha1.DatabaseRole

		degradedCondition = metav1.Condition{
			Type:    conditionDegraded,
			Status:  metav1.ConditionUnknown,
			Reason:  "Unknown",
			Message: "Unknown",
		}
	)
	defer func() {
		changed := meta.SetStatusCondition(&databaseRole.Status.Conditions, degradedCondition)
		if degradedCondition.Status != metav1.ConditionUnknown && changed {
			updateStatusErr := r.Status().Update(ctx, &databaseRole)
			if updateStatusErr != nil {
				logger.Error(updateStatusErr, "Failed to update DatabaseRole status")
			}
		}
	}()

	// 1. Get Role CRD
	if err := r.Get(ctx, req.NamespacedName, &databaseRole); err != nil {
		return ctrl.Result{}, err
	}

	// 2. Get CRD: DatabaseCluster
	if err := r.Get(ctx, types.NamespacedName{
		Name: databaseRole.Spec.DatabaseClusterRef.Name,
	}, &databaseCluster); err != nil {
		return ctrl.Result{}, err
	}

	// 3. Get DatabaseCluster connection
	dsn, err := getDatabaseClusterConnectionDSN(ctx, r.Client, &databaseCluster)
	if err != nil {
		degradedCondition = metav1.Condition{
			Type:    conditionDegraded,
			Status:  metav1.ConditionTrue,
			Reason:  "GetDSNFailed",
			Message: fmt.Sprintf("Failed to get DSN for cluster %v", err),
		}

		return ctrl.Result{}, err
	}

	var conn PostgreSQLConnection
	if r.PgConnectionFactory != nil {
		conn, err = r.PgConnectionFactory(dsn)
	} else {
		conn, err = NewPgConnection(dsn)
	}
	if err != nil {
		degradedCondition = metav1.Condition{
			Type:    conditionDegraded,
			Status:  metav1.ConditionTrue,
			Reason:  "GetConnectionFailed",
			Message: fmt.Sprintf("Failed to get connection to cluster %v", err),
		}

		return ctrl.Result{}, err
	}
	defer func() { _ = conn.Close(ctx) }()

	// 4. Check if the role exists
	exists, err := conn.IsRoleExist(ctx, databaseRole.Spec.RoleName)
	if err != nil {
		degradedCondition = metav1.Condition{
			Type:    conditionDegraded,
			Status:  metav1.ConditionTrue,
			Reason:  "GetRoleFailed",
			Message: fmt.Sprintf("Failed to get role %v", err),
		}

		return ctrl.Result{}, err
	}

	if !exists {
		// 1. Get password
		passwordSecret := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{Name: databaseRole.Spec.Password.Name, Namespace: databaseRole.Namespace}, passwordSecret); err != nil {
			degradedCondition = metav1.Condition{
				Type:    conditionDegraded,
				Status:  metav1.ConditionTrue,
				Reason:  "GetPasswordFailed",
				Message: fmt.Sprintf("Failed to get password %v", err),
			}

			return ctrl.Result{}, err
		}

		password, exists := passwordSecret.Data[databaseRole.Spec.Password.Key]
		if !exists {
			degradedCondition = metav1.Condition{
				Type:    conditionDegraded,
				Status:  metav1.ConditionTrue,
				Reason:  "PasswordKeyNotFound",
				Message: fmt.Sprintf("Password key %s not found in secret %s", databaseRole.Spec.Password.Key, databaseRole.Spec.Password.Name),
			}

			return ctrl.Result{}, err
		}

		err = conn.CreateRole(ctx, databaseRole.Spec.RoleName, string(password))
		if err != nil {
			degradedCondition = metav1.Condition{
				Type:    conditionDegraded,
				Status:  metav1.ConditionTrue,
				Reason:  "CreateRoleFailed",
				Message: fmt.Sprintf("Failed to create role %v", err),
			}

			return ctrl.Result{}, err
		}

		r.Recorder.Event(&databaseRole, "Normal", "Created", fmt.Sprintf("Created role %s", databaseRole.Spec.RoleName))
	}

	logger.Info("Reconciled role successfully", "role", databaseRole.Spec.RoleName)

	return ctrl.Result{}, nil
}

func (r *DatabaseRoleReconciler) ensurePermissionsConfigured(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx)
	// 0. Initialize the Database variables
	var (
		databaseCluster postgresv1alpha1.DatabaseCluster
		databaseRole    postgresv1alpha1.DatabaseRole

		degradedCondition = metav1.Condition{
			Type:    conditionDegraded,
			Status:  metav1.ConditionUnknown,
			Reason:  "Unknown",
			Message: "Unknown",
		}
	)
	defer func() {
		changed := meta.SetStatusCondition(&databaseRole.Status.Conditions, degradedCondition)
		if degradedCondition.Status != metav1.ConditionUnknown && changed {
			updateStatusErr := r.Status().Update(ctx, &databaseRole)
			if updateStatusErr != nil {
				logger.Error(updateStatusErr, "Failed to update DatabaseRole status")
			}
		}
	}()

	// 1. Get Role CRD
	if err := r.Get(ctx, req.NamespacedName, &databaseRole); err != nil {
		return ctrl.Result{}, err
	}

	// 2. Get CRD: DatabaseCluster
	if err := r.Get(ctx, types.NamespacedName{
		Name: databaseRole.Spec.DatabaseClusterRef.Name,
	}, &databaseCluster); err != nil {
		return ctrl.Result{}, err
	}

	// 3. Loop through permissions
	for _, permission := range databaseRole.Spec.Permissions {
		// 3.1. Get DatabaseCluster connection
		dsn, err := getDatabaseClusterConnectionDSN(ctx, r.Client, &databaseCluster)
		if err != nil {
			degradedCondition = metav1.Condition{
				Type:    conditionDegraded,
				Status:  metav1.ConditionTrue,
				Reason:  "GetDSNFailed",
				Message: fmt.Sprintf("Failed to get DSN for cluster %v", err),
			}

			logger.Error(err, "Failed to get DSN for cluster")
			return ctrl.Result{}, err
		}

		var conn PostgreSQLConnection
		if r.PgConnectionFactory != nil {
			conn, err = r.PgConnectionFactory(dsn)
		} else {
			conn, err = NewPgConnection(dsn)
		}
		if err != nil {
			degradedCondition = metav1.Condition{
				Type:    conditionDegraded,
				Status:  metav1.ConditionTrue,
				Reason:  "GetConnectionFailed",
				Message: fmt.Sprintf("Failed to get connection to cluster %v", err),
			}

			logger.Error(err, "Failed to get connection to cluster")
			return ctrl.Result{}, err
		}
		defer func() { _ = conn.Close(ctx) }()

		if exist, err := conn.IsDatabaseExist(ctx, permission.Database); err != nil {
			degradedCondition = metav1.Condition{
				Type:    conditionDegraded,
				Status:  metav1.ConditionTrue,
				Reason:  "GetDatabaseFailed",
				Message: fmt.Sprintf("Failed to get database %v", err),
			}

			return ctrl.Result{}, err
		} else if !exist {
			degradedCondition = metav1.Condition{
				Type:    conditionDegraded,
				Status:  metav1.ConditionTrue,
				Reason:  "DatabaseNotExists",
				Message: fmt.Sprintf("Database %s does not exist", permission.Database),
			}

			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}

		// 3.1.1. Grant connect permission
		if err := conn.GrantConnectPermission(ctx, permission.Database, databaseRole.Spec.RoleName); err != nil {
			degradedCondition = metav1.Condition{
				Type:    conditionDegraded,
				Status:  metav1.ConditionTrue,
				Reason:  "GrantConnectFailed",
				Message: fmt.Sprintf("Failed to grant connect permission %v", err),
			}

			logger.Error(err, "Failed to grant connect permission")
			return ctrl.Result{}, err
		}

		// 3.2. Loop through schemas
		for _, schemaPermission := range permission.Schemas {
			if exist, err := conn.IsSchemaExist(ctx, schemaPermission.Name); err != nil {
				degradedCondition = metav1.Condition{
					Type:    conditionDegraded,
					Status:  metav1.ConditionTrue,
					Reason:  "GetSchemaFailed",
					Message: fmt.Sprintf("Failed to get schema %v", err),
				}

				return ctrl.Result{}, err
			} else if !exist {
				degradedCondition = metav1.Condition{
					Type:    conditionDegraded,
					Status:  metav1.ConditionTrue,
					Reason:  "SchemaNotExists",
					Message: fmt.Sprintf("Schema %s does not exist", schemaPermission.Name),
				}

				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}

			// 3.2.1. Grant priviledges
			if err := conn.GrantSchemaPrivileges(ctx, schemaPermission.Name, databaseRole.Spec.RoleName, schemaPermission.Privileges); err != nil {
				degradedCondition = metav1.Condition{
					Type:    conditionDegraded,
					Status:  metav1.ConditionTrue,
					Reason:  "GrantPriviledgesFailed",
					Message: fmt.Sprintf("Failed to grant priviledges %v", err),
				}

				logger.Error(err, "Failed to grant schema priviledges")
				return ctrl.Result{}, err
			}

			for _, table := range schemaPermission.Tables {
				// 3.2.2. Grant permissions
				if err := conn.GrantTablePrivileges(ctx, table.Name, schemaPermission.Name, databaseRole.Spec.RoleName, table.Privileges); err != nil {
					degradedCondition = metav1.Condition{
						Type:    conditionDegraded,
						Status:  metav1.ConditionTrue,
						Reason:  "GrantPriviledgesFailed",
						Message: fmt.Sprintf("Failed to grant priviledges %v", err),
					}

					logger.Error(err, "Failed to grant table priviledges")
					return ctrl.Result{}, err
				}
			}
		}
	}

	logger.Info("Reconciled permissions successfully", "role", databaseRole.Spec.RoleName)
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DatabaseRoleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&postgresv1alpha1.DatabaseRole{}).
		Named("databaserole").
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 1,
			RateLimiter:             workqueue.NewTypedItemExponentialFailureRateLimiter[ctrl.Request](5*time.Second, 10*time.Second),
		}).
		Complete(r)
}
