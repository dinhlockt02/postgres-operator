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
	"net"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	postgresv1alpha1 "github.com/dinhlockt02/postgres-operator/api/v1alpha1"
	"github.com/jackc/pgx/v5"
)

// DatabaseReconciler reconciles a Database object
type DatabaseReconciler struct {
	client.Client
	Scheme              *runtime.Scheme
	Recorder            record.EventRecorder
	PgConnectionFactory PgConnectionFactory
}

// +kubebuilder:rbac:groups=postgres.databases.dinhloc.dev,resources=databases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=postgres.databases.dinhloc.dev,resources=databases/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=postgres.databases.dinhloc.dev,resources=databases/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Database object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.22.4/pkg/reconcile
func (r *DatabaseReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = logf.FromContext(ctx)

	logger := logf.FromContext(ctx)

	stages := []func(ctx context.Context, req ctrl.Request) (ctrl.Result, error){
		r.ensureDatabaseExists,
		r.ensureDatabaseSchemaExists,
		r.ensureDatabaseServiceExists,
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

	var database postgresv1alpha1.Database
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

func (r *DatabaseReconciler) ensureDatabaseExists(ctx context.Context, req ctrl.Request) (rs ctrl.Result, err error) {
	logger := logf.FromContext(ctx)
	// 0. Initialize the Database variables
	var (
		database        postgresv1alpha1.Database
		databaseCluster postgresv1alpha1.DatabaseCluster

		degradedCondition = metav1.Condition{
			Type:    conditionDegraded,
			Status:  metav1.ConditionUnknown,
			Reason:  "Unknown",
			Message: "Unknown",
		}
	)
	defer func() {
		changed := meta.SetStatusCondition(&database.Status.Conditions, degradedCondition)
		if degradedCondition.Status != metav1.ConditionUnknown && changed {
			updateStatusErr := r.Status().Update(ctx, &database)
			if updateStatusErr != nil {
				logger.Error(updateStatusErr, "Failed to update Database status")
			}
		}
	}()

	// 1. Get CRDs: Database and DatabaseCluster
	if err := r.Get(ctx, req.NamespacedName, &database); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if err := r.Get(ctx, types.NamespacedName{
		Name: database.Spec.DatabaseClusterRef.Name,
	}, &databaseCluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

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

	// 2. Check if the database exists
	exists, err := conn.IsDatabaseExist(ctx, database.Spec.Name)
	if err != nil {
		reason := "ListDatabaseFailed"
		message := fmt.Sprintf("Failed to list database in pg_database: %v", err)

		degradedCondition = metav1.Condition{
			Type:    conditionDegraded,
			Status:  metav1.ConditionTrue,
			Reason:  reason,
			Message: message,
		}
		r.Recorder.Event(&database, corev1.EventTypeWarning, reason, message)

		logger.Error(err, "Failed to list database in pg_database")
		return ctrl.Result{}, err
	}

	// 3. If exists, do nothing
	if exists {
		return ctrl.Result{}, nil
	}

	// 4. If not exists, create it
	if err := conn.CreateDatabase(ctx, database.Spec.Name); err != nil {
		reason := "CreateDatabaseFailed"
		message := fmt.Sprintf("Failed to create database %v: %v", database.Spec.Name, err)

		degradedCondition = metav1.Condition{
			Type:    conditionDegraded,
			Status:  metav1.ConditionTrue,
			Reason:  reason,
			Message: message,
		}

		r.Recorder.Event(&database, corev1.EventTypeWarning, reason, message)
		logger.Error(err, "Failed to create database")
		return ctrl.Result{}, err
	}

	reason := "DatabaseCreated"
	message := fmt.Sprintf("Database %v is created", database.Spec.Name)
	r.Recorder.Event(&database, corev1.EventTypeNormal, reason, message)

	return ctrl.Result{}, nil
}

func (r *DatabaseReconciler) ensureDatabaseServiceExists(ctx context.Context, req ctrl.Request) (rs ctrl.Result, err error) {
	logger := logf.FromContext(ctx)
	var (
		database        postgresv1alpha1.Database
		databaseService corev1.Service
		databaseCluster postgresv1alpha1.DatabaseCluster

		availableCondition = metav1.Condition{
			Type:    conditionAvailable,
			Status:  metav1.ConditionUnknown,
			Reason:  "Unknown",
			Message: "Unknown",
		}
	)

	defer func() {
		changed := meta.SetStatusCondition(&databaseService.Status.Conditions, availableCondition)
		if availableCondition.Status != metav1.ConditionUnknown && changed {
			updateStatusErr := r.Status().Update(ctx, &databaseService)
			if updateStatusErr != nil {
				logger.Error(updateStatusErr, "Failed to update DatabaseService status")
			}
		}
	}()

	// 1. Get CRDs: Database and DatabaseCluster
	if err := r.Get(ctx, req.NamespacedName, &database); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	if err := r.Get(ctx, types.NamespacedName{
		Name: database.Spec.DatabaseClusterRef.Name,
	}, &databaseCluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	// 2. Check if the service exists
	serviceName := database.Spec.DatabaseServiceName
	if serviceName == "" {
		serviceName = database.Name
	}

	err = r.Get(ctx, types.NamespacedName{
		Name:      serviceName,
		Namespace: database.Namespace,
	}, &databaseService)

	if err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	if apierrors.IsNotFound(err) {
		dsn, err := getDatabaseClusterConnectionDSN(ctx, r.Client, &databaseCluster)
		if err != nil {
			return ctrl.Result{}, err
		}

		err = r.createService(ctx, serviceName, database.Namespace, dsn)
		if err != nil {
			reason := "CreateServiceFailed"
			message := fmt.Sprintf("Failed to create service %v: %v", serviceName, err)

			availableCondition = metav1.Condition{
				Type:    conditionAvailable,
				Status:  metav1.ConditionFalse,
				Reason:  reason,
				Message: message,
			}

			r.Recorder.Event(&database, corev1.EventTypeWarning, reason, message)
			return ctrl.Result{}, err
		}

		reason := "CreateServiceSucceeded"
		message := fmt.Sprintf("Service %v created successfully", serviceName)
		r.Recorder.Event(&database, corev1.EventTypeNormal, reason, message)
	}

	// 3. Refetch the service
	if err := r.Get(ctx, types.NamespacedName{
		Name:      serviceName,
		Namespace: database.Namespace,
	}, &databaseService); err != nil {
		return ctrl.Result{}, err
	}

	// 4. Set ownership & add finalizer
	err = controllerutil.SetOwnerReference(&database, &databaseService, r.Scheme)
	if err != nil {
		return ctrl.Result{}, err
	}

	// 5. Update the service
	if err := r.Update(ctx, &databaseService); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *DatabaseReconciler) createService(ctx context.Context, serviceName, namespace, dsn string) error {
	parsedDsn, err := pgx.ParseConfig(dsn)
	if err != nil {
		return err
	}

	host := parsedDsn.Host
	hostIp := net.ParseIP(host)
	if hostIp == nil {
		return fmt.Errorf("host %s is not an IP", host)
	}
	port := int32(parsedDsn.Port)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: namespace,
		},
	}

	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Spec.Selector = nil
		svc.Spec.Ports = []corev1.ServicePort{
			{
				Name:       "postgres",
				Port:       5432,
				TargetPort: intstr.FromInt32(port),
			},
		}
		return nil
	})

	if err != nil {
		return err
	}

	epSlice := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: namespace,
			Labels: map[string]string{
				"kubernetes.io/service-name": serviceName,
			},
		},
	}

	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, epSlice, func() error {
		epSlice.AddressType = discoveryv1.AddressTypeIPv4
		if hostIp.To4() == nil {
			epSlice.AddressType = discoveryv1.AddressTypeIPv6
		}

		epSlice.Endpoints = []discoveryv1.Endpoint{{Addresses: []string{host}}}
		epSlice.Ports = []discoveryv1.EndpointPort{{Name: &[]string{"postgres"}[0], Port: &port}}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func (r *DatabaseReconciler) ensureDatabaseSchemaExists(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx)
	// 0. Initialize the Database variables
	var (
		database        postgresv1alpha1.Database
		databaseCluster postgresv1alpha1.DatabaseCluster

		degradedCondition = metav1.Condition{
			Type:    conditionDegraded,
			Status:  metav1.ConditionUnknown,
			Reason:  "Unknown",
			Message: "Unknown",
		}
	)
	defer func() {
		changed := meta.SetStatusCondition(&database.Status.Conditions, degradedCondition)
		if degradedCondition.Status != metav1.ConditionUnknown && changed {

			updateStatusErr := r.Status().Update(ctx, &database)
			if updateStatusErr != nil {
				logger.Error(updateStatusErr, "Failed to update Database status")
			}
		}
	}()

	// 1. Get CRDs: Database and DatabaseCluster
	if err := r.Get(ctx, req.NamespacedName, &database); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.Get(ctx, types.NamespacedName{
		Name: database.Spec.DatabaseClusterRef.Name,
	}, &databaseCluster); err != nil {
		return ctrl.Result{}, err
	}

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

		logger.Error(err, "Failed to get connection to cluster")
		return ctrl.Result{}, err
	}
	defer func() { _ = conn.Close(ctx) }()

	if err := conn.SelectDatabase(ctx, database.Spec.Name); err != nil {
		degradedCondition = metav1.Condition{
			Type:    conditionDegraded,
			Status:  metav1.ConditionTrue,
			Reason:  "SelectDatabaseFailed",
			Message: fmt.Sprintf("Failed to select database %v: %v", database.Spec.Name, err),
		}

		return ctrl.Result{}, err
	}
	// 2. Check if the database schema exists
	exists, err := conn.IsSchemaExist(ctx, database.Spec.Schema)
	if err != nil {
		reason := "ListDatabaseFailed"
		message := fmt.Sprintf("Failed to list schema %v in %v: %v", database.Spec.Schema, database.Spec.Name, err)

		degradedCondition = metav1.Condition{
			Type:    conditionDegraded,
			Status:  metav1.ConditionTrue,
			Reason:  reason,
			Message: message,
		}
		r.Recorder.Event(&database, corev1.EventTypeWarning, reason, message)

		return ctrl.Result{}, err
	}

	// 3. If exists, do nothing
	if exists {
		return ctrl.Result{}, nil
	}

	// 4. If not exists, create it
	if err := conn.CreateSchema(ctx, database.Spec.Schema); err != nil {
		reason := "CreateSchemaFailed"
		message := fmt.Sprintf("Failed to create schema %v: %v", database.Spec.Schema, err)

		degradedCondition = metav1.Condition{
			Type:    conditionDegraded,
			Status:  metav1.ConditionTrue,
			Reason:  reason,
			Message: message,
		}

		r.Recorder.Event(&database, corev1.EventTypeWarning, reason, message)
		return ctrl.Result{}, err
	}

	reason := "SchemaCreated"
	message := fmt.Sprintf("Schema %v is created", database.Spec.Schema)
	r.Recorder.Event(&database, corev1.EventTypeNormal, reason, message)

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DatabaseReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&postgresv1alpha1.Database{}).
		Named("database").
		WithOptions(controller.Options{
			RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[ctrl.Request](5*time.Second, 10*time.Second),
		}).
		Complete(r)
}
