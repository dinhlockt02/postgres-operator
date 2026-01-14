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

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	postgresv1alpha1 "github.com/dinhlockt02/postgres-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	conditionAvailable = "Available"
	conditionDegraded  = "Degraded"
	conditionReady     = "Ready"
)

// DatabaseClusterReconciler reconciles a DatabaseCluster object
type DatabaseClusterReconciler struct {
	client.Client
	Scheme              *runtime.Scheme
	Recorder            record.EventRecorder
	PgConnectionFactory PgConnectionFactory
}

// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=postgres.databases.dinhloc.dev,resources=databaseclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=postgres.databases.dinhloc.dev,resources=databaseclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=postgres.databases.dinhloc.dev,resources=databaseclusters/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the DatabaseCluster object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.22.4/pkg/reconcile
func (r *DatabaseClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx).WithValues("controller", "databasecluster", "resource", req.NamespacedName)
	ctx = logf.IntoContext(ctx, logger)

	logger.V(1).Info("Reconcile started")

	stages := []func(ctx context.Context, req ctrl.Request) (ctrl.Result, error){
		r.ensureDatabaseClusterInitialized,
		r.ensureDatabaseClusterConnected,
	}

	for _, stage := range stages {
		result, err := stage(ctx, req)
		if err != nil || !result.IsZero() {
			if err != nil {
				logger.Error(err, "Reconcile failed")
			}
			return result, err
		}
	}

	logger.V(1).Info("Reconcile completed, requeue after 30s")
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *DatabaseClusterReconciler) ensureDatabaseClusterInitialized(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// 1. Fetch the DatabaseCluster instance
	var cluster postgresv1alpha1.DatabaseCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 2. Initialize the DatabaseCluster status
	if len(cluster.Status.Conditions) == 0 {
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:               conditionAvailable,
			Status:             metav1.ConditionUnknown,
			Reason:             "Initializing",
			Message:            "Initializing DatabaseCluster",
			LastTransitionTime: metav1.Now(),
		})

		if err := r.Status().Update(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *DatabaseClusterReconciler) ensureDatabaseClusterConnected(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	// 1. Get DatabaseCluster instance
	var cluster postgresv1alpha1.DatabaseCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 2. Get connection to the DatabaseCluster
	dsn, err := getDatabaseClusterConnectionDSN(ctx, r.Client, &cluster)
	if err != nil {
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:    conditionAvailable,
			Status:  metav1.ConditionFalse,
			Reason:  "GetDSNFailed",
			Message: fmt.Sprintf("Failed to get DSN for cluster %v", err),
		})

		cluster.Status.FailureCount++
		if err := r.Status().Update(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}

		r.Recorder.Event(&cluster, corev1.EventTypeWarning, "GetDSNFailed", fmt.Sprintf("Failed to get DSN for cluster %v", err))
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}

	conn, err := GetConnection(r.PgConnectionFactory, dsn)
	if err != nil {
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
			Type:    conditionAvailable,
			Status:  metav1.ConditionFalse,
			Reason:  "CreateConnectionFailed",
			Message: fmt.Sprintf("Failed to create connection to cluster %v", err),
		})

		cluster.Status.FailureCount++
		if err := r.Status().Update(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}

		r.Recorder.Event(&cluster, corev1.EventTypeWarning, "CreateConnectionFailed", fmt.Sprintf("Failed to create connection to cluster %v", err))

		return ctrl.Result{}, err
	}
	defer func() { _ = conn.Close(ctx) }()

	// 3. Update the DatabaseCluster status
	cluster.Status.FailureCount = 0 // Reset on success
	changed := meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{
		Type:    conditionAvailable,
		Status:  metav1.ConditionTrue,
		Reason:  "Connected",
		Message: "Connected to DatabaseCluster",
	})

	if changed || cluster.Status.FailureCount == 0 {
		if err := r.Status().Update(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{}, nil
}

// mapSecretToDatabaseCluster maps a Secret to a DatabaseCluster
// Assume the number of database cluster in k8s cluster to be small enough to be listed in a short time
func (r *DatabaseClusterReconciler) mapSecretToDatabaseCluster(ctx context.Context, obj client.Object) []ctrl.Request {
	secret := obj.(*corev1.Secret)
	var requests []ctrl.Request

	// A. List ALL of your CRs
	var crList postgresv1alpha1.DatabaseClusterList
	if err := r.List(ctx, &crList); err != nil {
		logf.FromContext(ctx).Error(err, "Failed to list DatabaseCluster")
		return nil
	}

	// B. Loop through them to find matches
	for _, cr := range crList.Items {
		// Check if this CR's spec points to the Secret that just changed
		if cr.Spec.Connection.Name == secret.Name && cr.Spec.Connection.Namespace == secret.Namespace {
			// C. Found a match! Request a Reconcile for THIS CR.
			requests = append(requests, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      cr.Name,
					Namespace: cr.Namespace,
				},
			})
		}
	}
	return requests
}

// SetupWithManager sets up the controller with the Manager.
func (r *DatabaseClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&postgresv1alpha1.DatabaseCluster{}).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.mapSecretToDatabaseCluster)).
		WithOptions(controller.Options{
			RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[ctrl.Request](5*time.Second, 10*time.Second),
		}).
		Named("databasecluster").
		Complete(r)
}

func getDatabaseClusterConnectionDSN(ctx context.Context, k8sClient client.Client, cluster *postgresv1alpha1.DatabaseCluster) (string, error) {
	connectionSecret := &corev1.Secret{}
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: cluster.Spec.Connection.Name, Namespace: cluster.Spec.Connection.Namespace}, connectionSecret); err != nil {
		return "", err
	}
	return string(connectionSecret.Data["connection"]), nil
}
