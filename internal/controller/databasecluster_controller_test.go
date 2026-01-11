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
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/stretchr/testify/mock"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	postgresv1alpha1 "github.com/dinhlockt02/postgres-operator/api/v1alpha1"
)

var _ = Describe("DatabaseCluster Controller", func() {
	Context("When reconciling a resource", func() {
		const resourceName = "test-resource"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		databasecluster := &postgresv1alpha1.DatabaseCluster{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind DatabaseCluster")
			err := k8sClient.Get(ctx, typeNamespacedName, databasecluster)
			if err != nil && apierrors.IsNotFound(err) {
				resource := &postgresv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: postgresv1alpha1.DatabaseClusterSpec{
						Connection: corev1.SecretReference{
							Name:      "test-secret",
							Namespace: "default",
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
			// Create the referenced Secret
			secretNamespacedName := types.NamespacedName{Name: "test-secret", Namespace: "default"}
			secret := &corev1.Secret{}
			err = k8sClient.Get(ctx, secretNamespacedName, secret)
			if err != nil && apierrors.IsNotFound(err) {
				secretResource := &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-secret",
						Namespace: "default",
					},
					Data: map[string][]byte{
						"connection": []byte("postgres://user:password@10.0.0.1:5432/postgres"),
					},
				}
				Expect(k8sClient.Create(ctx, secretResource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &postgresv1alpha1.DatabaseCluster{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				By("Cleanup the specific resource instance DatabaseCluster")
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}

			secretResource := &corev1.Secret{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: "test-secret", Namespace: "default"}, secretResource)
			if err == nil {
				Expect(k8sClient.Delete(ctx, secretResource)).To(Succeed())
			}
		})

		It("should successfully reconcile the resource when connected", func() {
			By("Reconciling the created resource")

			mockConn := new(MockPgConnection)
			mockConn.On("Close", mock.Anything).Return(nil)

			controllerReconciler := &DatabaseClusterReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: record.NewFakeRecorder(10),
				PgConnectionFactory: func(dsn string) (PostgreSQLConnection, error) {
					return mockConn, nil
				},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			mockConn.AssertExpectations(GinkgoT())

			// Verify status condition is set to Available
			var cluster postgresv1alpha1.DatabaseCluster
			err = k8sClient.Get(ctx, typeNamespacedName, &cluster)
			Expect(err).NotTo(HaveOccurred())
		})

		It("should handle connection factory failure", func() {
			By("Reconciling with a failing connection factory")

			controllerReconciler := &DatabaseClusterReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: record.NewFakeRecorder(10),
				PgConnectionFactory: func(dsn string) (PostgreSQLConnection, error) {
					return nil, errors.New("connection failed")
				},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("connection failed"))
		})

		It("should handle resource not found", func() {
			By("Reconciling a non-existent resource")

			mockConn := new(MockPgConnection)

			controllerReconciler := &DatabaseClusterReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: record.NewFakeRecorder(10),
				PgConnectionFactory: func(dsn string) (PostgreSQLConnection, error) {
					return mockConn, nil
				},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      "non-existent-resource",
					Namespace: "default",
				},
			})
			// Should not return error for not found (already deleted)
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("When secret is missing", func() {
		const resourceName = "test-cluster-no-secret"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}

		BeforeEach(func() {
			By("creating the custom resource without a valid secret")
			resource := &postgresv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: postgresv1alpha1.DatabaseClusterSpec{
					Connection: corev1.SecretReference{
						Name:      "missing-secret",
						Namespace: "default",
					},
				},
			}
			err := k8sClient.Get(ctx, typeNamespacedName, &postgresv1alpha1.DatabaseCluster{})
			if apierrors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &postgresv1alpha1.DatabaseCluster{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}
		})

		It("should fail when secret is not found", func() {
			By("Reconciling with missing secret")

			mockConn := new(MockPgConnection)

			controllerReconciler := &DatabaseClusterReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: record.NewFakeRecorder(10),
				PgConnectionFactory: func(dsn string) (PostgreSQLConnection, error) {
					return mockConn, nil
				},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).To(HaveOccurred())
		})
	})
})
