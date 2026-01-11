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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	postgresv1alpha1 "github.com/dinhlockt02/postgres-operator/api/v1alpha1"
)

var _ = Describe("Database Controller", func() {
	Context("When reconciling a new database", func() {
		const resourceName = "test-database"
		const databaseClusterName = "test-cluster"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		database := &postgresv1alpha1.Database{}
		databaseCluster := &postgresv1alpha1.DatabaseCluster{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind Database")
			err := k8sClient.Get(ctx, typeNamespacedName, database)
			if err != nil && apierrors.IsNotFound(err) {
				resource := &postgresv1alpha1.Database{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: postgresv1alpha1.DatabaseSpec{
						Name:   "test_db",
						Schema: "public",
						DatabaseClusterRef: postgresv1alpha1.DatabaseClusterRef{
							Name: databaseClusterName,
						},
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}

			// Create the referenced DatabaseCluster
			clusterNamespacedName := types.NamespacedName{Name: databaseClusterName, Namespace: "default"}
			err = k8sClient.Get(ctx, clusterNamespacedName, databaseCluster)
			if err != nil && apierrors.IsNotFound(err) {
				clusterResource := &postgresv1alpha1.DatabaseCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      databaseClusterName,
						Namespace: "default",
					},
					Spec: postgresv1alpha1.DatabaseClusterSpec{
						Connection: corev1.SecretReference{
							Name:      "test-secret",
							Namespace: "default",
						},
					},
				}
				Expect(k8sClient.Create(ctx, clusterResource)).To(Succeed())
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
			resource := &postgresv1alpha1.Database{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				By("Cleanup the specific resource instance Database")
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}

			clusterResource := &postgresv1alpha1.DatabaseCluster{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: databaseClusterName, Namespace: "default"}, clusterResource)
			if err == nil {
				Expect(k8sClient.Delete(ctx, clusterResource)).To(Succeed())
			}

			secretResource := &corev1.Secret{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: "test-secret", Namespace: "default"}, secretResource)
			if err == nil {
				Expect(k8sClient.Delete(ctx, secretResource)).To(Succeed())
			}
		})

		It("should successfully create database and schema when they don't exist", func() {
			By("Reconciling the created resource")

			mockConn := new(MockPgConnection)
			mockConn.On("Close", mock.Anything).Return(nil)
			mockConn.On("IsDatabaseExist", mock.Anything, "test_db").Return(false, nil)
			mockConn.On("CreateDatabase", mock.Anything, "test_db").Return(nil)
			mockConn.On("SelectDatabase", mock.Anything, "test_db").Return(nil)
			mockConn.On("IsSchemaExist", mock.Anything, "public").Return(false, nil)
			mockConn.On("CreateSchema", mock.Anything, "public").Return(nil)

			controllerReconciler := &DatabaseReconciler{
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
		})

		It("should skip database creation when database already exists", func() {
			By("Reconciling when database exists")

			mockConn := new(MockPgConnection)
			mockConn.On("Close", mock.Anything).Return(nil)
			mockConn.On("IsDatabaseExist", mock.Anything, "test_db").Return(true, nil)
			// CreateDatabase should NOT be called
			mockConn.On("SelectDatabase", mock.Anything, "test_db").Return(nil)
			mockConn.On("IsSchemaExist", mock.Anything, "public").Return(false, nil)
			mockConn.On("CreateSchema", mock.Anything, "public").Return(nil)

			controllerReconciler := &DatabaseReconciler{
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
			mockConn.AssertNotCalled(GinkgoT(), "CreateDatabase", mock.Anything, mock.Anything)
		})

		It("should skip schema creation when schema already exists", func() {
			By("Reconciling when schema exists")

			mockConn := new(MockPgConnection)
			mockConn.On("Close", mock.Anything).Return(nil)
			mockConn.On("IsDatabaseExist", mock.Anything, "test_db").Return(true, nil)
			mockConn.On("SelectDatabase", mock.Anything, "test_db").Return(nil)
			mockConn.On("IsSchemaExist", mock.Anything, "public").Return(true, nil)
			// CreateSchema should NOT be called

			controllerReconciler := &DatabaseReconciler{
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
			mockConn.AssertNotCalled(GinkgoT(), "CreateSchema", mock.Anything, mock.Anything)
		})

		It("should handle database creation failure", func() {
			By("Reconciling with database creation failure")

			mockConn := new(MockPgConnection)
			mockConn.On("Close", mock.Anything).Return(nil)
			mockConn.On("IsDatabaseExist", mock.Anything, "test_db").Return(false, nil)
			mockConn.On("CreateDatabase", mock.Anything, "test_db").Return(errors.New("database creation failed"))

			controllerReconciler := &DatabaseReconciler{
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
			Expect(err.Error()).To(ContainSubstring("database creation failed"))

			mockConn.AssertExpectations(GinkgoT())
		})

		It("should handle schema creation failure", func() {
			By("Reconciling with schema creation failure")

			mockConn := new(MockPgConnection)
			mockConn.On("Close", mock.Anything).Return(nil)
			mockConn.On("IsDatabaseExist", mock.Anything, "test_db").Return(true, nil)
			mockConn.On("SelectDatabase", mock.Anything, "test_db").Return(nil)
			mockConn.On("IsSchemaExist", mock.Anything, "public").Return(false, nil)
			mockConn.On("CreateSchema", mock.Anything, "public").Return(errors.New("schema creation failed"))

			controllerReconciler := &DatabaseReconciler{
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
			Expect(err.Error()).To(ContainSubstring("schema creation failed"))

			mockConn.AssertExpectations(GinkgoT())
		})

		It("should handle connection factory failure", func() {
			By("Reconciling with connection factory failure")

			controllerReconciler := &DatabaseReconciler{
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

		It("should handle IsDatabaseExist error", func() {
			By("Reconciling with IsDatabaseExist error")

			mockConn := new(MockPgConnection)
			mockConn.On("Close", mock.Anything).Return(nil)
			mockConn.On("IsDatabaseExist", mock.Anything, "test_db").Return(false, errors.New("query failed"))

			controllerReconciler := &DatabaseReconciler{
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
			Expect(err.Error()).To(ContainSubstring("query failed"))

			mockConn.AssertExpectations(GinkgoT())
		})

		It("should handle SelectDatabase error", func() {
			By("Reconciling with SelectDatabase error")

			mockConn := new(MockPgConnection)
			mockConn.On("Close", mock.Anything).Return(nil)
			mockConn.On("IsDatabaseExist", mock.Anything, "test_db").Return(true, nil)
			mockConn.On("SelectDatabase", mock.Anything, "test_db").Return(errors.New("select database failed"))

			controllerReconciler := &DatabaseReconciler{
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
			Expect(err.Error()).To(ContainSubstring("select database failed"))

			mockConn.AssertExpectations(GinkgoT())
		})

		It("should handle resource not found", func() {
			By("Reconciling a non-existent resource")

			mockConn := new(MockPgConnection)

			controllerReconciler := &DatabaseReconciler{
				Client:   k8sClient,
				Scheme:   k8sClient.Scheme(),
				Recorder: record.NewFakeRecorder(10),
				PgConnectionFactory: func(dsn string) (PostgreSQLConnection, error) {
					return mockConn, nil
				},
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      "non-existent-database",
					Namespace: "default",
				},
			})
			Expect(err).To(HaveOccurred())
		})
	})

	Context("When DatabaseCluster is missing", func() {
		const resourceName = "test-database-no-cluster"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}

		BeforeEach(func() {
			By("creating the custom resource referencing non-existent cluster")
			resource := &postgresv1alpha1.Database{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: postgresv1alpha1.DatabaseSpec{
					Name:   "test_db",
					Schema: "public",
					DatabaseClusterRef: postgresv1alpha1.DatabaseClusterRef{
						Name: "non-existent-cluster",
					},
				},
			}
			err := k8sClient.Get(ctx, typeNamespacedName, &postgresv1alpha1.Database{})
			if apierrors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &postgresv1alpha1.Database{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}
		})

		It("should fail when DatabaseCluster is not found", func() {
			By("Reconciling with missing DatabaseCluster")

			mockConn := new(MockPgConnection)

			controllerReconciler := &DatabaseReconciler{
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
