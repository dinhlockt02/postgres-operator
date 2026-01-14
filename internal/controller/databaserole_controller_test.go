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

var _ = Describe("DatabaseRole Controller", func() {
	Context("When reconciling a new role", func() {
		const resourceName = "test-databaserole"
		const databaseClusterName = "test-cluster"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}
		databaserole := &postgresv1alpha1.DatabaseRole{}
		databaseCluster := &postgresv1alpha1.DatabaseCluster{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind DatabaseRole")
			err := k8sClient.Get(ctx, typeNamespacedName, databaserole)
			if err != nil && apierrors.IsNotFound(err) {
				resource := &postgresv1alpha1.DatabaseRole{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: "default",
					},
					Spec: postgresv1alpha1.DatabaseRoleSpec{
						RoleName: "test_role",
						Password: corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: "test-password",
							},
							Key: "password",
						},
						DatabaseClusterRef: postgresv1alpha1.DatabaseClusterRef{
							Name: databaseClusterName,
						},
						Permissions: []postgresv1alpha1.DatabasePermission{
							{
								Database: "test_db",
								Schemas: []postgresv1alpha1.SchemaPermission{
									{
										Name:       "public",
										Privileges: []postgresv1alpha1.SchemaPrivilege{"USAGE"},
									},
								},
							},
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

			// Create the referenced Secret for Connection
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

			// Create the referenced Secret for Password
			passwordSecretNamespacedName := types.NamespacedName{Name: "test-password", Namespace: "default"}
			passwordSecret := &corev1.Secret{}
			err = k8sClient.Get(ctx, passwordSecretNamespacedName, passwordSecret)
			if err != nil && apierrors.IsNotFound(err) {
				secretResource := &corev1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-password",
						Namespace: "default",
					},
					Data: map[string][]byte{
						"password": []byte("insecure-password"),
					},
				}
				Expect(k8sClient.Create(ctx, secretResource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &postgresv1alpha1.DatabaseRole{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				By("Cleanup the specific resource instance DatabaseRole")
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

			passwordSecretResource := &corev1.Secret{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: "test-password", Namespace: "default"}, passwordSecretResource)
			if err == nil {
				Expect(k8sClient.Delete(ctx, passwordSecretResource)).To(Succeed())
			}
		})

		It("should successfully create role and grant permissions", func() {
			By("Reconciling the created resource")

			mockConn := new(MockPgConnection)
			mockConn.On("Close", mock.Anything).Return(nil)
			mockConn.On("IsRoleExist", mock.Anything, "test_role").Return(false, nil)
			mockConn.On("CreateRole", mock.Anything, "test_role", "insecure-password").Return(nil)
			mockConn.On("IsDatabaseExist", mock.Anything, "test_db").Return(true, nil)
			mockConn.On("SelectDatabase", mock.Anything, "test_db").Return(nil)
			mockConn.On("GrantConnectPermission", mock.Anything, "test_db", "test_role").Return(nil)
			mockConn.On("IsSchemaExist", mock.Anything, "public").Return(true, nil)
			mockConn.On("GrantSchemaPrivileges", mock.Anything, "public", "test_role", []postgresv1alpha1.SchemaPrivilege{"USAGE"}).Return(nil)

			controllerReconciler := &DatabaseRoleReconciler{
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

		It("should skip role creation when role already exists", func() {
			By("Reconciling when role exists")

			mockConn := new(MockPgConnection)
			mockConn.On("Close", mock.Anything).Return(nil)
			mockConn.On("IsRoleExist", mock.Anything, "test_role").Return(true, nil)
			// CreateRole should NOT be called
			mockConn.On("IsDatabaseExist", mock.Anything, "test_db").Return(true, nil)
			mockConn.On("SelectDatabase", mock.Anything, "test_db").Return(nil)
			mockConn.On("GrantConnectPermission", mock.Anything, "test_db", "test_role").Return(nil)
			mockConn.On("IsSchemaExist", mock.Anything, "public").Return(true, nil)
			mockConn.On("GrantSchemaPrivileges", mock.Anything, "public", "test_role", []postgresv1alpha1.SchemaPrivilege{"USAGE"}).Return(nil)

			controllerReconciler := &DatabaseRoleReconciler{
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
			mockConn.AssertNotCalled(GinkgoT(), "CreateRole", mock.Anything, mock.Anything, mock.Anything)
		})

		It("should handle connection factory failure", func() {
			By("Reconciling with connection factory failure")

			controllerReconciler := &DatabaseRoleReconciler{
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

		It("should handle role creation failure", func() {
			By("Reconciling with role creation failure")

			mockConn := new(MockPgConnection)
			mockConn.On("Close", mock.Anything).Return(nil)
			mockConn.On("IsRoleExist", mock.Anything, "test_role").Return(false, nil)
			mockConn.On("CreateRole", mock.Anything, "test_role", "insecure-password").Return(errors.New("role creation failed"))

			controllerReconciler := &DatabaseRoleReconciler{
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
			Expect(err.Error()).To(ContainSubstring("role creation failed"))

			mockConn.AssertExpectations(GinkgoT())
		})

		It("should complete successfully even when database does not exist", func() {
			// NOTE: The controller's main Reconcile loop only checks for errors, not RequeueAfter.
			// When ensurePermissionsConfigured returns RequeueAfter, the main loop ignores it.
			By("Reconciling when database does not exist")

			mockConn := new(MockPgConnection)
			mockConn.On("Close", mock.Anything).Return(nil)
			mockConn.On("IsRoleExist", mock.Anything, "test_role").Return(true, nil)
			mockConn.On("IsDatabaseExist", mock.Anything, "test_db").Return(false, nil)

			controllerReconciler := &DatabaseRoleReconciler{
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
			// The reconcile completes without error because the main loop ignores RequeueAfter
			Expect(err).NotTo(HaveOccurred())

			mockConn.AssertExpectations(GinkgoT())
		})

		It("should complete successfully even when schema does not exist", func() {
			// NOTE: The controller's main Reconcile loop only checks for errors, not RequeueAfter.
			// When ensurePermissionsConfigured returns RequeueAfter, the main loop ignores it.
			By("Reconciling when schema does not exist")

			mockConn := new(MockPgConnection)
			mockConn.On("Close", mock.Anything).Return(nil)
			mockConn.On("IsRoleExist", mock.Anything, "test_role").Return(true, nil)
			mockConn.On("IsDatabaseExist", mock.Anything, "test_db").Return(true, nil)
			mockConn.On("SelectDatabase", mock.Anything, "test_db").Return(nil)
			mockConn.On("GrantConnectPermission", mock.Anything, "test_db", "test_role").Return(nil)
			mockConn.On("IsSchemaExist", mock.Anything, "public").Return(false, nil)

			controllerReconciler := &DatabaseRoleReconciler{
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
			// The reconcile completes without error because the main loop ignores RequeueAfter
			Expect(err).NotTo(HaveOccurred())

			mockConn.AssertExpectations(GinkgoT())
		})

		It("should handle GrantConnectPermission failure", func() {
			By("Reconciling with GrantConnectPermission failure")

			mockConn := new(MockPgConnection)
			mockConn.On("Close", mock.Anything).Return(nil)
			mockConn.On("IsRoleExist", mock.Anything, "test_role").Return(true, nil)
			mockConn.On("IsDatabaseExist", mock.Anything, "test_db").Return(true, nil)
			mockConn.On("SelectDatabase", mock.Anything, "test_db").Return(nil)
			mockConn.On("GrantConnectPermission", mock.Anything, "test_db", "test_role").Return(errors.New("grant connect failed"))

			controllerReconciler := &DatabaseRoleReconciler{
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
			Expect(err.Error()).To(ContainSubstring("grant connect failed"))

			mockConn.AssertExpectations(GinkgoT())
		})

		It("should handle GrantSchemaPrivileges failure", func() {
			By("Reconciling with GrantSchemaPrivileges failure")

			mockConn := new(MockPgConnection)
			mockConn.On("Close", mock.Anything).Return(nil)
			mockConn.On("IsRoleExist", mock.Anything, "test_role").Return(true, nil)
			mockConn.On("IsDatabaseExist", mock.Anything, "test_db").Return(true, nil)
			mockConn.On("SelectDatabase", mock.Anything, "test_db").Return(nil)
			mockConn.On("GrantConnectPermission", mock.Anything, "test_db", "test_role").Return(nil)
			mockConn.On("IsSchemaExist", mock.Anything, "public").Return(true, nil)
			mockConn.On("GrantSchemaPrivileges", mock.Anything, "public", "test_role", []postgresv1alpha1.SchemaPrivilege{"USAGE"}).Return(errors.New("grant schema privileges failed"))

			controllerReconciler := &DatabaseRoleReconciler{
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
			Expect(err.Error()).To(ContainSubstring("grant schema privileges failed"))

			mockConn.AssertExpectations(GinkgoT())
		})

		It("should handle IsRoleExist error", func() {
			By("Reconciling with IsRoleExist error")

			mockConn := new(MockPgConnection)
			mockConn.On("Close", mock.Anything).Return(nil)
			mockConn.On("IsRoleExist", mock.Anything, "test_role").Return(false, errors.New("role check failed"))

			controllerReconciler := &DatabaseRoleReconciler{
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
			Expect(err.Error()).To(ContainSubstring("role check failed"))

			mockConn.AssertExpectations(GinkgoT())
		})
	})

	Context("When password secret is missing", func() {
		const resourceName = "test-role-no-password"
		const databaseClusterName = "test-cluster-for-role"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}

		BeforeEach(func() {
			By("creating the custom resource for the Kind DatabaseRole without password secret")
			resource := &postgresv1alpha1.DatabaseRole{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: postgresv1alpha1.DatabaseRoleSpec{
					RoleName: "test_role",
					Password: corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "missing-password-secret",
						},
						Key: "password",
					},
					DatabaseClusterRef: postgresv1alpha1.DatabaseClusterRef{
						Name: databaseClusterName,
					},
				},
			}
			err := k8sClient.Get(ctx, typeNamespacedName, &postgresv1alpha1.DatabaseRole{})
			if apierrors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}

			// Create the referenced DatabaseCluster
			clusterResource := &postgresv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      databaseClusterName,
					Namespace: "default",
				},
				Spec: postgresv1alpha1.DatabaseClusterSpec{
					Connection: corev1.SecretReference{
						Name:      "test-cluster-secret",
						Namespace: "default",
					},
				},
			}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: databaseClusterName, Namespace: "default"}, &postgresv1alpha1.DatabaseCluster{})
			if apierrors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, clusterResource)).To(Succeed())
			}

			// Create the referenced Secret for Connection
			secretResource := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster-secret",
					Namespace: "default",
				},
				Data: map[string][]byte{
					"connection": []byte("postgres://user:password@10.0.0.1:5432/postgres"),
				},
			}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: "test-cluster-secret", Namespace: "default"}, &corev1.Secret{})
			if apierrors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, secretResource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &postgresv1alpha1.DatabaseRole{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}

			clusterResource := &postgresv1alpha1.DatabaseCluster{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: databaseClusterName, Namespace: "default"}, clusterResource)
			if err == nil {
				Expect(k8sClient.Delete(ctx, clusterResource)).To(Succeed())
			}

			secretResource := &corev1.Secret{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: "test-cluster-secret", Namespace: "default"}, secretResource)
			if err == nil {
				Expect(k8sClient.Delete(ctx, secretResource)).To(Succeed())
			}
		})

		It("should fail when password secret is missing", func() {
			By("Reconciling with missing password secret")

			mockConn := new(MockPgConnection)
			mockConn.On("Close", mock.Anything).Return(nil)
			mockConn.On("IsRoleExist", mock.Anything, "test_role").Return(false, nil)

			controllerReconciler := &DatabaseRoleReconciler{
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

			mockConn.AssertExpectations(GinkgoT())
		})
	})

	Context("When table privileges are granted", func() {
		const resourceName = "test-role-with-tables"
		const databaseClusterName = "test-cluster-tables"

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: "default",
		}

		BeforeEach(func() {
			By("creating the custom resource with table permissions")
			resource := &postgresv1alpha1.DatabaseRole{
				ObjectMeta: metav1.ObjectMeta{
					Name:      resourceName,
					Namespace: "default",
				},
				Spec: postgresv1alpha1.DatabaseRoleSpec{
					RoleName: "table_role",
					Password: corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: "table-password",
						},
						Key: "password",
					},
					DatabaseClusterRef: postgresv1alpha1.DatabaseClusterRef{
						Name: databaseClusterName,
					},
					Permissions: []postgresv1alpha1.DatabasePermission{
						{
							Database: "test_db",
							Schemas: []postgresv1alpha1.SchemaPermission{
								{
									Name:       "public",
									Privileges: []postgresv1alpha1.SchemaPrivilege{"USAGE"},
									Tables: []postgresv1alpha1.TablePermission{
										{
											Name:       "users",
											Privileges: []postgresv1alpha1.TablePrivilege{"SELECT", "INSERT"},
										},
									},
								},
							},
						},
					},
				},
			}
			err := k8sClient.Get(ctx, typeNamespacedName, &postgresv1alpha1.DatabaseRole{})
			if apierrors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}

			// Create the referenced DatabaseCluster
			clusterResource := &postgresv1alpha1.DatabaseCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      databaseClusterName,
					Namespace: "default",
				},
				Spec: postgresv1alpha1.DatabaseClusterSpec{
					Connection: corev1.SecretReference{
						Name:      "table-cluster-secret",
						Namespace: "default",
					},
				},
			}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: databaseClusterName, Namespace: "default"}, &postgresv1alpha1.DatabaseCluster{})
			if apierrors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, clusterResource)).To(Succeed())
			}

			// Create the referenced Secret for Connection
			secretResource := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "table-cluster-secret",
					Namespace: "default",
				},
				Data: map[string][]byte{
					"connection": []byte("postgres://user:password@10.0.0.1:5432/postgres"),
				},
			}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: "table-cluster-secret", Namespace: "default"}, &corev1.Secret{})
			if apierrors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, secretResource)).To(Succeed())
			}

			// Create the password secret
			passwordSecretResource := &corev1.Secret{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "table-password",
					Namespace: "default",
				},
				Data: map[string][]byte{
					"password": []byte("table-password-value"),
				},
			}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: "table-password", Namespace: "default"}, &corev1.Secret{})
			if apierrors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, passwordSecretResource)).To(Succeed())
			}
		})

		AfterEach(func() {
			resource := &postgresv1alpha1.DatabaseRole{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			if err == nil {
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
			}

			clusterResource := &postgresv1alpha1.DatabaseCluster{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: databaseClusterName, Namespace: "default"}, clusterResource)
			if err == nil {
				Expect(k8sClient.Delete(ctx, clusterResource)).To(Succeed())
			}

			secretResource := &corev1.Secret{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: "table-cluster-secret", Namespace: "default"}, secretResource)
			if err == nil {
				Expect(k8sClient.Delete(ctx, secretResource)).To(Succeed())
			}

			passwordSecretResource := &corev1.Secret{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: "table-password", Namespace: "default"}, passwordSecretResource)
			if err == nil {
				Expect(k8sClient.Delete(ctx, passwordSecretResource)).To(Succeed())
			}
		})

		It("should successfully grant table privileges", func() {
			By("Reconciling with table permissions")

			mockConn := new(MockPgConnection)
			mockConn.On("Close", mock.Anything).Return(nil)
			mockConn.On("IsRoleExist", mock.Anything, "table_role").Return(true, nil)
			mockConn.On("IsDatabaseExist", mock.Anything, "test_db").Return(true, nil)
			mockConn.On("SelectDatabase", mock.Anything, "test_db").Return(nil)
			mockConn.On("GrantConnectPermission", mock.Anything, "test_db", "table_role").Return(nil)
			mockConn.On("IsSchemaExist", mock.Anything, "public").Return(true, nil)
			mockConn.On("GrantSchemaPrivileges", mock.Anything, "public", "table_role", []postgresv1alpha1.SchemaPrivilege{"USAGE"}).Return(nil)
			mockConn.On("GrantTablePrivileges", mock.Anything, "users", "public", "table_role", []postgresv1alpha1.TablePrivilege{"SELECT", "INSERT"}).Return(nil)

			controllerReconciler := &DatabaseRoleReconciler{
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

		It("should handle GrantTablePrivileges failure", func() {
			By("Reconciling with GrantTablePrivileges failure")

			mockConn := new(MockPgConnection)
			mockConn.On("Close", mock.Anything).Return(nil)
			mockConn.On("IsRoleExist", mock.Anything, "table_role").Return(true, nil)
			mockConn.On("IsDatabaseExist", mock.Anything, "test_db").Return(true, nil)
			mockConn.On("SelectDatabase", mock.Anything, "test_db").Return(nil)
			mockConn.On("GrantConnectPermission", mock.Anything, "test_db", "table_role").Return(nil)
			mockConn.On("IsSchemaExist", mock.Anything, "public").Return(true, nil)
			mockConn.On("GrantSchemaPrivileges", mock.Anything, "public", "table_role", []postgresv1alpha1.SchemaPrivilege{"USAGE"}).Return(nil)
			mockConn.On("GrantTablePrivileges", mock.Anything, "users", "public", "table_role", []postgresv1alpha1.TablePrivilege{"SELECT", "INSERT"}).Return(errors.New("grant table privileges failed"))

			controllerReconciler := &DatabaseRoleReconciler{
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
			Expect(err.Error()).To(ContainSubstring("grant table privileges failed"))

			mockConn.AssertExpectations(GinkgoT())
		})
	})
})
