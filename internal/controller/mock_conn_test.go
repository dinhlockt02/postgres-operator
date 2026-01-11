package controller

import (
	"context"

	postgresv1alpha1 "github.com/dinhlockt02/postgres-operator/api/v1alpha1"
	"github.com/stretchr/testify/mock"
)

type MockPgConnection struct {
	mock.Mock
}

func (m *MockPgConnection) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockPgConnection) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockPgConnection) SelectDatabase(ctx context.Context, database string) error {
	args := m.Called(ctx, database)
	return args.Error(0)
}

func (m *MockPgConnection) CreateRole(ctx context.Context, rolename string, password string) error {
	args := m.Called(ctx, rolename, password)
	return args.Error(0)
}

func (m *MockPgConnection) IsRoleExist(ctx context.Context, rolename string) (bool, error) {
	args := m.Called(ctx, rolename)
	return args.Bool(0), args.Error(1)
}

func (m *MockPgConnection) IsDatabaseExist(ctx context.Context, database string) (bool, error) {
	args := m.Called(ctx, database)
	return args.Bool(0), args.Error(1)
}

func (m *MockPgConnection) CreateDatabase(ctx context.Context, database string) error {
	args := m.Called(ctx, database)
	return args.Error(0)
}

func (m *MockPgConnection) IsSchemaExist(ctx context.Context, schema string) (bool, error) {
	args := m.Called(ctx, schema)
	return args.Bool(0), args.Error(1)
}

func (m *MockPgConnection) CreateSchema(ctx context.Context, schema string) error {
	args := m.Called(ctx, schema)
	return args.Error(0)
}

func (m *MockPgConnection) GrantConnectPermission(ctx context.Context, database string, rolename string) error {
	args := m.Called(ctx, database, rolename)
	return args.Error(0)
}

func (m *MockPgConnection) GrantSchemaPrivileges(ctx context.Context, schema string, rolename string, privileges []postgresv1alpha1.SchemaPrivilege) error {
	args := m.Called(ctx, schema, rolename, privileges)
	return args.Error(0)
}

func (m *MockPgConnection) GrantTablePrivileges(ctx context.Context, table string, schema string, rolename string, privileges []postgresv1alpha1.TablePrivilege) error {
	args := m.Called(ctx, table, schema, rolename, privileges)
	return args.Error(0)
}
