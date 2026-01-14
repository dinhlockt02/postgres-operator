package controller

import (
	"context"
	"fmt"
	"strings"

	postgresv1alpha1 "github.com/dinhlockt02/postgres-operator/api/v1alpha1"
	"github.com/jackc/pgx/v5"
	"github.com/lib/pq"
)

type PostgreSQLConnection interface {
	Ping(ctx context.Context) error
	Close(ctx context.Context) error
	SelectDatabase(ctx context.Context, database string) error
	CreateRole(ctx context.Context, rolename string, password string) error
	IsRoleExist(ctx context.Context, rolename string) (bool, error)
	IsDatabaseExist(ctx context.Context, database string) (bool, error)
	CreateDatabase(ctx context.Context, database string) error
	IsSchemaExist(ctx context.Context, schema string) (bool, error)
	CreateSchema(ctx context.Context, schema string) error
	GrantConnectPermission(ctx context.Context, database string, rolename string) error
	GrantSchemaPrivileges(ctx context.Context, schema string, rolename string, privileges []postgresv1alpha1.SchemaPrivilege) error
	GrantTablePrivileges(ctx context.Context, table string, schema string, rolename string, privileges []postgresv1alpha1.TablePrivilege) error
}

type PgConnectionFactory func(dsn string) (PostgreSQLConnection, error)

// GetConnection creates a PostgreSQL connection using the factory or default implementation
func GetConnection(factory PgConnectionFactory, dsn string) (PostgreSQLConnection, error) {
	if factory != nil {
		return factory(dsn)
	}
	return NewPgConnection(dsn)
}

type PgConnection struct {
	connConfig *pgx.ConnConfig
	conn       *pgx.Conn
}

func NewPgConnection(dsn string) (PostgreSQLConnection, error) {
	connConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}

	conn, err := pgx.ConnectConfig(context.Background(), connConfig)
	if err != nil {
		return nil, err
	}

	return &PgConnection{
		connConfig: connConfig,
		conn:       conn,
	}, nil
}

func (c *PgConnection) Ping(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

func (c *PgConnection) Close(ctx context.Context) error {
	return c.conn.Close(ctx)
}

func (c *PgConnection) SelectDatabase(ctx context.Context, database string) error {
	c.connConfig.Database = database
	newConn, err := pgx.ConnectConfig(ctx, c.connConfig)
	if err != nil {
		return err
	}
	// close current connection, ignore error since we're replacing it
	_ = c.conn.Close(ctx)

	c.conn = newConn
	return nil
}

func (c *PgConnection) CreateRole(ctx context.Context, rolename string, password string) error {
	rolename = pq.QuoteIdentifier(rolename)
	password = pq.QuoteLiteral(password)

	query := fmt.Sprintf("CREATE ROLE %s WITH LOGIN PASSWORD %s;", rolename, password)

	_, err := c.conn.Exec(ctx, query)
	return err
}

func (c *PgConnection) IsRoleExist(ctx context.Context, rolename string) (bool, error) {
	rolename = pq.QuoteLiteral(rolename)

	query := fmt.Sprintf("SELECT TRUE FROM pg_roles WHERE rolname = %s", rolename)

	var exists bool
	err := c.conn.QueryRow(ctx, query).Scan(&exists)
	if err != nil && err != pgx.ErrNoRows {
		return false, err
	}

	if err == pgx.ErrNoRows {
		return false, nil
	}

	return true, nil
}

func (c *PgConnection) IsDatabaseExist(ctx context.Context, database string) (bool, error) {
	query := fmt.Sprintf("SELECT TRUE FROM pg_database WHERE datname = %s", pq.QuoteLiteral(database))

	var exists bool
	err := c.conn.QueryRow(ctx, query).Scan(&exists)
	if err != nil && err != pgx.ErrNoRows {
		return false, err
	}

	if err == pgx.ErrNoRows {
		return false, nil
	}

	return true, nil
}

func (c *PgConnection) CreateDatabase(ctx context.Context, database string) error {
	query := fmt.Sprintf("CREATE DATABASE %s", pq.QuoteIdentifier(database))
	_, err := c.conn.Exec(ctx, query)
	return err
}

func (c *PgConnection) IsSchemaExist(ctx context.Context, schema string) (bool, error) {
	query := fmt.Sprintf("SELECT TRUE FROM information_schema.schemata WHERE schema_name = %s", pq.QuoteLiteral(schema))

	var exists bool
	err := c.conn.QueryRow(ctx, query).Scan(&exists)
	if err != nil && err != pgx.ErrNoRows {
		return false, err
	}

	if err == pgx.ErrNoRows {
		return false, nil
	}

	return true, nil
}

func (c *PgConnection) CreateSchema(ctx context.Context, schema string) error {
	query := fmt.Sprintf("CREATE SCHEMA %s", pq.QuoteIdentifier(schema))
	_, err := c.conn.Exec(ctx, query)
	return err
}

func (c *PgConnection) GrantConnectPermission(ctx context.Context, database string, rolename string) error {
	query := fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", pq.QuoteIdentifier(database), pq.QuoteIdentifier(rolename))

	_, err := c.conn.Exec(ctx, query)
	return err
}

func (c *PgConnection) GrantSchemaPrivileges(ctx context.Context, schema string, rolename string, privileges []postgresv1alpha1.SchemaPrivilege) error {
	joinedPrivileges := strings.Join(privileges, ",")
	query := fmt.Sprintf("GRANT %s ON SCHEMA %s TO %s", joinedPrivileges, pq.QuoteIdentifier(schema), pq.QuoteIdentifier(rolename))

	_, err := c.conn.Exec(ctx, query)
	return err
}

func (c *PgConnection) GrantTablePrivileges(ctx context.Context, table string, schema string, rolename string, privileges []postgresv1alpha1.TablePrivilege) error {
	if table == "*" {
		table = "ALL TABLES IN SCHEMA " + pq.QuoteIdentifier(schema)
	} else {
		table = pq.QuoteIdentifier(table)
	}

	joinedPrivileges := strings.Join(privileges, ",")

	query := fmt.Sprintf("GRANT %s ON %s TO %s", joinedPrivileges, table, pq.QuoteIdentifier(rolename))

	_, err := c.conn.Exec(ctx, query)
	return err
}
