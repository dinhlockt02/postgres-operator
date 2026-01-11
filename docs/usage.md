# Usage Guide

This guide explains how to use the Postgres Operator to manage your PostgreSQL resources.

## Prerequisites

*   A Kubernetes cluster.
*   The Postgres Operator installed in the cluster.
*   Access to a PostgreSQL server (can be running inside or outside the cluster).

## Step 1: Define Connection Secret

First, create a Kubernetes Secret containing the connection string (DSN) to your PostgreSQL server.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: postgres-connection
  namespace: default
type: Opaque
stringData:
  # Format: postgres://user:password@host:port/dbname?sslmode=disable
  connection: "postgres://postgres:password@postgres-host:5432/postgres?sslmode=disable"
```

## Step 2: Create a DatabaseCluster

Create a `DatabaseCluster` resource to register the Postgres server with the operator. This resource is Cluster-scoped.

```yaml
apiVersion: postgres.databases.dinhloc.dev/v1alpha1
kind: DatabaseCluster
metadata:
  name: main-cluster
spec:
  connection:
    name: postgres-connection
    namespace: default
```

Check the status to ensure the operator can connect:
```bash
kubectl get databasecluster main-cluster -o yaml
```

## Step 3: Create a Database

Create a `Database` resource to provision a new logical database and schema.

```yaml
apiVersion: postgres.databases.dinhloc.dev/v1alpha1
kind: Database
metadata:
  name: my-app-db
  namespace: default
spec:
  name: my_app_db_production  # Actual DB name in Postgres
  schema: public
  databaseClusterRef:
    name: main-cluster
  databaseServiceName: my-app-db-svc # Optional: Creates a K8s service for this DB
```

## Step 4: Create a DatabaseRole

Create a `DatabaseRole` to manage users and permissions.

First, create a secret for the user's password:
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: app-user-password
  namespace: default
type: Opaque
stringData:
  password: "strong-password-here"
```

Then create the Role:
```yaml
apiVersion: postgres.databases.dinhloc.dev/v1alpha1
kind: DatabaseRole
metadata:
  name: app-user
  namespace: default
spec:
  roleName: my_app_user
  password:
    name: app-user-password
    key: password
  databaseClusterRef:
    name: main-cluster
  permissions:
    - database: my_app_db_production
      schemas:
        - name: public
          privileges: ["USAGE", "CREATE"]
          tables:
            - name: "*" # All tables
              privileges: ["SELECT", "INSERT", "UPDATE", "DELETE"]
```

## Cleaning Up

Deleting the CRs will not automatically drop the database or roles in the current implementation (to prevent accidental data loss), but it will stop managing them.
**Note**: Check the specific reclamation policy if implemented in future versions.
