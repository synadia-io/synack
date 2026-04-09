# Synack

A Kubernetes operator for managing [Synadia Control Plane](https://www.synadia.com/platform) and [Synadia Cloud](https://cloud.synadia.com) resources declaratively. Synack reconciles CRDs in your cluster against the Control Plane API, providing management for accounts, streams, consumers, KV buckets, object stores, NATS users, teams, service accounts, and role bindings.

## Installation

### Install CRDs

```sh
kubectl apply -f config/crd/bases/
```

### Create the token secret

```sh
kubectl create secret generic synack-token \
  --from-literal=SYNACK_TOKEN=<your-control-plane-token>
```

### Run the operator

The operator binary accepts the following flags:

| Flag | Default | Description |
|---|---|---|
| `--control-plane-base-url` | `https://cloud.synadia.com` | Control Plane API base URL |
| `--token-var` | `SYNACK_TOKEN` | Environment variable name containing the API token |
| `--reconcile-interval` | `1m` | Interval between drift-detection reconciliations |
| `--timeout` | `30s` | Timeout for Control Plane API requests |
| `--leader-elect` | `false` | Enable leader election for HA deployments |
| `--metrics-bind-address` | `:8080` | Metrics endpoint bind address |
| `--health-probe-bind-address` | `:8081` | Health/readiness probe bind address |

## Resources

All resources use `apiVersion: synack.synadia.io/v1alpha1` and are namespace-scoped.

### Account

Manages a NATS account within a system.

```yaml
apiVersion: synack.synadia.io/v1alpha1
kind: Account
metadata:
  name: app-prod
spec:
  systemId: <system-id>
  name: app-prod
```

To adopt an existing account, set `accountId` to the Control Plane account ID:

```yaml
spec:
  accountId: <existing-account-id>
  systemId: <system-id>
  name: app-test
```

Account can be used by reference in other resources. Deletion will be blocked until all dependent resources (Streams, KeyValues, ObjectStores, NatsUsers) referencing an Account are removed.

### Stream

Manages a NATS JetStream stream within an account.

**Account selection** -- Streams target an account using one of three methods:

| Method | Fields | Use case |
|---|---|---|
| Account CR reference | `accountRef.name` | Account managed in the same namespace |
| Direct account ID | `accountId` | Account managed externally |
| Public NKey lookup | `accountPublicNKey` + `systemId` | Resolve account by NKey at runtime |

These are mutually exclusive. When using `accountRef`, the operator waits for the referenced Account CR to have a `status.accountId` before proceeding.

```yaml
apiVersion: synack.synadia.io/v1alpha1
kind: Stream
metadata:
  name: orders
spec:
  accountRef:
    name: account-foo
  name: ORDERS
  subjects:
    - "orders.>"
  retention: limits
  storage: file
  replicas: 3
  maxAge: "720h"
```

**Additional spec fields:** `description`, `maxConsumers`, `maxMsgs`, `maxMsgsPerSubject`, `maxBytes`, `maxMsgSize`, `discard`, `noAck`, `duplicateWindow`, `placement`, `sources`, `compression`, `subjectTransform`, `republish`, `sealed`, `denyDelete`, `denyPurge`, `allowDirect`, `allowRollup`, `discardPerSubject`, `firstSequence`, `metadata`.

To adopt an existing stream, set `streamId` in the spec.

Streams can be used by reference in Consumer resources. Deletion will be blocked until all dependent Consumers referencing this Stream are removed.

### KeyValue

Manages a NATS JetStream Key/Value bucket.

```yaml
apiVersion: synack.synadia.io/v1alpha1
kind: KeyValue
metadata:
  name: app-config
spec:
  accountId: <account-id>
  bucket: app-config
  history: 5
  storage: file
```

**Additional spec fields:** `description`, `ttl`, `maxBytes`, `maxValueSize`, `replicas`, `compression`, `placement`, `republish`, `mirror`, `sources`.

To adopt an existing KV bucket, set `keyValueId` in the spec.

### ObjectStore

Manages a NATS JetStream Object Store.

```yaml
apiVersion: synack.synadia.io/v1alpha1
kind: ObjectStore
metadata:
  name: app-assets
spec:
  accountId: <account-id>
  bucket: app-assets
  storage: file
```

**Additional spec fields:** `description`, `ttl`, `maxBytes`, `replicas`, `compression`, `placement`, `metadata`.

To adopt an existing object store, set `objectStoreId` in the spec.

### Consumer

Manages a NATS JetStream consumer on a stream. Consumers reference their parent stream using either `streamRef` (a Stream CR in the same namespace) or `streamId` (a direct Control Plane stream ID). These are mutually exclusive.

**Pull consumer:**

```yaml
apiVersion: synack.synadia.io/v1alpha1
kind: Consumer
metadata:
  name: orders-consumer
spec:
  streamRef:
    name: orders
  name: ORDERS_CONSUMER
  ackPolicy: explicit
  deliverPolicy: all
  maxDeliver: 5
  maxAckPending: 1000
```

**Push consumer** (set `deliverSubject` to create a push consumer):

```yaml
apiVersion: synack.synadia.io/v1alpha1
kind: Consumer
metadata:
  name: orders-notifier
spec:
  streamRef:
    name: orders
  name: ORDERS_NOTIFIER
  deliverSubject: notify.orders
  deliverGroup: notifiers
  ackPolicy: explicit
  deliverPolicy: new
  maxDeliver: 3
```

**Additional spec fields:** `description`, `ackWait`, `durableName`, `filterSubjects`, `inactiveThreshold`, `memStorage`, `replicas`, `optStartSeq`, `optStartTime`, `replayPolicy`, `sampleFreq`, `backoff`, `direct`, `metadata`, `maxRequestBatch`, `maxRequestMaxBytes`, `maxRequestExpires`, `maxWaiting`, `flowControl`, `headersOnly`, `heartbeatInterval`, `rateLimitBps`.

To adopt an existing consumer, set `consumerId` in the spec.

### NatsUser

Manages a NATS user within an account. Supports the same account selection methods as Stream.

```yaml
apiVersion: synack.synadia.io/v1alpha1
kind: NatsUser
metadata:
  name: app-user
spec:
  accountRef:
    name: app-team
  name: app-user
  signingKeyGroupId: Default
  credentialsSecret:
    name: app-user-creds
    key: creds
```

**Credential injection** -- when `credentialsSecret` is configured, the operator downloads the user's NATS credentials from Control Plane and writes them into a Kubernetes Secret. The Secret is owned by the NatsUser CR and cleaned up automatically. If the secret name changes, the old Secret is deleted and a new one is created.

**Additional spec fields:** `jwtExpiresInSecs`, `bearerToken`, `data`, `payload`, `subs`, `allowedConnectionTypes`, `tags`.

To adopt an existing user, set `natsUserId` in the spec.

### Team

Manages a Control Plane team.

```yaml
apiVersion: synack.synadia.io/v1alpha1
kind: Team
metadata:
  name: engineering
spec:
  name: Engineering
```

To adopt an existing team, set `teamId` in the spec.

### TeamServiceAccount

Manages a service account within a team. References its parent team using either `teamRef` (a Team CR in the same namespace) or `teamId` (a direct Control Plane team ID). These are mutually exclusive.

```yaml
apiVersion: synack.synadia.io/v1alpha1
kind: TeamServiceAccount
metadata:
  name: ci-bot
spec:
  teamRef:
    name: engineering
  name: CI Bot
  teamRoleId: <role-id>
```

To adopt an existing service account, set `serviceAccountId` in the spec.

### AppUserRoleBinding

Assigns a role to a team app user (human or service account) scoped to a specific resource. This resource has two independent reference resolution paths.

**Subject** (who gets the role) -- use either:
- `subjectRef.name` -- references a TeamServiceAccount CR in the same namespace; the operator waits for its `status.teamAppUserId`.
- `teamAppUserId` -- a direct Control Plane team app user ID for pre-existing service accounts or human users.

**Target** (what the role is scoped to) -- use either:
- `targetRef.name` -- references a Team or Account CR in the same namespace (depending on `scope`). Only supported for `Team` and `Account` scopes.
- `targetId` -- a direct Control Plane resource ID. Required for `System` and `NatsUser` scopes.

**Scopes:** `Team`, `Account`, `System`, `NatsUser`.

```yaml
apiVersion: synack.synadia.io/v1alpha1
kind: AppUserRoleBinding
metadata:
  name: bot-account-access
spec:
  subjectRef:
    name: bot
  scope: Account
  targetRef:
    name: app-team
  roleId: <role-id>
```

Direct ID example:

```yaml
apiVersion: synack.synadia.io/v1alpha1
kind: AppUserRoleBinding
metadata:
  name: admin-system-access
spec:
  teamAppUserId: <team-app-user-id>
  scope: System
  targetId: <system-id>
  roleId: <role-id>
```

## Resource Dependencies

The operator enforces ordering through reference resolution and deletion guards:

```
Account
  |-- Stream
  |     \-- Consumer
  |-- KeyValue
  |-- ObjectStore
  \-- NatsUser

Team
  \-- TeamServiceAccount
        \-- AppUserRoleBinding (subject)

Account / Team (target)
  \-- AppUserRoleBinding (target)
```

- Resources using `accountRef`, `streamRef`, `teamRef`, or `subjectRef`/`targetRef` will wait (requeue every 5s) until the referenced resource has a status ID.
- Account deletion is blocked until all referencing Streams, KeyValues, ObjectStores, and NatsUsers are removed.
- Stream deletion is blocked until all referencing Consumers are removed.

## Status

Every resource reports its reconciliation state in `.status`:

| Field | Description |
|---|---|
| `message` | `"applied"` on success, or an error description |
| `lastSynced` | RFC 3339 timestamp of last successful reconciliation |
| Resource ID (varies) | The Control Plane ID assigned after creation (e.g. `streamId`, `accountId`) |
