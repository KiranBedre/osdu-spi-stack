# ADR-043: Airflow DAGs Ship Independently

## Context

Airflow is a platform component under ADR-006. Conversion DAGs are service
artifacts with their own source commits, task images, release cadence, and
rollback boundary. Baking a service DAG into the Airflow control-plane image
couples that service release to the scheduler, API server, triggerer, DAG
processor, migration Jobs, and KubernetesExecutor workers.

`infra/modules/storage-common.bicep` provisions the `airflow-dags` Blob
container, `infra/aks.bicep` enables the Blob CSI driver, and
`infra/modules/rbac.bicep` grants the shared OSDU identity Blob data access.
ADR-023 prohibits storage keys and SAS tokens, so the DAG mount must use the
federated `platform:workload-identity-sa`.

## Decision

`software/components/airflow/release.yaml` pins one generic Airflow runtime
image that contains the shared OSDU and Kubernetes providers but no service
DAGs. The same digest runs the control plane and KubernetesExecutor workers.
Service task images remain separate immutable Airflow Variables.

`software/components/airflow/dags-volume.yaml` binds a static read-only Blob
CSI volume to the existing `airflow-dags` container. Airflow mounts the claim
through `platform:workload-identity-sa` with
`mountWithWorkloadIdentityToken: "true"` and cache-disabled BlobFuse options.
The same manifest exposes a writer-only claim for delivery Jobs; Airflow never
mounts that claim. The Airflow HelmRelease and volumes resolve the identity
client ID, storage account, and resource group from `spi-cluster-config`.

Each service publishes one independently versioned OCI DAG artifact containing
its rendered DAG zip, checksum, and Workflow registration body. A
service-specific Flux release uses the generic DAG delivery Job in
`osdu-spi-init` to pull the exact artifact reference, validate the checksum and
zip, replace only that service's zip through the writer claim, and
register the artifact-owned Workflow body after `spi-airflow` and
`spi-osdu-init` are ready. Registration accepts an existing exact `dagName`,
creates a missing registration, and fails on a conflicting registration.

Conversion pods run in `platform` through `workload-identity-sa`. Their DAGs
retain explicit requests, limits, non-root execution, RuntimeDefault seccomp,
no privilege escalation, and no Linux capabilities.

Rejected: bake service DAGs into the Airflow image; one tested image carries
the dependencies, but any DAG rollback also rolls back the control plane.

Rejected: distribute DAGs through git-sync; branch updates are direct, but
they do not provide an immutable per-service artifact or registration body.

Rejected: combine VDS and ZGY in one DAG artifact; one delivery Job is smaller,
but either service would move and roll back the other.

Rejected: mount Blob storage with an account key or SAS token; the CSI
configuration is shorter, but it violates ADR-023 and creates stored
credentials.

## Consequences

- A service release must publish and retain the DAG artifact, task image, and
  compatibility evidence as separate immutable references.
- Airflow runtime upgrades no longer redeploy service DAGs, and one service
  DAG can roll back without moving another service or the control plane.
- The shared Blob folder is a multi-writer boundary. Each delivery Job owns
  only its service path and must not prune another service's files.
- Cache-disabled read-only mounts expose completed Blob writes to Airflow
  without restarting the control plane.
- Workflow registration remains partition-scoped and gates delivery. A
  conflicting `dagName` stops reconciliation instead of replacing tenant
  metadata.
- Airflow and conversion pods acquire Azure tokens through the existing
  federation. DAG artifacts and Helm values carry no storage credential or
  client secret.
