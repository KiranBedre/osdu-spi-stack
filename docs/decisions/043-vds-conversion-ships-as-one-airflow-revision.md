# ADR-043: VDS Conversion Ships as One Airflow Revision

## Context

The SEG-Y to OpenVDS workflow has two runtime images. The VDS Converter
repository builds an Airflow image containing the DAG and its Python
dependencies; the OpenVDS repository builds the `SEGYImport` image that the
DAG launches. The repositories have independent commits and publication
pipelines, so ADR-031's companion package suffix and shared source commit do
not describe this pair.

Airflow is a platform component under ADR-006, not an OSDU service resolved
through the image lock in ADR-017. OpenVDS is consumed only by this DAG as an
Airflow Variable. Moving either image without the other can install a DAG
whose operator contract has not been tested against its conversion runtime.

## Decision

`software/components/airflow/release.yaml` owns the VDS runtime pair. It pins
the custom Airflow repository with `defaultAirflowDigest` and sets
`AIRFLOW_VAR_OPENVDS_INGESTION_IMAGE` to an immutable OpenVDS digest in the
same HelmRelease values. One reviewed Stack commit changes both values and
one Flux revision rolls them back.

Apache Airflow chart 1.22 applies `defaultAirflowDigest` to its control-plane
pods but exposes only `repository` and `tag` fields for the
KubernetesExecutor pod template. The HelmRelease splits
`repository@sha256:<hex>` across those two fields so the chart's
`repository:tag` join renders the same digest reference for task workers.

The HelmRelease also sets
`AIRFLOW_VAR_CORE__SERVICE__SEISMIC__URL` to the in-cluster Seismic V3
endpoint. The rendered DAG launches conversion pods in `platform` through
`workload-identity-sa`, the namespace and federation ADR-005 and ADR-006
already assign to Airflow. It does not add a fourth application namespace, a
second federated credential, or cross-namespace pod RBAC.
The renderer schedules OpenVDS on the `osdu` node pool with explicit requests,
limits, non-root execution, RuntimeDefault seccomp, no privilege escalation,
and no Linux capabilities. The Airflow KubernetesExecutor template carries
the same security controls and its own platform-pool resource boundary.

`spi-osdu-workflows` registers `segy-to-vds-conversion` once per configured
partition after `spi-osdu-init` and `spi-airflow` are ready. Its Job first
reads existing Workflow metadata, accepts an exact `dagName` match, creates a
missing registration without DAG content, and fails on a conflicting
registration. The generation in
`software/stacks/osdu/workflows/release.yaml` changes when completed Jobs must
run again.

Rejected: represent OpenVDS as ADR-031 companion metadata. The existing
borrow, prove, and restore flow is useful, but a package suffix and one source
commit would record false provenance for two repositories.

Rejected: add the two images to `osdu-image-lock`. Runtime refresh and fork
pins would become available, but Airflow is middleware and OpenVDS has no
independent Flux workload; the lock would need a new cross-origin transaction
and an Airflow Variable consumer.

Rejected: launch conversion pods in an `airflow` namespace with
`airflow-identity-sa`. The namespace isolates task pods, but requires another
namespace, federated credential, ServiceAccount, and cross-namespace Role
while the Airflow control plane remains in `platform`.

Rejected: distribute the DAG through git-sync. DAG-only updates would avoid
an Airflow image build, but its Python dependencies and Airflow compatibility
would no longer share the image digest that CI validates.

## Consequences

- A VDS source image cannot borrow a live environment through the OSDU image
  lock. Live proof starts from a Stack change that names the tested VDS and
  OpenVDS digests.
- Scheduler, DAG processor, API server, triggerer, migration Jobs, and
  KubernetesExecutor workers run the same custom Airflow image.
- The pair is inspectable in one HelmRelease and rolls back with the Stack
  Git revision. It does not gain `spi service pin`, reset, or refresh
  semantics.
- Workflow registration is partition-scoped and gates deployment. A
  conflicting `dagName` stops reconciliation instead of replacing tenant
  metadata.
- Conversion pods acquire Azure tokens through the existing
  `platform:workload-identity-sa` federation. The DAG and Airflow Variables
  carry no client secret.
