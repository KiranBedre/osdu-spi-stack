# Copyright 2026, Microsoft
#
# Licensed under the Apache License, Version 2.0.

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
AIRFLOW_RELEASE = REPO_ROOT / "software" / "components" / "airflow" / "release.yaml"
DAGS_VOLUME = REPO_ROOT / "software" / "components" / "airflow" / "dags-volume.yaml"
WORKFLOW_RELEASE = REPO_ROOT / "software" / "stacks" / "osdu" / "workflows" / "release.yaml"
CORE_STACK = REPO_ROOT / "software" / "stacks" / "osdu" / "profiles" / "core" / "stack.yaml"
MINIMAL_STACK = REPO_ROOT / "software" / "stacks" / "osdu" / "profiles" / "minimal" / "stack.yaml"

AIRFLOW_IMAGE = (
    "ghcr.io/kiranbedre/osdu-spi-airflow@"
    "sha256:7a50601850488b35b8c257c4a062dbf5ce53c92c406c3fe578f027950f2ae67b"
)
OPENVDS_IMAGE = (
    "ghcr.io/kiranbedre/osdu-spi-open-vds@"
    "sha256:4e5ae9565a864a1272010002ad01382810125839f38becd5ee2e3a8362f9fa2d"
)


def test_airflow_pins_the_shared_runtime_and_openvds_task_image():
    release = yaml.safe_load(AIRFLOW_RELEASE.read_text(encoding="utf-8"))
    values = release["spec"]["values"]

    airflow_repository, airflow_digest = AIRFLOW_IMAGE.split("@", 1)
    assert values["airflowVersion"] == "3.2.2"
    assert values["defaultAirflowRepository"] == airflow_repository
    assert values["defaultAirflowDigest"] == airflow_digest
    executor = values["config"]["kubernetes_executor"]
    assert (
        f"{executor['worker_container_repository']}:{executor['worker_container_tag']}"
        == AIRFLOW_IMAGE
    )
    pod_template = values["images"]["pod_template"]
    assert f"{pod_template['repository']}:{pod_template['tag']}" == AIRFLOW_IMAGE
    assert values["dags"] == {
        "persistence": {"enabled": True, "existingClaim": "airflow-dags"},
        "gitSync": {"enabled": False},
    }

    service_account_consumers = [
        values["workers"],
        values["workers"]["kubernetes"],
        values["migrateDatabaseJob"],
        values["createUserJob"],
        values["apiServer"],
        values["scheduler"],
        values["dagProcessor"],
        values["triggerer"],
    ]
    for consumer in service_account_consumers:
        assert consumer["serviceAccount"] == {
            "create": False,
            "name": "workload-identity-sa",
        }

    env = {entry["name"]: entry["value"] for entry in values["env"]}
    assert env["AIRFLOW_VAR_OPENVDS_INGESTION_IMAGE"] == OPENVDS_IMAGE
    assert env["AIRFLOW_VAR_CORE__SERVICE__SEISMIC__URL"] == (
        "http://seismic.osdu.svc.cluster.local/seistore-svc/api/v3"
    )
    workers = values["workers"]
    assert workers["resources"] == {
        "requests": {"cpu": "250m", "memory": "512Mi"},
        "limits": {"cpu": "1", "memory": "2Gi"},
    }
    assert workers["nodeSelector"] == {"spi-pool": "platform"}
    assert workers["securityContexts"]["pod"] == {
        "runAsNonRoot": True,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    assert workers["securityContexts"]["container"] == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
    }
    assert "AZURE_CLIENT_SECRET" not in AIRFLOW_RELEASE.read_text(encoding="utf-8")


def test_airflow_dag_volume_uses_read_only_workload_identity_blob_csi():
    persistent_volume, claim = list(yaml.safe_load_all(DAGS_VOLUME.read_text(encoding="utf-8")))
    spec = persistent_volume["spec"]
    csi = spec["csi"]

    assert persistent_volume["metadata"]["name"] == "airflow-dags"
    assert spec["accessModes"] == ["ReadOnlyMany"]
    assert spec["persistentVolumeReclaimPolicy"] == "Retain"
    assert spec["storageClassName"] == "azureblob-fuse-premium"
    assert csi["driver"] == "blob.csi.azure.com"
    assert csi["readOnly"] is True
    assert csi["volumeAttributes"] == {
        "clientID": "${OSDU_IDENTITY_CLIENT_ID}",
        "containerName": "airflow-dags",
        "mountWithWorkloadIdentityToken": "true",
        "resourceGroup": "${AZURE_RESOURCE_GROUP}",
        "storageAccount": "${COMMON_STORAGE_ACCOUNT_NAME}",
    }
    assert spec["mountOptions"] == [
        "-o allow_other",
        "-o direct_io",
        "--file-cache-timeout-in-seconds=0",
        "--use-attr-cache=false",
        "--invalidate-on-sync=true",
        "--attr-timeout=0",
        "--entry-timeout=0",
        "--negative-timeout=0",
    ]
    assert claim["metadata"] == {"name": "airflow-dags", "namespace": "platform"}
    assert claim["spec"]["accessModes"] == ["ReadOnlyMany"]
    assert claim["spec"]["storageClassName"] == "azureblob-fuse-premium"
    assert claim["spec"]["volumeName"] == "airflow-dags"


def test_airflow_profiles_substitute_dag_volume_resource_facts():
    for stack_path in [CORE_STACK, MINIMAL_STACK]:
        kustomizations = {
            doc["metadata"]["name"]: doc
            for doc in yaml.safe_load_all(stack_path.read_text(encoding="utf-8"))
            if doc
        }
        airflow = kustomizations["spi-airflow"]
        assert airflow["spec"]["postBuild"]["substituteFrom"] == [
            {
                "kind": "ConfigMap",
                "name": "spi-cluster-config",
                "optional": False,
            }
        ]


def test_vds_workflow_registration_matches_the_baked_dag():
    release = yaml.safe_load(WORKFLOW_RELEASE.read_text(encoding="utf-8"))
    values = release["spec"]["values"]

    assert values["coreEnabled"] is False
    assert values["legalEnabled"] is False
    assert values["membersEnabled"] is False
    assert values["workflowsEnabled"] is True
    assert values["workflows"] == [
        {
            "name": "segy-to-vds-conversion",
            "dagName": "segy-to-vds-conversion",
            "description": "Convert SEG-Y datasets to OpenVDS",
        }
    ]


def test_workflow_registration_waits_for_airflow_and_osdu_bootstrap():
    kustomizations = {
        doc["metadata"]["name"]: doc
        for doc in yaml.safe_load_all(CORE_STACK.read_text(encoding="utf-8"))
        if doc
    }
    registration = kustomizations["spi-osdu-workflows"]

    assert registration["spec"]["path"] == "./software/stacks/osdu/workflows"
    assert {dependency["name"] for dependency in registration["spec"]["dependsOn"]} == {
        "spi-airflow",
        "spi-osdu-init",
    }
