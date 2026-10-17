# Copyright 2026, Microsoft
#
# Licensed under the Apache License, Version 2.0.

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
AIRFLOW_RELEASE = REPO_ROOT / "software" / "components" / "airflow" / "release.yaml"
WORKFLOW_RELEASE = REPO_ROOT / "software" / "stacks" / "osdu" / "workflows" / "release.yaml"
CORE_STACK = REPO_ROOT / "software" / "stacks" / "osdu" / "profiles" / "core" / "stack.yaml"

VDS_IMAGE = (
    "ghcr.io/kiranbedre/osdu-spi-vds-converter@"
    "sha256:5db47610ae2cb8aaefd97244fdcd8d2bbf9b121742d48a585e309f913013b9fa"
)
OPENVDS_IMAGE = (
    "ghcr.io/kiranbedre/osdu-spi-open-vds@"
    "sha256:4e5ae9565a864a1272010002ad01382810125839f38becd5ee2e3a8362f9fa2d"
)


def test_airflow_pins_the_vds_and_openvds_pair():
    release = yaml.safe_load(AIRFLOW_RELEASE.read_text(encoding="utf-8"))
    values = release["spec"]["values"]

    vds_repository, vds_digest = VDS_IMAGE.split("@", 1)
    assert values["airflowVersion"] == "3.2.2"
    assert values["defaultAirflowRepository"] == vds_repository
    assert values["defaultAirflowDigest"] == vds_digest
    executor = values["config"]["kubernetes_executor"]
    assert (
        f"{executor['worker_container_repository']}:{executor['worker_container_tag']}" == VDS_IMAGE
    )
    pod_template = values["images"]["pod_template"]
    assert f"{pod_template['repository']}:{pod_template['tag']}" == VDS_IMAGE

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
