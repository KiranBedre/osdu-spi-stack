# Copyright 2026, Microsoft
#
# Licensed under the Apache License, Version 2.0.

from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
AIRFLOW_RELEASE = REPO_ROOT / "software" / "components" / "airflow" / "release.yaml"
ZGY_RELEASE = REPO_ROOT / "software" / "stacks" / "osdu" / "zgy-dag" / "release.yaml"
CORE_STACK = REPO_ROOT / "software" / "stacks" / "osdu" / "profiles" / "core" / "stack.yaml"

ZGY_IMAGE = (
    "ghcr.io/kiranbedre/osdu-spi-zgy-converter@"
    "sha256:91089d80ac2a8d7b5dada702b1e35d083be1f72bf82796570adde06a50794bfe"
)
ZGY_DAG_ARTIFACT = (
    "ghcr.io/kiranbedre/osdu-spi-zgy-converter-dag@"
    "sha256:82d9d0df9621a4bcca7aeb0437639185151c06faa4c12e5d87d0a92301a5ab31"
)


def test_airflow_pins_the_zgy_task_image_and_auth_scope():
    release = yaml.safe_load(AIRFLOW_RELEASE.read_text(encoding="utf-8"))
    env = {entry["name"]: entry["value"] for entry in release["spec"]["values"]["env"]}

    assert env["AIRFLOW_VAR_ZGY_CONVERTER_IMAGE"] == ZGY_IMAGE
    assert env["AIRFLOW_VAR_ZGY_CONVERTER_AUTH_SCOPE"] == "${AAD_CLIENT_ID}/.default"


def test_zgy_dag_delivery_uses_the_service_owned_artifact():
    release = yaml.safe_load(ZGY_RELEASE.read_text(encoding="utf-8"))
    values = release["spec"]["values"]

    assert release["metadata"]["name"] == "osdu-spi-zgy-dag"
    assert release["spec"]["targetNamespace"] == "platform"
    assert values["coreEnabled"] is False
    assert values["legalEnabled"] is False
    assert values["membersEnabled"] is False
    assert values["workflowsEnabled"] is False
    assert values["namespace"] == "platform"
    assert values["dagDelivery"] == {
        "enabled": True,
        "serviceName": "segy-to-zgy-conversion",
        "artifact": {
            "reference": ZGY_DAG_ARTIFACT,
            "dagFile": "dag.zip",
            "checksumFile": "dag.zip.sha256",
            "workflowFile": "workflow.json",
        },
        "storage": {
            "claimName": "airflow-dags-writer",
            "destination": "services/segy-to-zgy-conversion/zgy_converter.zip",
        },
    }


def test_zgy_delivery_is_independent_and_waits_for_seismic():
    kustomizations = {
        doc["metadata"]["name"]: doc
        for doc in yaml.safe_load_all(CORE_STACK.read_text(encoding="utf-8"))
        if doc
    }
    delivery = kustomizations["spi-zgy-dag"]

    assert delivery["spec"]["path"] == "./software/stacks/osdu/zgy-dag"
    assert {dependency["name"] for dependency in delivery["spec"]["dependsOn"]} == {
        "spi-airflow",
        "spi-osdu-init",
        "spi-osdu-ddms",
    }
    assert delivery["spec"]["healthChecks"] == [
        {
            "apiVersion": "helm.toolkit.fluxcd.io/v2",
            "kind": "HelmRelease",
            "name": "osdu-spi-zgy-dag",
            "namespace": "osdu-flux",
        }
    ]
    assert kustomizations["spi-vds-dag"]["spec"]["path"] != delivery["spec"]["path"]
