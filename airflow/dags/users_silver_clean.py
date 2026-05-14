"""
================================================================
 users_silver_clean.py — Standalone Silver rebuild DAG
================================================================
 Hourly Bronze → Silver materialization. Idempotent.
================================================================
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

with DAG(
    dag_id="users_silver_clean",
    description="Bronze → Silver dedup + cleansing",
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "silver"],
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    KubernetesPodOperator(
        task_id="silver_clean",
        namespace="lakehouse-ingest",
        image="image-registry.openshift-image-registry.svc:5000/lakehouse-ingest/lakehouse-app:1.1",
        name="silver-users-clean-{{ ts_nodash }}",
        arguments=["--mode", "transform", "--layer", "silver"],
        env_from=[
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="minio-creds-env")),
        ],
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "2Gi", "cpu": "1"},
            limits={"memory": "4Gi", "cpu": "2"},
        ),
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )
