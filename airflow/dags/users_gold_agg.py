"""
================================================================
 users_gold_agg.py — Standalone Gold rebuild DAG
================================================================
 Daily Silver → Gold aggregation. Idempotent.
================================================================
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

with DAG(
    dag_id="users_gold_agg",
    description="Silver → Gold aggregation (email-domain, daily changes)",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "gold"],
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
) as dag:
    KubernetesPodOperator(
        task_id="gold_agg",
        namespace="lakehouse-ingest",
        image="image-registry.openshift-image-registry.svc:5000/lakehouse-ingest/lakehouse-app:1.1",
        name="gold-users-agg-{{ ds_nodash }}",
        arguments=["--mode", "transform", "--layer", "gold"],
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
