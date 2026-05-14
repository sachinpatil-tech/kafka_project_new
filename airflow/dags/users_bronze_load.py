"""
================================================================
 users_bronze_load.py — Standalone Bronze backfill DAG
================================================================
 Run on-demand to backfill Bronze from Kafka (batch read).
 Useful after schema changes or replays.
================================================================
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

with DAG(
    dag_id="users_bronze_load",
    description="One-shot Kafka batch → Bronze for users CDC topic",
    start_date=datetime(2026, 1, 1),
    schedule=None,                    # manual trigger only
    catchup=False,
    tags=["lakehouse", "bronze", "backfill"],
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    KubernetesPodOperator(
        task_id="bronze_load",
        namespace="lakehouse-ingest",
        image="image-registry.openshift-image-registry.svc:5000/lakehouse-ingest/lakehouse-app:1.1",
        name="bronze-users-batch-manual",
        arguments=["--mode", "kafka", "--topic", "pgb.public.users",
                   "--consume-mode", "batch"],
        env_from=[
            k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="kafka-creds-env")),
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
