"""
================================================================
 users_lakehouse_pipeline.py — Airflow orchestration DAG
================================================================
 End-to-end users pipeline on OpenShift:

   bronze_load   (KubernetesPodOperator → Spark batch CDC consume)
        ↓
   silver_clean  (KubernetesPodOperator → Bronze→Silver transform)
        ↓
   gold_agg      (KubernetesPodOperator → Silver→Gold aggregate)
        ↓
   trino_verify  (KubernetesPodOperator → smoke-test counts)

 Note: For real-time, use the streaming Deployment
 (bronze-streaming-deployment.yaml) instead of bronze_load task.
 This DAG handles scheduled batch + transforms.
================================================================
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


# ----------------------------------------------------------------
# Common config
# ----------------------------------------------------------------
NAMESPACE   = "lakehouse-ingest"
IMAGE       = "image-registry.openshift-image-registry.svc:5000/lakehouse-ingest/lakehouse-app:1.1"

ENV_FROM = [
    k8s.V1EnvFromSource(
        secret_ref=k8s.V1SecretEnvSource(name="kafka-creds-env"),
    ),
    k8s.V1EnvFromSource(
        secret_ref=k8s.V1SecretEnvSource(name="minio-creds-env"),
    ),
]

DEFAULT_RESOURCES = k8s.V1ResourceRequirements(
    requests={"memory": "2Gi", "cpu": "1"},
    limits={"memory": "4Gi", "cpu": "2"},
)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["data-eng@example.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


# ----------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------
with DAG(
    dag_id="users_lakehouse_pipeline",
    description="Users CDC → Bronze → Silver → Gold → Trino verify",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",                # silver every hour
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "users", "cdc", "iceberg"],
) as dag:

    # --------------------------------------------------------
    # 1) BRONZE — Kafka batch consume (for backfill / catchup)
    # --------------------------------------------------------
    bronze_load = KubernetesPodOperator(
        task_id="bronze_load",
        namespace=NAMESPACE,
        image=IMAGE,
        name="bronze-users-batch",
        arguments=[
            "--mode", "kafka",
            "--topic", "pgb.public.users",
            "--consume-mode", "batch",
        ],
        env_from=ENV_FROM,
        container_resources=DEFAULT_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )

    # --------------------------------------------------------
    # 2) SILVER — dedupe + cleanse → users_clean
    # --------------------------------------------------------
    silver_clean = KubernetesPodOperator(
        task_id="silver_clean",
        namespace=NAMESPACE,
        image=IMAGE,
        name="silver-users-clean",
        arguments=["--mode", "transform", "--layer", "silver"],
        env_from=ENV_FROM,
        container_resources=DEFAULT_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )

    # --------------------------------------------------------
    # 3) GOLD — aggregates (email-domain, daily changes)
    # --------------------------------------------------------
    gold_agg = KubernetesPodOperator(
        task_id="gold_agg",
        namespace=NAMESPACE,
        image=IMAGE,
        name="gold-users-agg",
        arguments=["--mode", "transform", "--layer", "gold"],
        env_from=ENV_FROM,
        container_resources=DEFAULT_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )

    # --------------------------------------------------------
    # 4) TRINO smoke-test — quick row-count + freshness check
    # --------------------------------------------------------
    trino_verify = KubernetesPodOperator(
        task_id="trino_verify",
        namespace="lakehouse-catalog",
        image="image-registry.openshift-image-registry.svc:5000/lakehouse-catalog/lakehouse-app:1.1",
        name="trino-users-verify",
        arguments=["--mode", "trino", "--section", "counts"],
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "256Mi", "cpu": "100m"},
            limits={"memory": "512Mi", "cpu": "500m"},
        ),
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
    )

    bronze_load >> silver_clean >> gold_agg >> trino_verify
