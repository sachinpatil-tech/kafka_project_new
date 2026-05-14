"""
================================================================
 probe.py — DNS / connectivity diagnostic
================================================================
 Cluster ke andar konse service hostnames resolve hote hain,
 isko detect karta hai. Use this when a Trino/Nessie/MinIO
 connection fails with NameResolutionError.

 Usage:
   python -m app.index --mode probe
================================================================
"""
import socket
from typing import List, Tuple

from app import config
from app.utils import get_logger, print_section

log = get_logger("probe")


# Candidate hostnames to test — common naming patterns across namespaces
TRINO_CANDIDATES = [
    "trino.lakehouse-catalog.svc.cluster.local",
    "trino.lakehouse-data.svc.cluster.local",
    "trino.lakehouse.svc.cluster.local",
    "trino.lakehouse-ingest.svc.cluster.local",
    "trino-coordinator.lakehouse-catalog.svc.cluster.local",
    "trino-coordinator.lakehouse.svc.cluster.local",
    "trino-service.lakehouse-catalog.svc.cluster.local",
    "trino.trino.svc.cluster.local",
    # Short DNS (same-namespace resolution)
    "trino",
    "trino-coordinator",
]

NESSIE_CANDIDATES = [
    "nessie.lakehouse-catalog.svc.cluster.local",
    "nessie.lakehouse-data.svc.cluster.local",
    "nessie.lakehouse.svc.cluster.local",
    "nessie.lakehouse-ingest.svc.cluster.local",
    "nessie",
]

MINIO_CANDIDATES = [
    "minio-api.lakehouse-data.svc.cluster.local",
    "minio.lakehouse-data.svc.cluster.local",
    "minio-api.lakehouse-ingest.svc.cluster.local",
    "minio.lakehouse-ingest.svc.cluster.local",
    "minio.lakehouse-catalog.svc.cluster.local",
    "minio-service.lakehouse-data.svc.cluster.local",
    "minio-api",
    "minio",
]

KAFKA_CANDIDATES = [
    "my-cluster-kafka-bootstrap.lakehouse-ingest.svc.cluster.local",
    "my-cluster-kafka-bootstrap.kafka.svc.cluster.local",
    "kafka.lakehouse-ingest.svc.cluster.local",
    "my-cluster-kafka-bootstrap",
]

POSTGRES_CANDIDATES = [
    "postgresql.lakehouse-ingest.svc.cluster.local",
    "postgresql.lakehouse-data.svc.cluster.local",
    "postgres.lakehouse-ingest.svc.cluster.local",
    "postgres.lakehouse.svc.cluster.local",
    "postgresql",
    "postgres",
]


def _probe_one(host: str) -> Tuple[bool, str]:
    """Try to resolve `host`. Returns (ok, ip_or_error)."""
    try:
        ip = socket.gethostbyname(host)
        return True, ip
    except socket.gaierror as e:
        return False, f"gaierror: {e}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def _probe_group(title: str, candidates: List[str]) -> List[str]:
    """Probe one logical service group, return list of resolved hosts."""
    log.info("")
    log.info("--- %s ---", title)
    resolved = []
    for host in candidates:
        ok, info = _probe_one(host)
        marker = "✓" if ok else "✗"
        log.info("  %s %-70s %s", marker, host, info)
        if ok:
            resolved.append(host)
    return resolved


def run() -> int:
    """
    Run a full DNS probe across all expected lakehouse services.
    Logs everything; returns 0 always (purely diagnostic).
    """
    print_section("DNS / CONNECTIVITY PROBE")
    log.info("Configured defaults:")
    log.info("  TRINO_HOST              = %s", config.TRINO_HOST)
    log.info("  NESSIE_URI              = %s", config.NESSIE_URI)
    log.info("  S3_ENDPOINT             = %s", config.S3_ENDPOINT)
    log.info("  KAFKA_BOOTSTRAP_SERVERS = %s", config.KAFKA_BOOTSTRAP_SERVERS)
    log.info("  PG_HOST                 = %s", config.PG_HOST)

    trino_ok    = _probe_group("TRINO",     TRINO_CANDIDATES)
    nessie_ok   = _probe_group("NESSIE",    NESSIE_CANDIDATES)
    minio_ok    = _probe_group("MINIO/S3",  MINIO_CANDIDATES)
    kafka_ok    = _probe_group("KAFKA",     KAFKA_CANDIDATES)
    postgres_ok = _probe_group("POSTGRES",  POSTGRES_CANDIDATES)

    print_section("RESOLVED HOSTS — copy these into config.py defaults")
    log.info("TRINO    : %s", trino_ok    or "(none — service not reachable from this namespace)")
    log.info("NESSIE   : %s", nessie_ok   or "(none)")
    log.info("MINIO/S3 : %s", minio_ok    or "(none)")
    log.info("KAFKA    : %s", kafka_ok    or "(none)")
    log.info("POSTGRES : %s", postgres_ok or "(none)")
    log.info("")
    log.info("Update config.py defaults (or pass env vars in the Job spec)")
    log.info("with the first ✓ host from each group above.")
    return 0
