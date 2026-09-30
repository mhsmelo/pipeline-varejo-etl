"""Helpers de S3 e PostgreSQL compartilhados pelas etapas."""
import io
import json
import logging
from contextlib import contextmanager
from pathlib import Path

import boto3
import pandas as pd

from etl import config

log = logging.getLogger(__name__)
SQL_DIR = Path(__file__).resolve().parent.parent / "sql"


# ── Data de execução ──────────────────────────────────────────────────
def data_execucao(context) -> str:
    """No Airflow 3 uma execução manual pode vir sem logical_date; nesse caso usa run_after."""
    if context.get("ds"):
        return context["ds"]
    return context["dag_run"].run_after.date().isoformat()


def particao(ds: str) -> str:
    return f"dt={ds}"


# ── S3 ────────────────────────────────────────────────────────────────
def s3_client():
    # Sem chaves explícitas: o boto3 lê AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY do ambiente
    # (ou usa a IAM Role, quando rodar na AWS).
    return boto3.client("s3", region_name=config.AWS_REGION, endpoint_url=config.AWS_ENDPOINT_URL)


def s3_put_json(key: str, data) -> str:
    body = json.dumps(data, ensure_ascii=False, default=str).encode("utf-8")
    s3_client().put_object(Bucket=config.S3_BUCKET, Key=key, Body=body,
                           ContentType="application/json")
    log.info("s3://%s/%s (%d bytes)", config.S3_BUCKET, key, len(body))
    return key


def s3_get_json(key: str):
    obj = s3_client().get_object(Bucket=config.S3_BUCKET, Key=key)
    return json.loads(obj["Body"].read())


def s3_put_parquet(key: str, df: pd.DataFrame) -> str:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    s3_client().put_object(Bucket=config.S3_BUCKET, Key=key, Body=buf.getvalue())
    log.info("s3://%s/%s (%d linhas)", config.S3_BUCKET, key, len(df))
    return key


def s3_get_parquet(key: str) -> pd.DataFrame:
    obj = s3_client().get_object(Bucket=config.S3_BUCKET, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))


# ── PostgreSQL (psycopg2 direto: funciona igual dentro e fora do Airflow) ──
def _conectar():
    """Dentro do Airflow usa a connection postgres_dw; fora dele (testes, scripts), POSTGRES_DSN."""
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
    except ImportError:
        import psycopg2
        return psycopg2.connect(config.POSTGRES_DSN)
    return PostgresHook(postgres_conn_id=config.POSTGRES_CONN_ID).get_conn()


@contextmanager
def pg_transacao():
    conn = _conectar()
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def pg_exec_file(relpath: str, params: dict | None = None):
    sql = (SQL_DIR / relpath).read_text(encoding="utf-8")
    with pg_transacao() as cur:
        cur.execute(sql, params or {})
