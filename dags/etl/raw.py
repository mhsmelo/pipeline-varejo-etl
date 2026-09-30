"""04 — Camada RAW (PostgreSQL): carga idempotente por data de carga."""
import logging

import pandas as pd
from psycopg2.extras import execute_values

from etl.utils import pg_transacao, s3_get_parquet

log = logging.getLogger(__name__)


def _colunas_da_tabela(cur, tabela: str) -> list[str]:
    cur.execute("""SELECT column_name FROM information_schema.columns
                   WHERE table_schema = 'raw' AND table_name = %s""", (tabela,))
    return [r[0] for r in cur.fetchall()]


def carregar_raw(entidade: str, key: str, ds: str) -> int:
    """Apaga o dia (dt_carga) e reinsere: reprocessar uma data nunca duplica linhas."""
    df = s3_get_parquet(key)
    df["_origem_s3"] = key
    tabela = f"raw_{entidade}"

    with pg_transacao() as cur:
        colunas_tabela = _colunas_da_tabela(cur, tabela)
        if not colunas_tabela:
            raise RuntimeError(f"Tabela raw.{tabela} não existe. O 00_ddl.sql foi aplicado?")
        ignoradas = sorted(set(df.columns) - set(colunas_tabela))
        if ignoradas:
            log.warning("raw.%s: colunas sem destino na tabela (ignoradas): %s", tabela, ignoradas)
        cols = [c for c in df.columns if c in colunas_tabela]

        # NaN/NA do pandas -> NULL; Int64/numpy -> tipos Python
        valores = df[cols].astype(object).where(df[cols].notna(), None).values.tolist()

        cur.execute(f"DELETE FROM raw.{tabela} WHERE dt_carga = %s", (ds,))
        execute_values(cur, f"INSERT INTO raw.{tabela} ({', '.join(cols)}) VALUES %s",
                       valores, page_size=1000)

    log.info("raw.%s: %d linhas carregadas (dt_carga=%s)", tabela, len(df), ds)
    return len(df)
