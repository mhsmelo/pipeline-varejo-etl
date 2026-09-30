"""07 — Logs e Auditoria: resultado de cada task + contagens por camada -> auditoria.execucoes.

No Airflow 3 as tasks não acessam o banco de metadados, então o status de cada etapa
vem do XCom: toda task retorna algo; se não há retorno, ela falhou ou foi pulada.
"""
import json
import logging

from etl.utils import pg_transacao

log = logging.getLogger(__name__)

CONTAGENS = {
    "raw.raw_produtos": "SELECT count(*) FROM raw.raw_produtos WHERE dt_carga = %(ds)s",
    "raw.raw_clientes": "SELECT count(*) FROM raw.raw_clientes WHERE dt_carga = %(ds)s",
    "raw.raw_vendas": "SELECT count(*) FROM raw.raw_vendas WHERE dt_carga = %(ds)s",
    "raw.raw_estoque": "SELECT count(*) FROM raw.raw_estoque WHERE dt_carga = %(ds)s",
    "shared.vendas_consolidadas": "SELECT count(*) FROM shared.vendas_consolidadas WHERE dt_carga = %(ds)s",
    "dw.dim_produto": "SELECT count(*) FROM dw.dim_produto",
    "dw.dim_cliente": "SELECT count(*) FROM dw.dim_cliente",
}


def logs_auditoria(dag_id: str, run_id: str, ds: str, resultados: dict) -> str:
    falhas = [t for t, r in resultados.items() if r is None]
    status = "falha" if falhas else "sucesso"

    linhagem = {}
    with pg_transacao() as cur:
        for tabela, sql in CONTAGENS.items():
            cur.execute("SAVEPOINT contagem")
            try:
                cur.execute(sql, {"ds": ds})
                linhagem[tabela] = cur.fetchone()[0]
            except Exception as exc:  # a tabela pode não existir se uma etapa falhou
                cur.execute("ROLLBACK TO SAVEPOINT contagem")
                linhagem[tabela] = f"erro: {exc.__class__.__name__}"
        cur.execute("""
            INSERT INTO auditoria.execucoes (dag_id, run_id, ds, status, tasks, linhagem)
            VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb)
        """, (dag_id, run_id, ds, status,
              json.dumps(resultados, default=str), json.dumps(linhagem, default=str)))

    log.info("Auditoria: %s | falhas=%s | linhagem=%s", status, falhas, linhagem)
    if falhas:
        raise RuntimeError(f"Pipeline com etapas sem sucesso: {falhas}")  # mantém a execução vermelha
    return status
