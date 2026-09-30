"""05 — Camada Shared (Silver/Gold) e 06 — Dimensões: executam os SQLs de dags/sql/."""
from etl.utils import pg_exec_file


def shared_gold(ds: str) -> str:
    pg_exec_file("05_shared_gold.sql", {"ds": ds})
    return "shared.vendas_consolidadas"


def construir_dimensao(nome: str, ds: str) -> str:
    pg_exec_file(f"dimensoes/dim_{nome}.sql", {"ds": ds})
    return f"dw.dim_{nome}"
