"""Testes do pipeline com S3 simulado (moto) e dados no formato da DummyJSON.

- test_etapas_s3: etapas 01–03, não precisa de nada além do pip install.
- test_pipeline_completo: etapas 01–07; roda só se houver um Postgres com o 00_ddl.sql
  aplicado em POSTGRES_DSN (ex.: o postgres-dw do docker compose, porta 5433).
"""
import sys
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "dags"))
sys.path.insert(0, str(Path(__file__).parent))

from etl import auditoria, config, datalake, ingestao, modelagem, processamento, raw  # noqa: E402
from etl.utils import s3_get_json, s3_get_parquet  # noqa: E402
import dados_dummyjson  # noqa: E402

DS = "2026-09-29"


@pytest.fixture
def s3(monkeypatch):
    monkeypatch.setattr(config, "S3_BUCKET", "bucket-teste")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "teste")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "teste")
    with mock_aws():
        boto3.client("s3", region_name=config.AWS_REGION).create_bucket(Bucket="bucket-teste")
        monkeypatch.setattr(ingestao, "buscar_api", dados_dummyjson.api_fake())
        yield


def rodar_ate_silver():
    k = ingestao.extracao_s3(DS)
    k = datalake.salvar_datalake(k, DS)
    k = processamento.limpeza_dados(k, DS)
    k = processamento.enriquecimento(k, DS)
    return processamento.validacao_regras(k, DS)


def test_etapas_s3(s3):
    silver = rodar_ate_silver()

    # mesmo padrão de chave que você já usava: raw/produtos/dt=AAAA-MM-DD/
    assert s3_get_json(f"raw/produtos/dt={DS}/produtos.json")[0]["title"] == "Produto 1"
    assert set(silver) == {"produtos", "clientes", "vendas", "estoque"}

    vendas = s3_get_parquet(silver["vendas"])
    assert len(vendas) == 15 * 3                                   # 1 linha por item do carrinho
    assert (vendas["valor_desconto"] >= 0).all()

    clientes = s3_get_parquet(silver["clientes"])
    assert clientes.loc[0, "email"] == "nome1@x.dummyjson.com"
    assert clientes.loc[0, "cidade"] == "Phoenix" and clientes.loc[0, "uf"] == "MS"

    estoque = s3_get_parquet(silver["estoque"])
    assert len(estoque) == 30 and "quantidade" in estoque


def test_validacao_barra_rejeicao_alta(s3, monkeypatch):
    ruins = dados_dummyjson.usuarios()
    for u in ruins[:5]:
        u["email"] = "sem-arroba"                                 # 25% inválidos
    fake = dados_dummyjson.api_fake()
    monkeypatch.setattr(ingestao, "buscar_api",
                        lambda e, session=None: ruins if e == "clientes" else fake(e))
    with pytest.raises(ValueError, match="clientes: rejeição"):
        rodar_ate_silver()


def _postgres_disponivel():
    try:
        import psycopg2
        with psycopg2.connect(config.POSTGRES_DSN, connect_timeout=2) as c, c.cursor() as cur:
            cur.execute("SELECT to_regclass('raw.raw_vendas')")
            return cur.fetchone()[0] is not None
    except Exception:
        return False


@pytest.mark.skipif(not _postgres_disponivel(), reason="Postgres do DW não disponível")
def test_pipeline_completo(s3):
    for _ in range(2):                                             # 2ª rodada prova idempotência
        silver = rodar_ate_silver()
        cargas = {e: raw.carregar_raw(e, silver[e], DS) for e in silver}
        modelagem.shared_gold(DS)
        dims = {d: modelagem.construir_dimensao(d, DS)
                for d in ["produto", "cliente", "tempo", "categoria", "marca"]}

    assert cargas == {"produtos": 30, "clientes": 20, "vendas": 45, "estoque": 30}
    resultados = {"raw": cargas, **dims}
    assert auditoria.logs_auditoria("pipeline_varejo_etl", "teste", DS, resultados) == "sucesso"

    from etl.utils import pg_transacao
    with pg_transacao() as cur:
        cur.execute("SELECT count(*) FROM raw.raw_vendas WHERE dt_carga = %s", (DS,))
        assert cur.fetchone()[0] == 45                              # não duplicou
        cur.execute("SELECT count(*), count(categoria), count(nome_cliente) "
                    "FROM shared.vendas_consolidadas WHERE dt_carga = %s", (DS,))
        assert cur.fetchone() == (45, 45, 45)                       # joins completos
        cur.execute("SELECT count(*) FROM dw.dim_categoria")
        assert cur.fetchone()[0] == 5
        cur.execute("SELECT count(*) FROM dw.dim_marca WHERE marca = 'Sem marca'")
        assert cur.fetchone()[0] == 1
