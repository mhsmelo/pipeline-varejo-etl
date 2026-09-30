"""
# pipeline_varejo_etl

API DummyJSON → S3 (raw / bronze / silver) → PostgreSQL (raw / shared / dw) → auditoria.

| Etapa | Grupo              | Tasks                                                         |
|-------|--------------------|---------------------------------------------------------------|
| 01    | 01_ingestao_s3     | extracao_s3                                                   |
| 02    | 02_data_lake       | salvar_datalake                                               |
| 03    | 03_processamento   | limpeza_dados → enriquecimento → validacao_regras             |
| 04    | 04_camada_raw      | raw_produtos, raw_clientes, raw_vendas, raw_estoque           |
| 05    | 05_camada_shared   | shared_gold                                                   |
| 06    | 06_dimensoes       | dim_produto, dim_cliente, dim_tempo, dim_categoria, dim_marca |
| 07    | 07_logs_auditoria  | logs_auditoria (roda mesmo se algo falhar)                    |
| 08    | 08_fim             | fim_pipeline                                                  |
"""
from datetime import datetime, timedelta

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag, task, task_group

ENTIDADES_RAW = ["produtos", "clientes", "vendas", "estoque"]
DIMENSOES = ["produto", "cliente", "tempo", "categoria", "marca"]

TASKS_MONITORADAS = (
    ["01_ingestao_s3.extracao_s3", "02_data_lake.salvar_datalake"]
    + [f"03_processamento.{t}" for t in ("limpeza_dados", "enriquecimento", "validacao_regras")]
    + [f"04_camada_raw.raw_{e}" for e in ENTIDADES_RAW]
    + ["05_camada_shared.shared_gold"]
    + [f"06_dimensoes.dim_{d}" for d in DIMENSOES]
)


@dag(
    dag_id="pipeline_varejo_etl",
    description="Varejo: API → S3 → PostgreSQL (raw/shared/dimensional)",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "engenharia_dados", "retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["varejo", "etl", "s3", "postgres"],
    doc_md=__doc__,
)
def pipeline_varejo_etl():

    # 01 ── Ingestão S3 ────────────────────────────────────────────────
    @task_group(group_id="01_ingestao_s3", tooltip="Ingestão de dados brutos")
    def ingestao_s3():
        @task
        def extracao_s3(**context):
            from etl.ingestao import extracao_s3 as run
            from etl.utils import data_execucao
            return run(data_execucao(context))

        return extracao_s3()

    # 02 ── Data Lake (Bronze) ─────────────────────────────────────────
    @task_group(group_id="02_data_lake", tooltip="Camada de Landing (Bronze)")
    def data_lake(raw_keys):
        @task
        def salvar_datalake(keys, **context):
            from etl.datalake import salvar_datalake as run
            from etl.utils import data_execucao
            return run(keys, data_execucao(context))

        return salvar_datalake(raw_keys)

    # 03 ── Processamento (Silver) ─────────────────────────────────────
    @task_group(group_id="03_processamento", tooltip="Transformações e padronização")
    def processamento(bronze_keys):
        @task
        def limpeza_dados(keys, **context):
            from etl.processamento import limpeza_dados as run
            from etl.utils import data_execucao
            return run(keys, data_execucao(context))

        @task
        def enriquecimento(keys, **context):
            from etl.processamento import enriquecimento as run
            from etl.utils import data_execucao
            return run(keys, data_execucao(context))

        @task
        def validacao_regras(keys, **context):
            from etl.processamento import validacao_regras as run
            from etl.utils import data_execucao
            return run(keys, data_execucao(context))

        return validacao_regras(enriquecimento(limpeza_dados(bronze_keys)))

    # 04 ── Camada RAW (PostgreSQL) ────────────────────────────────────
    @task_group(group_id="04_camada_raw", tooltip="Dados detalhados e históricos")
    def camada_raw(silver_keys):
        @task
        def carregar_raw(entidade, keys, **context):
            from etl.raw import carregar_raw as run
            from etl.utils import data_execucao
            return run(entidade, keys[entidade], data_execucao(context))

        return [carregar_raw.override(task_id=f"raw_{e}")(e, silver_keys) for e in ENTIDADES_RAW]

    # 05 ── Camada Shared (Silver/Gold) ────────────────────────────────
    @task_group(group_id="05_camada_shared", tooltip="Dados confiáveis (Silver/Gold)")
    def camada_shared():
        @task
        def shared_gold(**context):
            from etl.modelagem import shared_gold as run
            from etl.utils import data_execucao
            return run(data_execucao(context))

        return shared_gold()

    # 06 ── Dimensões ──────────────────────────────────────────────────
    @task_group(group_id="06_dimensoes", tooltip="Camada dimensional")
    def dimensoes():
        @task
        def construir_dimensao(nome, **context):
            from etl.modelagem import construir_dimensao as run
            from etl.utils import data_execucao
            return run(nome, data_execucao(context))

        return [construir_dimensao.override(task_id=f"dim_{d}")(d) for d in DIMENSOES]

    # 07 ── Logs e Auditoria ───────────────────────────────────────────
    @task_group(group_id="07_logs_auditoria", tooltip="Monitoramento e linhagem")
    def auditoria():
        @task(trigger_rule="all_done", retries=0)
        def logs_auditoria(**context):
            from etl.auditoria import logs_auditoria as run
            from etl.utils import data_execucao
            ti = context["ti"]
            resultados = {t: ti.xcom_pull(task_ids=t) for t in TASKS_MONITORADAS}
            return run(context["dag"].dag_id, context["run_id"], data_execucao(context), resultados)

        return logs_auditoria()

    # 08 ── Fim ────────────────────────────────────────────────────────
    fim = EmptyOperator(task_id="fim_pipeline")

    raws = camada_raw(processamento(data_lake(ingestao_s3())))
    raws >> camada_shared() >> dimensoes() >> auditoria() >> fim


pipeline_varejo_etl()
