# Projeto ETL AWS — pipeline_varejo_etl

Pipeline de varejo ponta a ponta com **Airflow 3 + Python + S3 + PostgreSQL**, usando a API
[DummyJSON](https://dummyjson.com) como fonte:

```
API DummyJSON ─► S3 raw ─► S3 bronze ─► S3 silver ─► Postgres raw ─► shared ─► dw ─► auditoria
 (products,      JSON       Parquet      limpo/        (histórico    (vendas    (dimensões)
  users, carts)                          validado)      por dia)      consolid.)
```

| Etapa | Tasks | Onde grava |
|---|---|---|
| 01 Ingestão S3 | `extracao_s3` | `s3://bucket/raw/<entidade>/dt=AAAA-MM-DD/<entidade>.json` |
| 02 Data Lake | `salvar_datalake` | `bronze/<entidade>/dt=…/*.parquet` + `bronze/_catalogo/` |
| 03 Processamento | `limpeza_dados` → `enriquecimento` → `validacao_regras` | `silver/{limpo,enriquecido,validado,rejeitados}/…` |
| 04 Camada RAW | `raw_produtos`, `raw_clientes`, `raw_vendas`, `raw_estoque` | Postgres `raw.*` |
| 05 Camada Shared | `shared_gold` | `shared.vendas_consolidadas` |
| 06 Dimensões | `dim_produto`, `dim_cliente`, `dim_tempo`, `dim_categoria`, `dim_marca` | `dw.*` |
| 07 Logs e Auditoria | `logs_auditoria` | `auditoria.execucoes` |
| 08 Fim | `fim_pipeline` | — |

**Como a DummyJSON vira o modelo:**
- `/products` → produtos, e também a fotografia diária de **estoque** (campo `stock`).
- `/users` → clientes. Usa `select=` para **não** trazer senha, cartão e IP para o Data Lake.
- `/carts` → vendas, com cada carrinho quebrado em uma linha por produto.
  A API não informa data nos carrinhos, então `data_venda` = data da execução.
- A DummyJSON não tem lojas nem vendedores. Por isso, no lugar de `dim_loja` e `dim_vendedor`
  do desenho de referência, o modelo tem `dim_categoria` e `dim_marca`.

## Estrutura

```
dags/
  pipeline_varejo_etl.py   # a DAG (8 TaskGroups)
  .airflowignore           # evita o Airflow varrer etl/ e sql/ procurando DAGs
  etl/                     # uma etapa por módulo: ingestao, datalake, processamento,
                           # raw, modelagem (shared + dims), auditoria; config e utils
  sql/00_ddl.sql           # tabelas (aplicado sozinho na 1ª subida do postgres-dw)
  sql/05_shared_gold.sql
  sql/dimensoes/dim_*.sql
src/ingestao/              # seus scripts originais (continuam funcionando avulsos)
tests/                     # testes com S3 simulado e dados no formato da DummyJSON
Dockerfile                 # imagem do Airflow 3.0.2 + dependências do pipeline
docker-compose.yaml        # o seu compose + Postgres do DW + .env nos containers
```

## Como rodar

**1. `.env`**: o seu atual continua valendo; basta acrescentar `AIRFLOW_UID=50000`
(veja `.env.example`). O bucket é lido de `S3_BUCKET_NAME`, como nos seus scripts.

**2. Subir** (a primeira vez demora, porque constrói a imagem):
```bash
docker compose up -d --build
```

**3. Executar**: acesse http://localhost:8080 (usuário e senha `airflow`), ative
`pipeline_varejo_etl` e clique em **Trigger**. Pelo terminal:
```bash
docker compose exec airflow-scheduler airflow dags trigger pipeline_varejo_etl
```

**4. Conferir**
```bash
aws s3 ls s3://SEU_BUCKET/ --recursive
docker compose exec postgres-dw psql -U dw -d varejo -c "select * from auditoria.execucoes"
```
O DW também fica acessível de fora, em `localhost:5433` (usuário e senha `dw`, banco `varejo`),
para usar no DBeaver ou pgAdmin.

## Testes (sem Docker e sem AWS)

```bash
python -m venv venv && venv\Scripts\activate      # Linux/Mac: source venv/bin/activate
pip install -r requirements.txt
pytest -q
```
Sem Postgres, 2 testes passam e 1 é pulado. Com o `postgres-dw` do compose no ar, os 3 rodam,
incluindo o pipeline completo até a auditoria.

## Decisões

- **O XCom só carrega caminhos do S3**, nunca os dados.
- **Idempotência**: rodar a mesma data de novo não duplica nada. O S3 sobrescreve `dt=`,
  o RAW faz delete + insert do dia, e shared/dw fazem upsert.
- **Validação**: registros inválidos vão para `silver/rejeitados/`. Acima de 5% (`LIMITE_REJEICAO`),
  a task falha.
- **Auditoria** roda mesmo se algo falhar (`all_done`). O status de cada etapa vem do XCom,
  porque no Airflow 3 as tasks não acessam o banco de metadados.
- Mudou o `00_ddl.sql`? Ele só roda na criação do volume, então use
  `docker compose down -v && docker compose up -d`.
