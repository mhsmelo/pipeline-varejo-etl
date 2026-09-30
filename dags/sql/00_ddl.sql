-- Executado automaticamente na primeira subida do postgres-dw (docker-entrypoint-initdb.d)
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS shared;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS auditoria;

-- ══ 04 RAW: fiel à origem, histórico completo (uma "fotografia" por dt_carga) ══
CREATE TABLE IF NOT EXISTS raw.raw_produtos (
    id_produto             INTEGER NOT NULL,
    nome_produto           TEXT,
    descricao              TEXT,
    categoria              TEXT,
    marca                  TEXT,
    sku                    TEXT,
    preco                  NUMERIC(14,2),
    desconto_percentual    NUMERIC(6,2),
    preco_com_desconto     NUMERIC(14,2),
    avaliacao              NUMERIC(4,2),
    estoque                INTEGER,
    status_disponibilidade TEXT,
    dt_carga               DATE NOT NULL,
    _origem_s3             TEXT,
    _carregado_em          TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_raw_produtos_dt ON raw.raw_produtos (dt_carga);

CREATE TABLE IF NOT EXISTS raw.raw_clientes (
    id_cliente      INTEGER NOT NULL,
    nome            TEXT,
    sobrenome       TEXT,
    nome_completo   TEXT,
    email           TEXT,
    idade           INTEGER,
    faixa_etaria    TEXT,
    genero          TEXT,
    data_nascimento DATE,
    cidade          TEXT,
    estado          TEXT,
    uf              TEXT,
    pais            TEXT,
    dt_carga        DATE NOT NULL,
    _origem_s3      TEXT,
    _carregado_em   TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_raw_clientes_dt ON raw.raw_clientes (dt_carga);

CREATE TABLE IF NOT EXISTS raw.raw_vendas (
    id_carrinho          INTEGER NOT NULL,
    id_cliente           INTEGER,
    id_produto           INTEGER NOT NULL,
    nome_produto         TEXT,
    preco                NUMERIC(14,2),
    quantidade           INTEGER,
    valor_bruto          NUMERIC(14,2),
    desconto_percentual  NUMERIC(6,2),
    valor_desconto       NUMERIC(14,2),
    valor_total          NUMERIC(14,2),
    data_venda           DATE,
    dt_carga             DATE NOT NULL,
    _origem_s3           TEXT,
    _carregado_em        TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_raw_vendas_dt ON raw.raw_vendas (dt_carga);

CREATE TABLE IF NOT EXISTS raw.raw_estoque (
    id_produto             INTEGER NOT NULL,
    data_referencia        DATE NOT NULL,
    quantidade             INTEGER,
    preco                  NUMERIC(14,2),
    status_disponibilidade TEXT,
    dt_carga               DATE NOT NULL,
    _origem_s3             TEXT,
    _carregado_em          TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_raw_estoque_dt ON raw.raw_estoque (dt_carga);

-- ══ 05 SHARED (Silver/Gold): venda por item, já com produto e cliente ══
CREATE TABLE IF NOT EXISTS shared.vendas_consolidadas (
    id_carrinho         INTEGER NOT NULL,
    id_produto          INTEGER NOT NULL,
    data_venda          DATE NOT NULL,
    nome_produto        TEXT,
    categoria           TEXT,
    marca               TEXT,
    id_cliente          INTEGER,
    nome_cliente        TEXT,
    cidade              TEXT,
    uf                  TEXT,
    faixa_etaria        TEXT,
    quantidade          INTEGER,
    preco               NUMERIC(14,2),
    valor_bruto         NUMERIC(14,2),
    valor_desconto      NUMERIC(14,2),
    valor_total         NUMERIC(14,2),
    dt_carga            DATE NOT NULL,
    PRIMARY KEY (id_carrinho, id_produto)
);

-- ══ 06 DW: dimensões ══
CREATE TABLE IF NOT EXISTS dw.dim_produto (
    sk_produto    SERIAL PRIMARY KEY,
    id_produto    INTEGER UNIQUE NOT NULL,
    nome_produto  TEXT,
    categoria     TEXT,
    marca         TEXT,
    sku           TEXT,
    preco_atual   NUMERIC(14,2),
    atualizado_em TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS dw.dim_cliente (
    sk_cliente      SERIAL PRIMARY KEY,
    id_cliente      INTEGER UNIQUE NOT NULL,
    nome_completo   TEXT,
    email           TEXT,
    genero          TEXT,
    faixa_etaria    TEXT,
    cidade          TEXT,
    estado          TEXT,
    uf              TEXT,
    pais            TEXT,
    atualizado_em   TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS dw.dim_tempo (
    sk_tempo        INTEGER PRIMARY KEY,          -- AAAAMMDD
    data            DATE UNIQUE NOT NULL,
    ano             SMALLINT,
    trimestre       SMALLINT,
    mes             SMALLINT,
    nome_mes        TEXT,
    dia             SMALLINT,
    dia_semana      SMALLINT,
    nome_dia_semana TEXT,
    fim_de_semana   BOOLEAN
);
CREATE TABLE IF NOT EXISTS dw.dim_categoria (
    sk_categoria    SERIAL PRIMARY KEY,
    categoria       TEXT UNIQUE NOT NULL,
    qtd_produtos    INTEGER,
    atualizado_em   TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS dw.dim_marca (
    sk_marca        SERIAL PRIMARY KEY,
    marca           TEXT UNIQUE NOT NULL,
    qtd_produtos    INTEGER,
    atualizado_em   TIMESTAMPTZ DEFAULT now()
);

-- ══ 07 Auditoria ══
CREATE TABLE IF NOT EXISTS auditoria.execucoes (
    id            BIGSERIAL PRIMARY KEY,
    dag_id        TEXT NOT NULL,
    run_id        TEXT NOT NULL,
    ds            DATE NOT NULL,
    status        TEXT NOT NULL,
    tasks         JSONB,
    linhagem      JSONB,
    registrado_em TIMESTAMPTZ DEFAULT now()
);
