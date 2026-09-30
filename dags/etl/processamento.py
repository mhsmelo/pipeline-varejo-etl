"""03 — Processamento (Silver): limpeza -> enriquecimento -> validação de regras."""
import json
import logging

import pandas as pd

from etl import config
from etl.utils import particao, s3_get_parquet, s3_put_parquet

log = logging.getLogger(__name__)

# De/para: nome na API (bronze) -> nome no modelo (silver). Só o que está aqui segue adiante.
COLUNAS = {
    "produtos": {
        "id": "id_produto", "title": "nome_produto", "description": "descricao",
        "category": "categoria", "brand": "marca", "sku": "sku", "price": "preco",
        "discountPercentage": "desconto_percentual", "rating": "avaliacao",
        "stock": "estoque", "availabilityStatus": "status_disponibilidade",
    },
    "clientes": {
        "id": "id_cliente", "firstName": "nome", "lastName": "sobrenome", "email": "email",
        "age": "idade", "gender": "genero", "birthDate": "data_nascimento",
        "address.city": "cidade", "address.state": "estado",
        "address.stateCode": "uf", "address.country": "pais",
    },
    # vendas: cada carrinho é "explodido" em uma linha por produto (ver _explodir_carrinhos)
}
NUMERICOS = ["preco", "desconto_percentual", "avaliacao", "estoque", "idade",
             "quantidade", "valor_bruto", "valor_total"]
INTEIROS_ID = ["id_produto", "id_cliente", "id_carrinho"]
DATAS = ["data_nascimento"]


def _key(entidade, etapa, ds):
    return f"{config.PREFIX_SILVER}/{etapa}/{entidade}/{particao(ds)}/{entidade}.parquet"


def _explodir_carrinhos(df: pd.DataFrame) -> pd.DataFrame:
    linhas = []
    for _, carrinho in df.iterrows():
        for item in json.loads(carrinho["products"] or "[]"):
            linhas.append({
                "id_carrinho": carrinho["id"],
                "id_cliente": carrinho["userId"],
                "id_produto": item.get("id"),
                "nome_produto": item.get("title"),
                "preco": item.get("price"),
                "quantidade": item.get("quantity"),
                "valor_bruto": item.get("total"),
                "desconto_percentual": item.get("discountPercentage"),
                "valor_total": item.get("discountedTotal"),
            })
    return pd.DataFrame(linhas)


# ── limpeza_dados ─────────────────────────────────────────────────────
def limpeza_dados(keys: dict[str, str], ds: str) -> dict[str, str]:
    out = {}
    for entidade, key in keys.items():
        bruto = s3_get_parquet(key)
        if entidade == "vendas":
            df = _explodir_carrinhos(bruto)
        else:
            mapa = COLUNAS[entidade]
            df = bruto[[c for c in mapa if c in bruto]].rename(columns=mapa)

        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].astype("string").str.strip().replace({"": pd.NA})
        for col in NUMERICOS:
            if col in df:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        for col in INTEIROS_ID:
            if col in df:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        for col in DATAS:
            if col in df:
                df[col] = pd.to_datetime(df[col], errors="coerce", format="mixed").dt.date

        chave = config.SCHEMAS[entidade]["chave"]
        antes = len(df)
        df = df.drop_duplicates(subset=chave, keep="last")
        log.info("%s: %d linhas, %d duplicadas removidas", entidade, len(df), antes - len(df))
        out[entidade] = s3_put_parquet(_key(entidade, "limpo", ds), df)
    return out


# ── enriquecimento ────────────────────────────────────────────────────
def _faixa_etaria(idade):
    if pd.isna(idade):
        return None
    for limite, faixa in [(25, "18-24"), (35, "25-34"), (45, "35-44"), (60, "45-59")]:
        if idade < limite:
            return faixa
    return "60+"


def enriquecimento(keys: dict[str, str], ds: str) -> dict[str, str]:
    dfs = {e: s3_get_parquet(k) for e, k in keys.items()}
    data = pd.to_datetime(ds).date()

    p = dfs["produtos"]
    p["preco_com_desconto"] = (p["preco"] * (1 - p["desconto_percentual"].fillna(0) / 100)).round(2)

    c = dfs["clientes"]
    c["nome_completo"] = (c["nome"].fillna("") + " " + c["sobrenome"].fillna("")).str.strip()
    c["email"] = c["email"].str.lower()
    c["faixa_etaria"] = c["idade"].map(_faixa_etaria)

    v = dfs["vendas"]
    v["data_venda"] = data  # a DummyJSON não informa data do carrinho: usamos a data da extração
    v["valor_desconto"] = (v["valor_bruto"] - v["valor_total"]).round(2)

    # Estoque: fotografia diária do saldo de cada produto (vira histórico na camada RAW)
    dfs["estoque"] = p[["id_produto", "estoque", "preco", "status_disponibilidade"]].rename(
        columns={"estoque": "quantidade"})
    dfs["estoque"]["data_referencia"] = data

    out = {}
    for entidade, df in dfs.items():
        df["dt_carga"] = data
        out[entidade] = s3_put_parquet(_key(entidade, "enriquecido", ds), df)
    return out


# ── validacao_regras ──────────────────────────────────────────────────
def validacao_regras(keys: dict[str, str], ds: str) -> dict[str, str]:
    """Separa válidos e rejeitados; falha a task se a rejeição passar do limite."""
    out = {}
    for entidade, key in keys.items():
        df = s3_get_parquet(key)
        schema = config.SCHEMAS[entidade]

        faltando = [c for c in schema["obrigatorias"] if c not in df]
        if faltando:
            raise ValueError(f"{entidade}: colunas obrigatórias ausentes {faltando}")

        invalido = df[schema["obrigatorias"]].isna().any(axis=1)
        if entidade == "vendas":
            invalido |= (df["quantidade"] <= 0) | (df["preco"] < 0)
        elif entidade == "produtos":
            invalido |= df["preco"] < 0
        elif entidade == "estoque":
            invalido |= df["quantidade"] < 0
        elif entidade == "clientes":
            invalido |= ~df["email"].fillna("").str.contains("@")

        rejeitados, validos = df[invalido], df[~invalido]
        taxa = len(rejeitados) / max(len(df), 1)
        log.info("%s: %d válidos, %d rejeitados (%.1f%%)",
                 entidade, len(validos), len(rejeitados), taxa * 100)
        if len(rejeitados):
            s3_put_parquet(_key(entidade, "rejeitados", ds), rejeitados)
        if taxa > config.LIMITE_REJEICAO:
            raise ValueError(f"{entidade}: rejeição de {taxa:.1%} acima do limite "
                             f"de {config.LIMITE_REJEICAO:.0%}")
        out[entidade] = s3_put_parquet(_key(entidade, "validado", ds), validos)
    return out
