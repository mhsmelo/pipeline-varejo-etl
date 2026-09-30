"""02 — Data Lake (Bronze): JSON bruto -> Parquet + catálogo de metadados.

Nada é transformado aqui: listas/objetos aninhados viram texto JSON e todas as colunas
ficam como texto, para o bronze nunca perder informação.
"""
import json
from datetime import datetime, timezone

import pandas as pd

from etl import config
from etl.utils import particao, s3_get_json, s3_put_json, s3_put_parquet


def _para_texto(valor):
    if isinstance(valor, (list, dict)):
        return json.dumps(valor, ensure_ascii=False)
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return None
    return str(valor)


def salvar_datalake(raw_keys: dict[str, str], ds: str) -> dict[str, str]:
    bronze_keys, catalogo = {}, []
    ingerido_em = datetime.now(timezone.utc).isoformat()

    for entidade, key in raw_keys.items():
        df = pd.json_normalize(s3_get_json(key), max_level=1)  # address.city -> coluna própria
        df = df.apply(lambda col: col.map(_para_texto)).astype("object")
        df["_origem_arquivo"] = key
        df["_ingerido_em"] = ingerido_em

        destino = f"{config.PREFIX_BRONZE}/{entidade}/{particao(ds)}/{entidade}.parquet"
        bronze_keys[entidade] = s3_put_parquet(destino, df)
        catalogo.append({"entidade": entidade, "origem": key, "destino": destino,
                         "linhas": len(df), "colunas": list(df.columns), "dt": ds})

    s3_put_json(f"{config.PREFIX_BRONZE}/_catalogo/{particao(ds)}/catalogo.json", catalogo)
    return bronze_keys
