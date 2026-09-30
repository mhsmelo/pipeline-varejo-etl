"""01 — Ingestão: API DummyJSON -> s3://bucket/raw/<entidade>/dt=AAAA-MM-DD/<entidade>.json

É a evolução do seu src/ingestao/extrair_produtos.py, agora para as três entidades.
"""
import logging
import time

import requests

from etl import config
from etl.utils import particao, s3_put_json

log = logging.getLogger(__name__)


def buscar_api(entidade: str, session: requests.Session | None = None) -> list[dict]:
    rota, chave_lista, params = config.API_ENDPOINTS[entidade]
    session = session or requests.Session()
    url = config.API_BASE_URL + rota

    for tentativa in range(4):
        resp = session.get(url, params=params, timeout=60)
        if resp.status_code == 429 or resp.status_code >= 500:
            espera = 2 ** tentativa
            log.warning("%s: HTTP %s, nova tentativa em %ss", entidade, resp.status_code, espera)
            time.sleep(espera)
            continue
        resp.raise_for_status()
        break
    else:
        resp.raise_for_status()

    payload = resp.json()
    registros = payload.get(chave_lista)
    validar_resposta(entidade, registros, payload)
    log.info("%s: %d registros (total informado pela API: %s)",
             entidade, len(registros), payload.get("total"))
    return registros


def validar_resposta(entidade: str, registros, payload: dict):
    if not isinstance(registros, list):
        raise ValueError(f"{entidade}: resposta sem a lista esperada. Chaves: {list(payload)}")
    if registros and not isinstance(registros[0], dict):
        raise ValueError(f"{entidade}: itens da lista não são objetos JSON")
    if not registros:
        raise ValueError(f"{entidade}: API retornou lista vazia")


def extracao_s3(ds: str) -> dict[str, str]:
    """Retorna {entidade: key} para o XCom (só o caminho no S3, nunca os dados)."""
    session = requests.Session()
    keys = {}
    for entidade in config.API_ENDPOINTS:
        registros = buscar_api(entidade, session)
        key = f"{config.PREFIX_RAW}/{entidade}/{particao(ds)}/{entidade}.json"
        keys[entidade] = s3_put_json(key, registros)
    return keys
