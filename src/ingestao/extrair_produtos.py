import requests
import boto3
import os
import json
from datetime import date
from dotenv import load_dotenv

load_dotenv()


def buscar_produtos():
    """Busca os produtos na API DummyJSON e retorna os dados como dict."""
    resposta = requests.get("https://dummyjson.com/products?limit=0")
    resposta.raise_for_status()
    return resposta.json()


def montar_chave_s3(data_execucao):
    """Monta o caminho (key) do arquivo no S3, baseado na data de execução."""
    return f"raw/produtos/dt={data_execucao}/produtos.json"


def salvar_no_s3(cliente_s3, bucket, chave, dados):
    """Salva os dados (dict) como JSON no S3, na chave especificada."""
    cliente_s3.put_object(
        Bucket=bucket,
        Key=chave,
        Body=json.dumps(dados, ensure_ascii=False),
        ContentType="application/json"
    )


if __name__ == "__main__":
    cliente_s3 = boto3.client(
        "s3",
        region_name=os.getenv("AWS_REGION"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    bucket = os.getenv("S3_BUCKET_NAME")
    data_execucao = date.today().isoformat()

    produtos = buscar_produtos()
    chave = montar_chave_s3(data_execucao)
    salvar_no_s3(cliente_s3, bucket, chave, produtos)

    print(f"Extração concluída! Salvo em: s3://{bucket}/{chave}")