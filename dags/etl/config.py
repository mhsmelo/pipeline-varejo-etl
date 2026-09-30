"""Configuração central, lida das variáveis de ambiente (arquivo .env do projeto)."""
import os

# ── AWS / S3 ──────────────────────────────────────────────────────────
S3_BUCKET = os.getenv("S3_BUCKET_NAME", "")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL") or None  # opcional: LocalStack/MinIO

# Camadas do Data Lake. "raw" mantém o padrão que você já usa: raw/produtos/dt=AAAA-MM-DD/
PREFIX_RAW = "raw"
PREFIX_BRONZE = "bronze"
PREFIX_SILVER = "silver"

# ── API (DummyJSON) ───────────────────────────────────────────────────
API_BASE_URL = os.getenv("API_BASE_URL", "https://dummyjson.com")

# entidade -> (rota, chave da lista no JSON, parâmetros)
# Em /users, o "select" evita trazer senha, cartão, IP etc. para o Data Lake.
API_ENDPOINTS = {
    "produtos": ("/products", "products", {"limit": 0}),
    "clientes": ("/users", "users", {
        "limit": 0,
        "select": "firstName,lastName,email,age,gender,birthDate,address",
    }),
    "vendas": ("/carts", "carts", {"limit": 0}),
}

# ── PostgreSQL (Data Warehouse) ───────────────────────────────────────
POSTGRES_CONN_ID = "postgres_dw"  # definida no docker-compose (AIRFLOW_CONN_POSTGRES_DW)
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "host=localhost port=5433 dbname=varejo user=dw password=dw")

# ── Regras por entidade (após a limpeza) ──────────────────────────────
SCHEMAS = {
    "produtos": {"chave": ["id_produto"],
                 "obrigatorias": ["id_produto", "nome_produto", "categoria", "preco"]},
    "clientes": {"chave": ["id_cliente"],
                 "obrigatorias": ["id_cliente", "nome", "email"]},
    "vendas":   {"chave": ["id_carrinho", "id_produto"],
                 "obrigatorias": ["id_carrinho", "id_cliente", "id_produto", "quantidade", "preco"]},
    "estoque":  {"chave": ["id_produto", "data_referencia"],
                 "obrigatorias": ["id_produto", "data_referencia", "quantidade"]},
}
LIMITE_REJEICAO = float(os.getenv("LIMITE_REJEICAO", "0.05"))
