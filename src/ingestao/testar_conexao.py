import boto3
import os
from dotenv import load_dotenv

load_dotenv()

cliente_s3 = boto3.client(
    "s3",
    region_name=os.getenv("AWS_REGION"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

nome_bucket = os.getenv("S3_BUCKET_NAME")
resposta = cliente_s3.list_objects_v2(Bucket=nome_bucket)

print(f"Conexão bem-sucedida com o bucket '{nome_bucket}'!")
print("Objetos encontrados:", resposta.get("KeyCount", 0))