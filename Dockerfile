FROM apache/airflow:3.0.2

# Dependências do pipeline, travadas nas versões compatíveis com o Airflow 3.0.2
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir -r /tmp/requirements-airflow.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.0.2/constraints-3.12.txt"
