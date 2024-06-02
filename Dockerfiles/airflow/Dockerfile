FROM apache/airflow:2.7.1
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    python3-dev
USER airflow
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

