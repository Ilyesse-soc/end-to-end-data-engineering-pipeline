FROM apache/airflow:2.9.3-python3.11

COPY airflow/requirements.txt /requirements.txt
RUN pip uninstall -y apache-airflow-providers-google apache-airflow-providers-snowflake || true
RUN pip install --no-cache-dir -r /requirements.txt
