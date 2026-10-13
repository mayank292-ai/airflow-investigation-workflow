FROM apache/airflow:3.3.1-python3.12
RUN pip install --no-cache-dir pytest==9.1.1
WORKDIR /patch
ENTRYPOINT ["python", "-m", "pytest", "-q", "-p", "no:cacheprovider", "/checks"]
