FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir psycopg2-binary
COPY cleaner.py ./
COPY cleaner_config.json ./
CMD ["python3", "-u", "cleaner.py"]
