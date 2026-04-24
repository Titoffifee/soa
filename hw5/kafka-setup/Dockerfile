FROM python:3.12-slim

RUN pip install --no-cache-dir confluent-kafka requests

COPY kafka_init.py /scripts/kafka_init.py

CMD ["python3", "/scripts/kafka_init.py"]
