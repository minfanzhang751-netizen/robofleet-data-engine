# LLM client image (Module 3 mock classifier → Module 4-swappable backend).
# Build from repo root:
#   docker build -t robofleet-llm-client:local -f services/llm_client/Dockerfile .
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY services /app/services
ENV PYTHONPATH=/app
ENV KAFKA_BOOTSTRAP_SERVERS=robofleet-kafka-bootstrap.kafka.svc:9092
ENV KAFKA_INPUT_TOPIC=robot-telemetry-windows
ENV KAFKA_OUTPUT_TOPIC=robot-diagnostics-classified
ENV KAFKA_GROUP_ID=robofleet-llm-client

CMD ["python", "-m", "services.llm_client.main"]
