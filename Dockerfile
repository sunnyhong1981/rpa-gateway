FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt && groupadd --gid 10001 gateway && useradd --uid 10001 --gid gateway gateway
COPY rpa_gateway /app/rpa_gateway
USER 10001:10001
CMD ["uvicorn", "rpa_gateway.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8080", "--workers", "1", "--no-access-log", "--ws-max-size", "4096", "--limit-concurrency", "8"]
