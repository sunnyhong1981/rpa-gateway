FROM python:3.11-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt && groupadd --gid 10001 gateway && useradd --uid 10001 --gid gateway gateway
COPY rpa_gateway /app/rpa_gateway
USER 10001:10001
CMD ["uvicorn", "rpa_gateway.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8080", "--workers", "1", "--no-access-log", "--ws-max-size", "4096", "--limit-concurrency", "4096", "--backlog", "4096", "--ws-max-queue", "4", "--timeout-graceful-shutdown", "30"]

FROM runtime AS tests
USER root
COPY requirements-dev.txt /app/requirements-dev.txt
RUN pip install --no-cache-dir -r requirements-dev.txt
COPY tests /app/tests
COPY pyproject.toml /app/pyproject.toml
USER 10001:10001
CMD ["python", "-m", "pytest", "-q", "-p", "no:cacheprovider"]
