FROM python:3.13-slim
RUN apt-get update && apt-get install -y --no-install-recommends procps sysstat && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY src ./src
RUN pip install --no-cache-dir .
CMD ["uvicorn", "miniapm.instrumented_service:create_instrumented_app", "--factory", "--host", "0.0.0.0", "--port", "8080"]
