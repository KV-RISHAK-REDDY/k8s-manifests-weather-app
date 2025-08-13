# Base stage: install dependencies
FROM python:3.13-slim AS base

RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt

RUN pip install --upgrade pip && \
    pip install --prefix=/install --no-cache-dir -r /tmp/requirements.txt

# Final stage: copy only necessary files from base
FROM python:3.13-slim

WORKDIR /app

RUN useradd -m flask-server && \
    mkdir -p /app && \
    chown flask-server:flask-server /app

USER flask-server

COPY --from=base /install /usr/local

COPY svc1_streamlit.py streamlit_app.py

USER flask-server

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

ENTRYPOINT ["streamlit", "run", "streamlit_app.py"]
CMD ["--server.port=8501", "--server.address=0.0.0.0"]