FROM python:3.13-slim AS builder

# System setup
RUN apt-get update && apt-get install -y \
    curl \
    libpq-dev \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

# Install dependencies as root
RUN pip install --upgrade pip && \
    pip install --prefix=/install --no-cache-dir -r requirements.txt

# Final image
FROM python:3.13-slim

# Create user and app directory
RUN useradd -m flask-server && mkdir -p /app && chown flask-server:flask-server /app

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY app.py gunicorn_config.py .

USER flask-server

EXPOSE 8080

ENTRYPOINT ["gunicorn"]
CMD ["-b", "0.0.0.0:8080", "-c", "gunicorn_config.py", "app:app"]