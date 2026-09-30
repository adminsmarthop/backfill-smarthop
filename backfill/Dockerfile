FROM python:3.13-alpine

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY run.py /app/run.py

RUN pip install --no-cache-dir requests

LABEL \
    io.hass.version="0.1.0" \
    io.hass.type="app" \
    io.hass.arch="amd64|aarch64"

CMD ["python3", "/app/run.py"]
