FROM python:3.13-alpine

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY run.py /app/run.py

RUN pip install --no-cache-dir requests

CMD ["python3", "/app/run.py"]
