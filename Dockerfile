FROM python:3.12-alpine

RUN apk add --no-cache redis

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backup.py .

CMD ["python", "backup.py"]
