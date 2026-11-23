FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY worker/requirements.txt ./worker/requirements.txt
RUN pip install --no-cache-dir -r worker/requirements.txt

# Copy shared package and worker package
COPY shared/ ./shared/
COPY worker/ ./worker/

VOLUME ["/data/chunks"]
EXPOSE 8001

CMD ["uvicorn", "worker.app.main:app", "--host", "0.0.0.0", "--port", "8001"]
