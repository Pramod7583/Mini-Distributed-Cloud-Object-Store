FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY controller/requirements.txt ./controller/requirements.txt
RUN pip install --no-cache-dir -r controller/requirements.txt

# Copy shared package and controller package
COPY shared/ ./shared/
COPY controller/ ./controller/

EXPOSE 8000

CMD ["uvicorn", "controller.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
