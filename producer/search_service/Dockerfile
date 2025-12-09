FROM python:3.12-slim 
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# CMD komutu, uvicorn ile FastAPI uygulamasını başlatır
CMD ["uvicorn", "search_api:app", "--host", "0.0.0.0", "--port", "8000"]
