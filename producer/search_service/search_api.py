import time
import sys
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError
from fastapi import FastAPI, Query

# Elasticsearch bağlantı bilgileri
ES_HOSTS = ["http://elasticsearch:9200"]
INDEX_NAME = "app_logs"

# FastAPI uygulamasını oluştur
app = FastAPI(title="Elasticsearch Arama API")

# Elasticsearch istemcisini global olarak tut
es_client = None

def connect_to_elasticsearch(hosts, max_retries=10, delay=5):
    """Elasticsearch'e bağlanmayı dener ve client'ı döndürür."""
    for attempt in range(max_retries):
        try:
            print(f"[{time.strftime('%H:%M:%S')}] ES'e bağlanma denemesi ({attempt + 1}/{max_retries})...")
            es = Elasticsearch(hosts)
            if es.ping():
                print("Elasticsearch bağlantısı BAŞARILI.")
                return es
            else:
                raise ConnectionError("Ping başarısız.")
        except ConnectionError:
            print(f"Elasticsearch hazır değil, {delay} saniye bekliyorum...")
            time.sleep(delay)
    
    print("HATA: Elasticsearch'a bağlanılamadı — API başlatılamıyor.")
    sys.exit(1)

# API başlatılmadan hemen önce ES bağlantısını kur
@app.on_event("startup")
async def startup_event():
    global es_client
    es_client = connect_to_elasticsearch(ES_HOSTS)

@app.get("/")
def read_root():
    return {"status": "Elasticsearch API çalışıyor!", "index": INDEX_NAME}

@app.get("/search")
async def search_logs(q: str = Query(..., description="Arama sorgusu"), level: str = None):
    """Elasticsearch'te mesajlar arasında arama yapar."""
    
    # Arama sorgusunu oluştur
    query_body = {
        "query": {
            "bool": {
                "must": [
                    # message alanında arama sorgusunu eşleştir
                    {"match": {"message": q}} 
                ]
            }
        }
    }
    
    # Eğer level filtresi varsa ekle
    if level:
        query_body["query"]["bool"]["filter"] = [
            {"term": {"level": level.upper()}}
        ]

    try:
        # Elasticsearch'te arama yap
        res = es_client.search(index=INDEX_NAME, body=query_body)
        
        return {
            "query": q,
            "level_filter": level,
            "total_hits": res['hits']['total']['value'],
            "results": [hit['_source'] for hit in res['hits']['hits']]
        }
    except Exception as e:
        return {"error": str(e), "message": "Arama sorgusu çalıştırılamadı."}

if __name__ == "__main__":
    import uvicorn
    # Bu kısmı Dockerfile CMD'sinde zaten başlattığımız için gerek yok, ancak yerel test için bırakılabilir.
    # uvicorn.run(app, host="0.0.0.0", port=8000)
    print("API Docker tarafından başlatılacak.")
