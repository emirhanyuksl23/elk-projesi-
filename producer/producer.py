import time
import sys
import json
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError

# Elasticsearch bağlantı bilgileri
ES_HOSTS = ["http://elasticsearch:9200"]
INDEX_NAME = "app_logs"

def connect_to_elasticsearch(hosts, max_retries=10, delay=5):
    """Elasticsearch'e bağlanmayı dener."""
    for attempt in range(max_retries):
        try:
            print(f"[{time.strftime('%H:%M:%S')}] Elasticsearch'a bağlanma denemesi ({attempt + 1}/{max_retries})...")
            es = Elasticsearch(hosts)
            if es.ping():
                print("Elasticsearch bağlantısı BAŞARILI.")
                return es
            else:
                raise ConnectionError("Ping başarısız.")
        except ConnectionError:
            print(f"Elasticsearch hazır değil, {delay} saniye bekliyorum...")
            time.sleep(delay)
    
    print("HATA: Elasticsearch'a bağlanılamadı — program sonlandırılıyor.")
    sys.exit(1)


def start_producing():
    """Sürekli olarak log verisi üretir ve ES'e gönderir."""
    es_client = connect_to_elasticsearch(ES_HOSTS)
    
    # İndeks yoksa oluştur
    if not es_client.indices.exists(index=INDEX_NAME):
        es_client.indices.create(index=INDEX_NAME)
        print(f"'{INDEX_NAME}' indeksi oluşturuldu.")
        
    log_id = 0
    print(f" [*] Her 3 saniyede bir '{INDEX_NAME}' indeksine log gönderiliyor. Çıkış için CTRL+C.")

    try:
        while True:
            log_id += 1
            timestamp = time.strftime('%Y-%m-%dT%H:%M:%S')
            
            # Gönderilecek log belgesi (JSON formatında)
            log_doc = {
                "log_id": log_id,
                "timestamp": timestamp,
                "level": "INFO" if log_id % 3 != 0 else "ERROR",
                "message": f"Kullanıcı {log_id % 5} bir işlem gerçekleştirdi.",
                "user_id": log_id % 10
            }
            
            # Belgeyi Elasticsearch'e gönder (indeksleme)
            es_client.index(index=INDEX_NAME, document=log_doc)
            
            print(f" [x] Log gönderildi: ID {log_id}, Level: {log_doc['level']}")
            
            time.sleep(3)

    except KeyboardInterrupt:
        print('\nProgram durduruldu.')
    finally:
        print('Producer sonlandı.')


if __name__ == "__main__":
    start_producing()
