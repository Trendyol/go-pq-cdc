# Go-PQ-CDC-Kafka Scaling Rehberi

Bu rehber, `go-pq-cdc-kafka` servisinden birden fazla instance çalıştırmanız için gerekli adımları açıklar.

## Yapılan Değişiklikler

### 1. Docker Compose Değişiklikleri
- ✅ `container_name` kaldırıldı (Docker Compose her instance için otomatik isim verecek)
- ✅ Port mapping `expose` ile değiştirildi (port çakışmasını önlemek için)
- ✅ Metrics sadece internal network'te expose ediliyor

### 2. Prometheus Yapılandırması
- ✅ DNS service discovery eklendi
- ✅ Tüm scaled instance'lar otomatik olarak scrape edilecek

## Kullanım

### Birden Fazla Instance Başlatma

**3 instance ile başlatmak için:**
```bash
cd benchmark/benchmark_initial
docker-compose up --scale go-pq-cdc-kafka=3 -d
```

**5 instance ile başlatmak için:**
```bash
docker-compose up --scale go-pq-cdc-kafka=5 -d
```

### Çalışan Instance'ları Görüntüleme

```bash
docker-compose ps go-pq-cdc-kafka
```

Veya:
```bash
docker ps | grep go-pq-cdc-kafka
```

### Instance Sayısını Değiştirme

**Çalışırken scale etme (örn: 3'ten 5'e çıkarma):**
```bash
docker-compose up --scale go-pq-cdc-kafka=5 -d
```

**Scale down (örn: 5'ten 2'ye düşürme):**
```bash
docker-compose up --scale go-pq-cdc-kafka=2 -d
```

### Belirli Bir Instance'ın Loglarını İzleme

```bash
# Tüm instance'ların logları
docker-compose logs -f go-pq-cdc-kafka

# Belirli bir container
docker logs -f benchmark_initial_go-pq-cdc-kafka_1
docker logs -f benchmark_initial_go-pq-cdc-kafka_2
```

### Prometheus'ta Instance'ları Kontrol Etme

Prometheus UI'da (http://localhost:9090):
1. Status → Targets'a gidin
2. `go_pq_cdc_exporter` job'ını bulun
3. Tüm scaled instance'ların listelendiğini göreceksiniz

## Önemli Notlar

### ⚠️ Dikkat Edilmesi Gerekenler

1. **Snapshot Mode**: Şu anda her instance `SnapshotModeSnapshotOnly` modunda çalışıyor. Bu, her instance'ın veritabanından snapshot almaya çalışacağı anlamına gelir. Koordinasyon için dikkatli olun.

2. **Replication Slot**: Her instance aynı PostgreSQL replication slot'u kullanamaz. Eğer replication kullanıyorsanız, her instance için farklı slot isimleri gerekir.

3. **Kafka Partitions**: Birden fazla instance kullanıyorsanız, Kafka topic'inizin birden fazla partition'a sahip olması performans için önemlidir.

4. **Resource Limits**: Her instance için CPU ve memory limitleri şu şekilde:
   - CPU Limit: 1 core
   - Memory Limit: 512MB
   - 3 instance = toplam 3 core, 1.5GB RAM

### 📊 Monitoring

Grafana'da (http://localhost:3000) tüm instance'ların metrics'lerini görebilirsiniz:
- CPU kullanımı
- Memory kullanımı
- Kafka produce rate
- CDC lag

### 🔧 Troubleshooting

**Problem: Instance'lar başlamıyor**
```bash
# Logları kontrol edin
docker-compose logs go-pq-cdc-kafka

# Sağlık durumunu kontrol edin
docker-compose ps
```

**Problem: Prometheus instance'ları görmüyor**
```bash
# DNS çözümlemeyi test edin
docker-compose exec prometheus nslookup tasks.go-pq-cdc-kafka
```

**Problem: Resource yetersiz**
```bash
# Resource kullanımını kontrol edin
docker stats
```

## Alternatif: Manuel Instance Tanımlama

Eğer her instance için farklı yapılandırma istiyorsanız, docker-compose.yml'de manuel olarak tanımlayabilirsiniz:

```yaml
  go-pq-cdc-kafka-1:
    build:
      context: ../../
      dockerfile: ./benchmark/benchmark_initial/go-pq-cdc-kafka/Dockerfile
    # ... diğer ayarlar

  go-pq-cdc-kafka-2:
    build:
      context: ../../
      dockerfile: ./benchmark/benchmark_initial/go-pq-cdc-kafka/Dockerfile
    # ... diğer ayarlar

  go-pq-cdc-kafka-3:
    build:
      context: ../../
      dockerfile: ./benchmark/benchmark_initial/go-pq-cdc-kafka/Dockerfile
    # ... diğer ayarlar
```

## Performance İpuçları

1. **Batch Size**: `main.go`'da `ProducerBatchSize: 10000` ayarı var. Instance sayısıyla optimize edin.

2. **Chunk Size**: Snapshot için `ChunkSize: 8000` ayarı var. Veritabanı yüküne göre ayarlayın.

3. **Network**: Tüm servisler aynı Docker network'te olmalı.

4. **PostgreSQL**: WAL (Write-Ahead Log) ayarları yeterli olmalı:
   ```
   wal_level=logical
   max_wal_senders=10
   max_replication_slots=10
   ```

## Örnek Senaryolar

### Senaryo 1: Yüksek Throughput Test
```bash
# 5 instance ile başlat
docker-compose up --scale go-pq-cdc-kafka=5 -d

# Test verisi ekle
docker-compose exec postgres psql -U cdc_user -d cdc_db -c \
  "INSERT INTO users (name) SELECT 'User' || i FROM generate_series(1, 1000000) AS i;"

# Performance'ı izle
docker stats
```

### Senaryo 2: Graceful Scale Down
```bash
# Önce mevcut instance'ları göster
docker-compose ps go-pq-cdc-kafka

# Yavaşça scale down
docker-compose up --scale go-pq-cdc-kafka=3 -d
sleep 30
docker-compose up --scale go-pq-cdc-kafka=1 -d
```

### Senaryo 3: Load Testing
```bash
# Farklı instance sayılarıyla test
for i in 1 2 3 5 10; do
  echo "Testing with $i instances..."
  docker-compose up --scale go-pq-cdc-kafka=$i -d
  sleep 60
  # Metrics'leri kaydet
done
```

## Kaynaklar

- Docker Compose Scale: https://docs.docker.com/compose/reference/up/
- Prometheus DNS SD: https://prometheus.io/docs/prometheus/latest/configuration/configuration/#dns_sd_config
- Kafka Partitioning: https://kafka.apache.org/documentation/#intro_concepts_and_terms

