# 🚀 Snapshot Test - Quick Start

Manuel test için hazır setup. Tüm test case'leri buradan çalıştırabilirsin.

## 📦 Setup

```bash
# 1. PostgreSQL başlat
make db-up

# 2. Build
make build
```

## 🎯 Test Akışı

### Test 1.1: Tek Tablo, Tek Chunk

```bash
# Setup
make test-setup-1

# Run CDC
./snapshot-test

# Verify
make verify
# veya
./test-helper.sh verify-1.1

# CDC test (başka terminal'de)
./test-helper.sh cdc-insert users
./test-helper.sh cdc-update users
./test-helper.sh cdc-delete users
```

### Test 1.2: Tek Tablo, Birden Fazla Chunk

```bash
make test-setup-2
./snapshot-test
make verify
```

### Test 1.3: Birden Fazla Tablo

```bash
make test-setup-3
./snapshot-test
make verify
```

### Test 2.1: Crash & Resume

```bash
# Setup
make test-setup-crash

# Run CDC (background)
./snapshot-test > snapshot-test.log 2>&1 &

# 2-3 saniye bekle
sleep 3

# Status kontrol
./test-helper.sh chunks-status

# Crash simülasyonu
./test-helper.sh crash

# State kontrol
./test-helper.sh chunks-status
./test-helper.sh job-status

# Resume
./snapshot-test

# Verify tüm chunk'lar completed
make verify
```

### Test 3.2: Empty Table

```bash
make test-setup-empty
./snapshot-test
make verify
```

### Test 3.3: No Primary Key

```bash
make test-setup-no-pk
./snapshot-test
make verify
```

## 🛠️ Utility Commands

```bash
# Database shell
make db-shell

# Status kontrol
make status

# Job status
./test-helper.sh job-status

# Chunks status
./test-helper.sh chunks-status

# Tüm chunk'ları göster
./test-helper.sh all-chunks

# Stale chunk'ları bul
./test-helper.sh stale-chunks

# Clean & restart
make db-clean
make db-restart
```

## 🧪 CDC Test Commands

```bash
# INSERT test
./test-helper.sh cdc-insert users

# UPDATE test
./test-helper.sh cdc-update users

# DELETE test
./test-helper.sh cdc-delete users

# Diğer tablolar için
./test-helper.sh cdc-insert orders
./test-helper.sh cdc-update products
```

## 📊 Metrics

```bash
# Prometheus metrics
curl http://localhost:8081/metrics | grep snapshot
```

## 🔄 Test Tekrarı

```bash
# Her test öncesi temizlik
make db-clean

# Yeni test setup
make test-setup-1  # veya test-setup-2, test-setup-3, etc.

# Run
./snapshot-test
```

## 📝 Config Ayarları

`config.yml` dosyasında:

```yaml
snapshot:
  chunkSize: 100          # Test case'e göre değiştir
  claimTimeout: 30s
  heartbeatInterval: 10s
```

## 🐛 Troubleshooting

```bash
# PostgreSQL durumu
docker-compose ps

# PostgreSQL logs
docker-compose logs -f postgres

# Database bağlantı testi
make db-shell

# Slot durumu
psql -c "SELECT * FROM pg_replication_slots;"

# Publication durumu
psql -c "SELECT * FROM pg_publication_tables;"
```

