# 📋 go-pq-cdc Snapshot Test Plan (Single Instance)

## 🎯 Test Stratejisi

Bu doküman, chunk-based snapshot implementation'ının **single instance** modunda test edilmesi için hazırlanmıştır. Her test case:
- ✅ **Beklenen sonuç** (Expected)
- 🧪 **Nasıl test edilir** (How to test)
- 📊 **Verification query'leri**

---

## 📑 Test Kategorileri

1. [Happy Path Tests](#1-happy-path-tests) - Temel senaryolar
2. [Failure & Recovery Tests](#2-failure--recovery-tests) - Hata durumları
3. [Data Consistency Tests](#3-data-consistency-tests) - Veri tutarlılığı
4. [Edge Cases](#4-edge-cases) - Sınır durumları
5. [Observability Tests](#5-observability-tests) - Metrik ve log kontrolü

---

## 1. Happy Path Tests

### ✅ Test 1.1: Tek Tablo, Tek Chunk Snapshot

**TODO**: [ ] Test edildi

**Beklenti**:
- 1 table için 1 chunk oluşturulur
- Chunk `pending` → `in_progress` → `completed` state'lerinden geçer
- Job `completed=true` olur
- Snapshot LSN kaydedilir
- CDC, snapshot LSN'den başlar

**Nasıl Test Edilir**:
```bash
# 1. Küçük bir tablo oluştur (chunkSize'dan az row)
psql -c "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, email TEXT);"
psql -c "INSERT INTO users SELECT i, 'user_' || i, 'user' || i || '@test.com' FROM generate_series(1, 50) i;"

# 2. Config: chunkSize=100 (50 row < 100, tek chunk olur)
# 3. CDC başlat
./go-pq-cdc --config config.yml

# 4. Verify
```

**Verification**:
```sql
-- Job completed olmalı
SELECT completed, total_chunks, completed_chunks 
FROM cdc_snapshot_job 
WHERE slot_name = 'your_slot_name';
-- Expected: completed=true, total_chunks=1, completed_chunks=1

-- Chunk completed olmalı
SELECT table_name, status, rows_processed 
FROM cdc_snapshot_chunks;
-- Expected: 1 row, status='completed', rows_processed=50

-- Snapshot LSN kaydedildi mi?
SELECT snapshot_lsn FROM cdc_snapshot_job;
-- Expected: Bir LSN değeri (örn: '0/123456')
```

**Logs'da Ara**:
```
✅ "chunk-based snapshot starting"
✅ "instance elected as coordinator"
✅ "coordinator: chunks created" table=users chunks=1
✅ "claimed chunk" table=users
✅ "chunk completed" rowsProcessed=50
✅ "all chunks completed"
✅ "snapshot completed"
✅ "CDC will continue from snapshot LSN"
```

---

### ✅ Test 1.2: Tek Tablo, Birden Fazla Chunk

**TODO**: [ ] Test edildi

**Beklenti**:
- 1 table için N chunk oluşturulur (N = ceiling(rowCount / chunkSize))
- Her chunk sırayla `chunk_index` order'ında işlenir
- Toplam rows_processed = table row count

**Nasıl Test Edilir**:
```bash
# 1. Büyük tablo oluştur
psql -c "CREATE TABLE orders (id SERIAL PRIMARY KEY, user_id INT, amount DECIMAL, created_at TIMESTAMP);"
psql -c "INSERT INTO orders SELECT i, (i % 100) + 1, random() * 1000, NOW() FROM generate_series(1, 500) i;"

# 2. Config: chunkSize=100 (500 rows → 5 chunks)
# 3. CDC başlat
```

**Verification**:
```sql
-- 5 chunk oluşturuldu mu?
SELECT COUNT(*) FROM cdc_snapshot_chunks WHERE table_name='orders';
-- Expected: 5

-- Her chunk'ın row_processed toplamı
SELECT SUM(rows_processed) FROM cdc_snapshot_chunks WHERE table_name='orders';
-- Expected: 500

-- Chunk order doğru mu?
SELECT chunk_index, chunk_start, chunk_size, rows_processed, status
FROM cdc_snapshot_chunks 
WHERE table_name='orders'
ORDER BY chunk_index;
-- Expected: chunk_index = 0,1,2,3,4; chunk_start = 0,100,200,300,400
```

**Logs'da Ara**:
```
✅ "coordinator: chunks created" table=orders chunks=5
✅ "claimed chunk" chunkIndex=0
✅ "chunk completed" chunkIndex=0
✅ ... (5 kez)
```

---

### ✅ Test 1.3: Birden Fazla Tablo Snapshot

**TODO**: [ ] Test edildi

**Beklenti**:
- Her table için chunk'lar oluşturulur
- Total chunks = sum(all table chunks)
- Her table'ın chunk'ları parallel değil, sequential işlenir (single instance)

**Nasıl Test Edilir**:
```bash
# 1. Multiple tables
psql -c "CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT);"
psql -c "INSERT INTO users SELECT i, 'user_' || i FROM generate_series(1, 150) i;"

psql -c "CREATE TABLE products (id SERIAL PRIMARY KEY, name TEXT, price DECIMAL);"
psql -c "INSERT INTO products SELECT i, 'product_' || i, i * 10.5 FROM generate_series(1, 80) i;"

# 2. Config: chunkSize=100
# users: 150 rows → 2 chunks
# products: 80 rows → 1 chunk
# Total: 3 chunks

# 3. CDC başlat
```

**Verification**:
```sql
-- Total 3 chunk var mı?
SELECT total_chunks, completed_chunks FROM cdc_snapshot_job;
-- Expected: total_chunks=3, completed_chunks=3

-- Table breakdown
SELECT table_name, COUNT(*) as chunk_count, SUM(rows_processed) as total_rows
FROM cdc_snapshot_chunks
GROUP BY table_name;
-- Expected: 
--   users     | 2 | 150
--   products  | 1 | 80
```

**Logs'da Ara**:
```
✅ "coordinator: chunks created" table=users chunks=2
✅ "coordinator: chunks created" table=products chunks=1
✅ "coordinator: job initialized" totalChunks=3
```

---

### ✅ Test 1.4: Snapshot Bitince CDC Devam Ediyor (No Duplicate)

**TODO**: [ ] Test edildi

**Beklenti**:
- Snapshot biter, LSN=X kaydedilir
- CDC `START_REPLICATION ... LSN X` ile başlar
- Snapshot'taki data duplicate olarak CDC'de gelmez
- Sadece snapshot sonrası INSERT/UPDATE/DELETE gelir

**Nasıl Test Edilir**:
```bash
# 1. Tablo oluştur
psql -c "CREATE TABLE test_cdc (id SERIAL PRIMARY KEY, value TEXT);"
psql -c "INSERT INTO test_cdc SELECT i, 'snapshot_' || i FROM generate_series(1, 100) i;"

# 2. CDC başlat, snapshot al
./go-pq-cdc --config config.yml

# 3. Snapshot bitene kadar bekle (logs: "snapshot completed")

# 4. Snapshot LSN'i kaydet
SNAPSHOT_LSN=$(psql -tAc "SELECT snapshot_lsn FROM cdc_snapshot_job;")
echo "Snapshot LSN: $SNAPSHOT_LSN"

# 5. CDC çalışırken yeni data ekle
psql -c "INSERT INTO test_cdc VALUES (101, 'cdc_new');"
psql -c "UPDATE test_cdc SET value='cdc_updated' WHERE id=50;"
psql -c "DELETE FROM test_cdc WHERE id=25;"

# 6. Consumer'da event'leri kontrol et
```

**Consumer'da Beklenen Event Sırası**:
```
1. BEGIN (snapshot)
2. 100x DATA events (id=1..100, value='snapshot_X')
3. END (snapshot)
--- SNAPSHOT BİTTİ ---
4. INSERT (id=101, value='cdc_new')
5. UPDATE (id=50, old='snapshot_50', new='cdc_updated')
6. DELETE (id=25)
```

**❌ HATA: Duplicate Event Kontrolü**:
```
# Eğer id=1..100 arasında tekrar DATA eventi gelirse → BUG!
# Snapshot LSN düzgün set edilmemiş demektir
```

**Verification**:
```sql
-- CDC streaming'de kullanılan LSN
-- Check logs: "CDC will continue from snapshot LSN: 0/XXXXX"
```

---

### ✅ Test 1.5: Completed Snapshot Skip Ediliyor

**TODO**: [ ] Test edildi

**Beklenti**:
- Job completed=true ise
- Yeni start'ta snapshot atlanır
- Direkt CDC başlar
- "snapshot already completed" log'u görülür

**Nasıl Test Edilir**:
```bash
# 1. İlk run: Snapshot tamamla
./go-pq-cdc --config config.yml
# Bekle: "snapshot completed" log'u gelene kadar

# 2. Stop et (Ctrl+C)

# 3. Tekrar başlat
./go-pq-cdc --config config.yml
```

**Logs'da Ara**:
```
✅ "snapshot already completed"
✅ "CDC will continue from snapshot LSN" (hemen başlamalı)
❌ "chunk-based snapshot starting" (OLMAMALI!)
❌ "instance elected as coordinator" (OLMAMALI!)
```

**Verification**:
```sql
-- Job hala completed=true olmalı
SELECT completed FROM cdc_snapshot_job;
-- Expected: true

-- Chunk sayısı değişmemeli
SELECT COUNT(*) FROM cdc_snapshot_chunks;
-- Expected: Öncekiyle aynı
```

---

## 2. Failure & Recovery Tests

### 🔄 Test 2.1: Snapshot Ortasında Crash, Resume

**TODO**: [ ] Test edildi

**Beklenti**:
- Process crash olur
- Bazı chunk'lar `completed`, bazıları `pending` veya `in_progress` kalır
- Restart sonrası: Completed chunk'lar skip edilir, pending/in_progress chunk'lar işlenir
- Job completion doğru hesaplanır

**Nasıl Test Edilir**:
```bash
# 1. Büyük tablo oluştur
psql -c "CREATE TABLE crash_test (id SERIAL PRIMARY KEY, data TEXT);"
psql -c "INSERT INTO crash_test SELECT i, md5(random()::text) FROM generate_series(1, 500) i;"

# 2. Config: chunkSize=100 (5 chunks)

# 3. CDC başlat
./go-pq-cdc --config config.yml &
PID=$!

# 4. 2-3 saniye bekle (2-3 chunk tamamlansın)
sleep 3

# 5. Crash simülasyonu (SIGKILL)
kill -9 $PID

# 6. State'i kontrol et
```

**Verification (Crash Sonrası)**:
```sql
-- Bazı chunk'lar completed, bazıları pending/in_progress
SELECT status, COUNT(*) 
FROM cdc_snapshot_chunks 
WHERE table_name='crash_test'
GROUP BY status;
-- Expected: Örneğin 2 completed, 3 pending/in_progress

-- Job completed=false olmalı
SELECT completed FROM cdc_snapshot_job;
-- Expected: false
```

**Resume Test**:
```bash
# 7. Restart
./go-pq-cdc --config config.yml
```

**Logs'da Ara**:
```
✅ "job loaded" (existing job buldu)
✅ "claimed chunk" chunkIndex=2,3,4 (pending olanları aldı)
❌ "claimed chunk" chunkIndex=0,1 (completed olanları ALMAMALI)
✅ "all chunks completed"
✅ "snapshot completed"
```

**Verification (Resume Sonrası)**:
```sql
-- Tüm chunk'lar completed
SELECT COUNT(*) FROM cdc_snapshot_chunks 
WHERE table_name='crash_test' AND status='completed';
-- Expected: 5

-- Job completed=true
SELECT completed FROM cdc_snapshot_job;
-- Expected: true
```

---

### 🔄 Test 2.2: Heartbeat Timeout, Chunk Reclaim

**TODO**: [ ] Test edildi

**Beklenti**:
- Worker chunk işlerken heartbeat gönderemez (network issue, freeze, etc.)
- `heartbeat_at < NOW() - claimTimeout` koşulu sağlanır
- Başka bir instance (veya aynı instance restart'ta) stale chunk'ı reclaim eder

**Nasıl Test Edilir**:
```bash
# 1. Config ayarları:
#    claimTimeout: 30s
#    heartbeatInterval: 10s

# 2. Tablo oluştur
psql -c "CREATE TABLE heartbeat_test (id SERIAL PRIMARY KEY, data TEXT);"
psql -c "INSERT INTO heartbeat_test SELECT i, repeat('x', 10000) FROM generate_series(1, 200) i;"

# 3. CDC başlat
./go-pq-cdc --config config.yml &
PID=$!

# 4. 1 chunk claim edilene kadar bekle
sleep 2

# 5. Process'i STOP (heartbeat dursun ama process ölmesin)
kill -STOP $PID

# 6. 35 saniye bekle (claimTimeout=30s geçsin)
sleep 35
```

**Verification (STOP Durumunda)**:
```sql
-- 1 chunk in_progress, heartbeat_at eski
SELECT id, status, claimed_by, heartbeat_at, 
       NOW() - heartbeat_at as stale_duration
FROM cdc_snapshot_chunks
WHERE status='in_progress';
-- Expected: 1 row, stale_duration > 30 seconds
```

**Resume Test**:
```bash
# 7. Process'i devam ettir veya yeni instance başlat
kill -CONT $PID
# veya
kill -9 $PID && ./go-pq-cdc --config config.yml
```

**Logs'da Ara**:
```
✅ "claimed chunk" (stale chunk reclaim edildi)
✅ "chunk completed"
```

**Verification (Sonra)**:
```sql
-- Stale chunk reclaim edilip completed olmalı
SELECT status FROM cdc_snapshot_chunks WHERE id=<stale_chunk_id>;
-- Expected: completed
```

---

### 🔄 Test 2.3: markJobAsCompleted() Fail, Auto-Fix

**TODO**: [ ] Test edildi

**Beklenti**:
- Tüm chunk'lar completed
- `markJobAsCompleted()` DB error alır (job completed=false kalır)
- Restart sonrası: `setupJob()` içinde auto-fix yapılır
- Job completed=true olur, snapshot skip edilir

**Nasıl Test Edilir**:
```bash
# Bu test için database'i simüle etmek zor
# Alternatif: Manual state manipulation

# 1. Normal snapshot tamamla
./go-pq-cdc --config config.yml

# 2. Stop et

# 3. Manuel olarak job'u incomplete yap
psql -c "UPDATE cdc_snapshot_job SET completed=false WHERE slot_name='your_slot';"

# 4. Verify: Tüm chunk'lar completed AMA job completed=false
```

**Verification (Broken State)**:
```sql
-- Inconsistent state
SELECT 
    j.completed as job_completed,
    COUNT(c.*) FILTER (WHERE c.status='completed') as completed_chunks,
    j.total_chunks
FROM cdc_snapshot_job j
LEFT JOIN cdc_snapshot_chunks c ON c.slot_name = j.slot_name
GROUP BY j.completed, j.total_chunks;
-- Expected: job_completed=false, completed_chunks=total_chunks (inconsistent!)
```

**Auto-Fix Test**:
```bash
# 5. Restart
./go-pq-cdc --config config.yml
```

**Logs'da Ara**:
```
✅ "all chunks completed but job not marked, fixing state" 
    (setupJob içinde auto-fix)
✅ "snapshot already completed" (sonra skip eder)
```

**Verification (Auto-Fix Sonrası)**:
```sql
-- Job completed=true olmalı
SELECT completed FROM cdc_snapshot_job;
-- Expected: true
```

---

### 🔄 Test 2.4: Database Connection Retry

**TODO**: [ ] Test edildi

**Beklenti**:
- Network hiccup, DB connection timeout
- `retryDBOperation()` 3 kez dener (1s, 2s, 4s backoff)
- Transient error ise retry başarılı
- Non-transient error ise direkt fail

**Nasıl Test Edilir**:
```bash
# Simüle etmek için DB'yi geçici olarak durdur

# 1. CDC başlat
./go-pq-cdc --config config.yml &
PID=$!

# 2. Chunk processing sırasında DB'yi durdur
sudo systemctl stop postgresql
# veya
docker stop postgres-container

# 3. 2 saniye bekle
sleep 2

# 4. DB'yi tekrar başlat
sudo systemctl start postgresql

# 5. Retry çalıştı mı kontrol et
```

**Logs'da Ara**:
```
⚠️ "claim chunk" error: "connection refused"
✅ "retrying database operation" attempt=1 delay=1s
✅ "retrying database operation" attempt=2 delay=2s
✅ "claimed chunk" (retry başarılı)
```

**Verification**:
```sql
-- Chunk işlenmeye devam etmeli
SELECT status FROM cdc_snapshot_chunks WHERE status='in_progress';
```

---

## 3. Data Consistency Tests

### 📊 Test 3.1: LSN Consistency (Snapshot vs CDC)

**TODO**: [ ] Test edildi

**Beklenti**:
- Snapshot başlarken LSN=X kaydedilir
- Snapshot sırasında yapılan DB write'lar snapshot'a GİRMEZ (transaction isolation)
- CDC, LSN=X'den başlar
- Snapshot sonrası write'lar CDC'de yakalanır

**Nasıl Test Edilir**:
```bash
# 1. Tablo oluştur
psql -c "CREATE TABLE lsn_test (id SERIAL PRIMARY KEY, value TEXT, source TEXT);"
psql -c "INSERT INTO lsn_test SELECT i, 'before_snapshot', 'initial' FROM generate_series(1, 100) i;"

# 2. CDC başlat
./go-pq-cdc --config config.yml &

# 3. Snapshot başladığı anda (BEGIN marker geldiğinde) INSERT yap
# Terminal 1: CDC çalışıyor, logs izle
# Terminal 2:
sleep 1  # Snapshot başladı
psql -c "INSERT INTO lsn_test VALUES (101, 'during_snapshot', 'inserted_during');"
psql -c "UPDATE lsn_test SET value='updated_during' WHERE id=50;"

# 4. Snapshot bitince (END marker sonrası) INSERT yap
# Logs'da "snapshot completed" görünce:
psql -c "INSERT INTO lsn_test VALUES (102, 'after_snapshot', 'inserted_after');"
```

**Consumer'da Beklenen**:
```
Snapshot Events:
- BEGIN
- 100 DATA events (id=1..100, value='before_snapshot') ✅
- END

CDC Events:
- INSERT id=101 'during_snapshot' ✅ (snapshot SIRASINDA yapılan)
- UPDATE id=50 'updated_during' ✅
- INSERT id=102 'after_snapshot' ✅
```

**❌ HATA: Eğer snapshot'ta id=101 gelirse**:
```
Transaction isolation çalışmıyor!
pg_export_snapshot() düzgün kullanılmamış
```

**Verification**:
```sql
-- Snapshot LSN
SELECT snapshot_lsn FROM cdc_snapshot_job;

-- CDC'nin başladığı LSN (logs'dan al)
-- "CDC will continue from snapshot LSN: 0/XXXXX"
-- İkisi aynı olmalı
```

---

### 📊 Test 3.2: Empty Table Handling

**TODO**: [ ] Test edildi

**Beklenti**:
- Empty table için 1 chunk oluşturulur
- Chunk claim edilir, 0 row process edilir, completed olur
- Job başarıyla tamamlanır

**Nasıl Test Edilir**:
```bash
# 1. Boş tablo oluştur
psql -c "CREATE TABLE empty_table (id SERIAL PRIMARY KEY, data TEXT);"

# 2. CDC başlat
./go-pq-cdc --config config.yml
```

**Verification**:
```sql
-- 1 chunk oluşturuldu mu?
SELECT COUNT(*) FROM cdc_snapshot_chunks WHERE table_name='empty_table';
-- Expected: 1

-- 0 row processed
SELECT rows_processed FROM cdc_snapshot_chunks WHERE table_name='empty_table';
-- Expected: 0

-- Job completed
SELECT completed FROM cdc_snapshot_job;
-- Expected: true
```

**Logs'da Ara**:
```
✅ "coordinator: chunks created" table=empty_table chunks=1
✅ "chunk completed" rowsProcessed=0
```

---

### 📊 Test 3.3: Table Without Primary Key (ctid Usage)

**TODO**: [ ] Test edildi

**Beklenti**:
- Primary key yok
- `getOrderByClause()` → "ctid" döner
- Snapshot ORDER BY ctid ile alınır
- Data consistency korunur

**Nasıl Test Edilir**:
```bash
# 1. Primary key olmayan tablo
psql -c "CREATE TABLE no_pk_table (name TEXT, value INT);"
psql -c "INSERT INTO no_pk_table SELECT 'user_' || i, i FROM generate_series(1, 100) i;"

# 2. CDC başlat
./go-pq-cdc --config config.yml
```

**Logs'da Ara**:
```
✅ "no primary key found, using ctid" table=no_pk_table
```

**Verification**:
```sql
-- 100 row snapshot'a geldi mi?
SELECT SUM(rows_processed) FROM cdc_snapshot_chunks WHERE table_name='no_pk_table';
-- Expected: 100

-- Data consistency (consumer'da kontrol et)
-- 100 adet DATA event gelmeli
```

---

### 📊 Test 3.4: Mixed Data Types (NULL, JSON, Array)

**TODO**: [ ] Test edildi

**Beklenti**:
- Complex data type'lar parse edilir
- NULL values handle edilir
- `decodeColumnData()` hatasız çalışır

**Nasıl Test Edilir**:
```bash
# 1. Complex tablo
psql -c "CREATE TABLE complex_types (
    id SERIAL PRIMARY KEY,
    name TEXT,
    age INT,
    metadata JSONB,
    tags TEXT[],
    is_active BOOLEAN,
    created_at TIMESTAMP,
    amount DECIMAL(10,2),
    binary_data BYTEA,
    nullable_field TEXT
);"

psql -c "INSERT INTO complex_types VALUES (
    1,
    'Test User',
    25,
    '{\"key\": \"value\", \"nested\": {\"data\": 123}}',
    ARRAY['tag1', 'tag2', 'tag3'],
    true,
    NOW(),
    99.99,
    '\xDEADBEEF',
    NULL
);"

# 2. CDC başlat
./go-pq-cdc --config config.yml
```

**Consumer'da Kontrol Et**:
```json
{
  "eventType": "DATA",
  "table": "complex_types",
  "data": {
    "id": 1,
    "name": "Test User",
    "age": 25,
    "metadata": {"key": "value", "nested": {"data": 123}},
    "tags": ["tag1", "tag2", "tag3"],
    "is_active": true,
    "created_at": "2024-01-15T10:30:00Z",
    "amount": 99.99,
    "binary_data": "3q2+7w==",
    "nullable_field": null
  }
}
```

**Verification**:
```
✅ Tüm field'lar doğru type'da
✅ NULL field null olarak geldi
✅ JSON/Array parse edildi
✅ Hata log'u YOK
```

---

## 4. Edge Cases

### ⚙️ Test 4.1: ChunkSize Edge Cases

**TODO**: [ ] Test edildi

**Beklenti**:
- chunkSize=1 → Her row ayrı chunk
- chunkSize > table row count → Tek chunk
- chunkSize tam bölünmüyor → Son chunk kısa olabilir

**Nasıl Test Edilir**:
```bash
# Test 4.1a: chunkSize=1
psql -c "CREATE TABLE chunk_size_test_1 (id SERIAL PRIMARY KEY);"
psql -c "INSERT INTO chunk_size_test_1 SELECT generate_series(1, 10);"

# Config: chunkSize=1
./go-pq-cdc --config config_chunk_1.yml
```

**Verification**:
```sql
-- 10 chunk oluştu mu?
SELECT COUNT(*) FROM cdc_snapshot_chunks WHERE table_name='chunk_size_test_1';
-- Expected: 10

-- Her chunk 1 row
SELECT rows_processed FROM cdc_snapshot_chunks WHERE table_name='chunk_size_test_1';
-- Expected: Hepsi 1
```

```bash
# Test 4.1b: chunkSize > row count
psql -c "CREATE TABLE chunk_size_test_2 (id SERIAL PRIMARY KEY);"
psql -c "INSERT INTO chunk_size_test_2 SELECT generate_series(1, 50);"

# Config: chunkSize=1000
./go-pq-cdc --config config_chunk_1000.yml
```

**Verification**:
```sql
-- 1 chunk
SELECT COUNT(*) FROM cdc_snapshot_chunks WHERE table_name='chunk_size_test_2';
-- Expected: 1

-- 50 row
SELECT rows_processed FROM cdc_snapshot_chunks WHERE table_name='chunk_size_test_2';
-- Expected: 50
```

---

### ⚙️ Test 4.2: Large Table Performance

**TODO**: [ ] Test edildi

**Beklenti**:
- 1M row table snapshot alınabilir
- Memory leak yok
- Progress tracking çalışıyor

**Nasıl Test Edilir**:
```bash
# 1. 1M row tablo (dikkat: zaman alır!)
psql -c "CREATE TABLE large_table (id SERIAL PRIMARY KEY, data TEXT);"
psql -c "INSERT INTO large_table SELECT i, md5(random()::text) FROM generate_series(1, 1000000) i;"

# Config: chunkSize=10000 (100 chunks)

# 2. CDC başlat, metrics izle
./go-pq-cdc --config config.yml
```

**Monitoring**:
```bash
# Prometheus metrics
curl http://localhost:8080/metrics | grep snapshot

# Expected:
# snapshot_total_chunks 100
# snapshot_completed_chunks 0..100 (artıyor)
# snapshot_rows_total 0..1000000 (artıyor)
```

**Verification**:
```sql
-- Progress tracking
SELECT 
    total_chunks,
    completed_chunks,
    ROUND(100.0 * completed_chunks / total_chunks, 2) as progress_pct
FROM cdc_snapshot_job;
```

**Performance Check**:
```
✅ Memory kullanımı stabil (RSS artmıyor)
✅ Chunk process süresi tutarlı (~1-2s per chunk)
✅ Snapshot completion time reasonable (~3-5 minutes for 1M rows)
```

---

## 5. Observability Tests

### 📊 Test 5.1: Metrics Accuracy

**TODO**: [ ] Test edildi

**Beklenti**:
- `snapshot_total_chunks` = job.total_chunks
- `snapshot_completed_chunks` = job.completed_chunks
- `snapshot_rows_total` = SUM(rows_processed)
- `snapshot_duration_seconds` > 0

**Nasıl Test Edilir**:
```bash
# 1. Snapshot al
./go-pq-cdc --config config.yml

# 2. Metrics endpoint'i query'le
curl http://localhost:8080/metrics > metrics.txt
```

**Verification**:
```bash
# Parse metrics
grep snapshot_total_chunks metrics.txt
grep snapshot_completed_chunks metrics.txt
grep snapshot_rows_total metrics.txt
grep snapshot_duration_seconds metrics.txt

# Compare with DB
psql -c "SELECT total_chunks, completed_chunks FROM cdc_snapshot_job;"
psql -c "SELECT SUM(rows_processed) FROM cdc_snapshot_chunks;"

# Values should match!
```

---

### 📊 Test 5.2: BEGIN/END Marker Count

**TODO**: [ ] Test edildi

**Beklenti**:
- 1 snapshot = 1 BEGIN + N DATA + 1 END
- Multiple table için de 1 BEGIN + N DATA (all tables) + 1 END
- Duplicate marker YOK

**Nasıl Test Edilir**:
```bash
# Consumer'da event count

# 1. Tek tablo, 100 row
psql -c "CREATE TABLE marker_test (id INT PRIMARY KEY);"
psql -c "INSERT INTO marker_test SELECT generate_series(1, 100);"

# 2. CDC başlat, consumer'da count
```

**Consumer'da Kontrol**:
```go
var beginCount, dataCount, endCount int

for event := range events {
    switch event.EventType {
    case "BEGIN":
        beginCount++
    case "DATA":
        dataCount++
    case "END":
        endCount++
    }
}

// Verification
assert.Equal(t, 1, beginCount)
assert.Equal(t, 100, dataCount)
assert.Equal(t, 1, endCount)
```

---

### 📊 Test 5.3: Log Verbosity Check

**TODO**: [ ] Test edildi

**Beklenti**:
- Debug logs: Chunk detail, query execution
- Info logs: Milestone events (job initialized, chunk completed, snapshot completed)
- Warn logs: Retry, heartbeat fail
- Error logs: Critical failures

**Nasıl Test Edilir**:
```bash
# Different log levels
LOG_LEVEL=debug ./go-pq-cdc --config config.yml > debug.log
LOG_LEVEL=info ./go-pq-cdc --config config.yml > info.log
```

**Verify Log Contents**:
```bash
# Debug logs should include
grep "executing chunk query" debug.log
grep "heartbeat sent" debug.log

# Info logs should include
grep "snapshot starting" info.log
grep "chunk completed" info.log
grep "snapshot completed" info.log

# Warn logs should include (if retry happened)
grep "retrying database operation" *.log

# Error logs (shouldn't exist in happy path)
grep "ERROR" *.log
```

---

## 📝 Test Execution Checklist

```markdown
### Phase 1: Happy Path ✅
- [ ] Test 1.1: Tek tablo, tek chunk
- [ ] Test 1.2: Tek tablo, birden fazla chunk
- [ ] Test 1.3: Birden fazla tablo
- [ ] Test 1.4: CDC continuity, no duplicate
- [ ] Test 1.5: Completed snapshot skip

### Phase 2: Failure & Recovery 🔄
- [ ] Test 2.1: Crash & resume
- [ ] Test 2.2: Heartbeat timeout, reclaim
- [ ] Test 2.3: markJobAsCompleted fail, auto-fix
- [ ] Test 2.4: DB connection retry

### Phase 3: Data Consistency 📊
- [ ] Test 3.1: LSN consistency
- [ ] Test 3.2: Empty table
- [ ] Test 3.3: No primary key (ctid)
- [ ] Test 3.4: Mixed data types

### Phase 4: Edge Cases ⚙️
- [ ] Test 4.1: ChunkSize edge cases
- [ ] Test 4.2: Large table performance

### Phase 5: Observability 📊
- [ ] Test 5.1: Metrics accuracy
- [ ] Test 5.2: BEGIN/END marker count
- [ ] Test 5.3: Log verbosity
```

---

## 🛠️ Useful SQL Queries for Debugging

### Job Status
```sql
SELECT 
    slot_name,
    completed,
    total_chunks,
    completed_chunks,
    ROUND(100.0 * completed_chunks / NULLIF(total_chunks, 0), 2) as progress_pct,
    snapshot_lsn,
    started_at
FROM cdc_snapshot_job;
```

### Chunk Breakdown by Table
```sql
SELECT 
    table_name,
    status,
    COUNT(*) as count,
    SUM(rows_processed) as total_rows
FROM cdc_snapshot_chunks
GROUP BY table_name, status
ORDER BY table_name, status;
```

### Find Stale Chunks
```sql
SELECT 
    id,
    table_name,
    chunk_index,
    claimed_by,
    status,
    heartbeat_at,
    NOW() - heartbeat_at as stale_duration
FROM cdc_snapshot_chunks
WHERE status = 'in_progress' 
  AND heartbeat_at < NOW() - INTERVAL '5 minutes'
ORDER BY heartbeat_at;
```

### Chunk Processing Timeline
```sql
SELECT 
    id,
    table_name,
    chunk_index,
    status,
    claimed_at,
    completed_at,
    completed_at - claimed_at as processing_duration,
    rows_processed
FROM cdc_snapshot_chunks
ORDER BY claimed_at;
```

### Row Processing Stats
```sql
SELECT 
    table_schema,
    table_name,
    COUNT(*) as chunk_count,
    SUM(rows_processed) as total_rows,
    AVG(rows_processed) as avg_rows_per_chunk,
    MIN(rows_processed) as min_rows,
    MAX(rows_processed) as max_rows
FROM cdc_snapshot_chunks
WHERE status = 'completed'
GROUP BY table_schema, table_name;
```

---

## 🎯 Success Criteria

Tüm test'ler başarılı ise:

✅ **Functional**: Snapshot alınıyor, resume çalışıyor, CDC devam ediyor  
✅ **Reliability**: Retry çalışıyor, crash recovery yapıyor, auto-fix çalışıyor  
✅ **Data Integrity**: Duplicate yok, LSN consistency var, transaction isolation çalışıyor  
✅ **Performance**: 1M row handle ediliyor, memory leak yok  
✅ **Observability**: Metrics doğru, logs anlamlı  

**Production'a hazırsınız!** 🚀

---

## 📚 Additional Resources

- [PostgreSQL Replication Documentation](https://www.postgresql.org/docs/current/logical-replication.html)
- [pg_export_snapshot()](https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-SNAPSHOT-SYNCHRONIZATION)
- [Advisory Locks](https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS)
- [SELECT FOR UPDATE SKIP LOCKED](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE)

---

**Last Updated**: 2024-01-15  
**Version**: 1.0  
**Author**: go-pq-cdc Team

