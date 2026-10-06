# Partitioned table snapshot regression example

Bu bağımsız örnek, partitioned parent snapshot düzeltmesini metadata üzerinden
deterministik olarak doğrular ve snapshot sonrasında aynı connector ile CDC akışını
simüle eder. Herhangi bir gerçek servis şeması veya verisi içermez.

Fixture özellikleri:

- `public.partitioned_events`, `created_date` ile beş aylık leaf partition'a ayrılmıştır.
- Partition satır sayıları sırasıyla `100`, `10`, `0`, `500` ve `300` değerleridir.
- Primary key `(event_id, created_date)` olduğu için `auto` strateji `ctid_block` seçer.
- Parent relation fiziksel veri tutmadığı için `pg_relation_size(public.partitioned_events) = 0` olur.
- Snapshot her leaf partition için ayrı fiziksel chunk'lar oluşturur.
- Aynı snapshot job içinde 75 satırlı normal `public.regular_events` tablosu da işlenir.
- Üç leaf partition'lı `public.filtered_partitioned_events`, `status = 'active'`
  query condition ile çalışır; toplam 60 satırdan yalnızca 25'i snapshot'a girer.
- Snapshot tamamlandıktan sonra üç tabloda da `INSERT`, `UPDATE` ve `DELETE` çalıştırılır.
- Partitioned CDC event'lerinin child yerine root tablo adıyla geldiği doğrulanır.
- Filtreli tabloya yazılan `inactive` kaydın CDC'de geldiği doğrulanır; `QueryCondition`
  yalnızca snapshot sorgularına uygulanır.

## Podman ile çalıştırma

İlk kullanımda Compose provider yoksa bir kere kurun:

```bash
brew install podman-compose
```

Ardından:

```bash
cd example/partitioned-snapshot-cdc-regular
podman compose up -d
```

`main.go` dosyasını GoLand'dan çalıştırın veya repository root'tan:

```bash
go run ./example/partitioned-snapshot-cdc-regular
```

Uygulama varsayılan olarak `localhost:55432` üzerindeki PostgreSQL'e bağlanır.
Gerekirse `POSTGRES_HOST` ve `POSTGRES_PORT` environment variable'larıyla değiştirilebilir.

Ortamı kapatıp temizlemek için:

```bash
podman compose down -v
```

Başarılı doğrulama sonunda snapshot logunda aşağıdakine benzer bir kayıt görülür:

```text
partitioned, filtered partitioned, and regular tables completed together partitioned_rows=910 filtered_partitioned_rows=25 regular_rows=75
snapshot followed by CDC completed successfully cdc_events=9
```

Metadata'yı interaktif incelemek için servisleri ayrı çalıştırabilirsiniz:

```bash
docker compose -f example/partitioned-snapshot-cdc-regular/docker-compose.yml up -d postgres
go run ./example/partitioned-snapshot-cdc-regular
docker compose -f example/partitioned-snapshot-cdc-regular/docker-compose.yml exec postgres \
  psql -U postgres -d snapshot_example -x -c \
  "SELECT * FROM cdc_snapshot_job; SELECT * FROM cdc_snapshot_chunks;"
```

Beklenen kritik değerler:

```text
completed          = true
partitioned_leafs          = 5
partitioned_root_scans     = 0
partitioned_rows           = 910
regular_physical_scans     = 0
regular_rows               = 75
filtered_partitioned_leafs = 3
filtered_partitioned_rows  = 25
```

`partitioned_root_scans=0`, CTID sorgularının fiziksel verisi olmayan parent yerine doğrudan leaf
partition'larda çalıştığını gösterir. Böylece parent üzerinden bütün partition'ları tek
bir `ReadAll()` çağrısıyla belleğe alma problemi ortadan kalkar.
