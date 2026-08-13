# Real-Time Veri Pipeline: Binance REST → Kafka → Spark → Mongo + JSON

> Bu plan 12 Ağustos 2026'da yazıldı ve proje boyunca takip edilen tek yol haritasıdır.
> Adımlar tamamlandıkça bu dosyadaki durum tablosu güncellenir.

## Durum

| Adım | Konu | Durum |
|---|---|---|
| 0 | Docker Desktop + WSL2 kurulumu | ✅ Tamam |
| 1 | İskelet + altyapı ayakta (compose, .env, topic) | ✅ Tamam, doğrulandı |
| 2 | Producer: Binance REST → Kafka | ✅ Tamam, doğrulandı |
| 3 | Spark job iskeleti (uçtan uca akış) | ✅ Tamam, doğrulandı |
| 4 | Transform: şema + pencereli agregasyon | ✅ Tamam, doğrulandı |
| 5 | Sink: MongoDB + geçici JSON dosyası | ⏭️ Sıradaki |
| 6 | Tam containerize + Linux'a taşıma | ⏳ Bekliyor |

---

## Context

Amaç: bir public REST API'den sürekli veri çekip Kafka event'i olarak yayınlamak, Apache Spark
Structured Streaming ile gerçek zamanlı dönüştürmek, sonucu hem NoSQL veritabanına hem de
geçici bir JSON dosyasına akıtmak.

Bu yapı ileride bir **Linux makineye taşınacak**, o yüzden her bileşen `docker-compose.yml`
içinde tanımlanır; taşıma işi "dosyaları kopyala + `docker compose up`"a iner. Host'a özel hiçbir
kurulum (winutils.exe, native Kafka, native Mongo) pipeline'ın parçası olmaz.

Verilen kararlar:
- **Kaynak:** Binance public REST API (SSE/WebSocket kesinlikle yok, saf REST GET)
- **Stream motoru:** Apache Spark Structured Streaming (PySpark)
- **NoSQL:** MongoDB — compose içinde, host portu **27018** (makinedeki native Mongo 8.2'ye dokunulmaz)
- **Altyapı:** Docker Compose (taşınabilirlik için)
- **Git:** repo'yu ve commit'leri her zaman kullanıcı yönetir
- **Dil:** sohbet Türkçe, dosya içi yorumlar İngilizce

Çalışma kuralı: **adım adım gidilecek**, her adım sonunda durulup kullanıcının doğrulaması ve
commit'i beklenecek. Gereksiz kod/dosya üretilmeyecek.

---

## Hedef Mimari

```
Binance REST  GET /api/v3/aggTrades?symbol=X&fromId=N
      │        (producer.py — 2 sn poll, fromId watermark ile mükerrersiz artan çekim)
      ▼
Kafka topic: binance.trades.raw     key=symbol, value=JSON, 3 partition
      │
      ▼
Spark Structured Streaming (readStream kafka)
      │  from_json → 5 sn tumbling window + watermark → symbol bazlı OHLC/VWAP
      │
      ├──► MongoDB   (foreachBatch + pymongo, _id=symbol|window_start ile idempotent upsert)
      └──► output/live_ohlc.jsonl   (append, geçici JSON çıktısı)
```

**Neden `/api/v3/aggTrades`, `/api/v3/trades` değil:** ikisi de aynı REST ailesi ve `fromId`
watermark mantığı birebir aynı, ancak `trades` endpoint'inin rate-limit ağırlığı yüksek
(3 sembol × sık poll → 1200 weight/dk sınırını aşıyor). `aggTrades` çok daha düşük ağırlıkta,
yani daha sık poll edip daha canlı bir akış elde ediyoruz. Yanıttaki `X-MBX-USED-WEIGHT-1M`
header'ı loglanacak, 429/418 durumunda `Retry-After` ile exponential backoff uygulanacak.

---

## Adım 0 — Docker Desktop kurulumu ✅

Makinede Docker ve WSL2 yoktu. `winget install Docker.DockerDesktop` ile kuruldu.

Sonuç: Docker Desktop 4.86.0, Docker CLI 29.7.2, Compose 5.3.1, WSL 2.7.11.0
(çekirdek 6.18.33.2-2). Windows 11 Home olduğu için Hyper-V yok → WSL2 backend zorunluydu;
kurulum `VirtualMachinePlatform` + `Microsoft-Windows-Subsystem-Linux` özelliklerini açtı,
bir kez yeniden başlatma ve ardından `wsl --update` gerekti.

Yeniden kurulum gerekmeyen mevcut ortam: Python 3.13.2 + pip, Node, Git,
MongoDB 8.2 (native, 27017 — bu projede **kullanılmıyor**, sadece port çakışmasını
önlemek için 27018 seçildi).

---

## Adım 1 — İskelet + altyapı ayakta ✅

Oluşturulan dosyalar:

- `docker-compose.yml` — dört servis:
  - **kafka**: `apache/kafka:3.9.1`, KRaft modu (Zookeeper yok), tek node.
    İki listener tanımlı — bu kritik: container'lar `kafka:9092`'yi, host'taki
    geliştirme scriptleri `localhost:29092`'yi kullanır.
  - **kafka-init**: topic'i `--if-not-exists` ile idempotent oluşturan tek seferlik servis
  - **mongo**: `mongo:8`, `ports: "27018:27017"`, named volume ile kalıcı veri
  - **kafka-ui**: `kafbat/kafka-ui` — topic/mesajları tarayıcıdan görmek için (`localhost:8080`)
- `.env` — `SYMBOLS`, `POLL_INTERVAL_SEC`, `KAFKA_TOPIC`, portlar
- `.env.example` — `.env` gitignore'da olduğu için tracked şablon
- `.gitignore` — `.env`, `output/`, `checkpoint/`, `.ivy/`, `__pycache__/`, `.venv/`

**Doğrulama sonuçları:** üç servis de `healthy`; topic `binance.trades.raw` 3 partition ile
oluştu; `kafka-init` ikinci çalıştırmada da exit 0 (idempotency); Mongo `ping` → `{"ok":1}`;
`localhost:8080` HTTP 200; host'tan `localhost:29092` TCP bağlantısı başarılı.

### Plandan sapmalar (gerekçeleriyle)

1. **`kafka-init` servisi eklendi.** Plan topic'i elle `docker compose exec` ile oluşturmayı
   söylüyordu. Kafka'ya kalıcı volume verilmediği için `down -v` sonrası topic kaybolur ve her
   seferinde elle komut gerekirdi. Bu servis her `up`'ta idempotent çalışır — Linux'a taşırken
   hiçbir manuel adım kalmıyor.
2. **`provectuslabs/kafka-ui` yerine `kafbat/kafka-ui`.** Provectus deposu arşivlendi,
   proje kafbat altında devam ediyor.
3. **`.env` gitignore'a alındı, `.env.example` eklendi.** Plan `.env`'i repo'da tutuyordu.
   İlk commit'te `.env` var ve orada bırakıldı — içinde sır yok, sadece topic adı, portlar ve
   sembol listesi. Geçmiş yeniden yazılmadı.

---

## Adım 2 — Producer: Binance REST → Kafka ✅

`producer/producer.py` (tek dosya) + `producer/requirements.txt` (`requests`, `confluent-kafka`).

`kafka-python` yerine **confluent-kafka**: kafka-python'ın Python 3.13 ile bilinen uyum
sorunları var, confluent-kafka'nın hazır wheel'i hem Windows hem Linux'ta sorunsuz.

Producer mantığı:
1. Her sembol için `last_id` sözlükte tutulur (bellekte; ilk çağrıda `fromId` gönderilmez,
   son N agg-trade alınır ve watermark oradan başlar)
2. Döngü: her sembol için `GET /api/v3/aggTrades?symbol=X&fromId=last_id+1&limit=1000`
3. Dönen kayıtlar normalize edilir → `{symbol, trade_id, price, qty, quote_qty, trade_time,
   is_buyer_maker, ingest_time}`
4. Kafka'ya `key=symbol` ile üretilir → aynı sembol hep aynı partition'a düşer, sıra korunur
5. `last_id` güncellenir, `POLL_INTERVAL_SEC` kadar beklenir
6. `X-MBX-USED-WEIGHT-1M` loglanır; 429/418'de `Retry-After` ile backoff; ağ hatalarında retry

Bu adımda producer **host'tan** çalıştırılır (`localhost:29092`) — iterasyon hızlı olsun diye.
Container'a alma işi Adım 6'da.

**Doğrulama sonuçları** (iki ayrı çalıştırma, 30 sn + 15 sn):

| Kontrol | Sonuç |
|---|---|
| Kafka'ya ulaşan mesaj | 233 — producer'ın saydığıyla birebir (164 + 69), kayıp yok |
| Mükerrer `trade_id` | **0** (yeniden başlatma sonrası dahil) |
| `trade_id` sırası | Her sembolde artan sırada korunmuş |
| Partition yerleşimi | Her sembol tek partition'da, key = sembol |
| REST weight | ~360/dk (limit 1200) |

`confluent-kafka` 2.15.0 cp313 wheel'i sorunsuz kuruldu — kütüphane tercihi doğrulandı.

Partition dağılımı dengesiz (BTCUSDT + ETHUSDT → p0, SOLUSDT → p2, p1 boş): üç key'in
murmur2 hash'i çakıştı. Sorun değil — ihtiyacımız olan garanti "aynı sembol hep aynı
partition", o sağlanıyor; sembol sayısı arttıkça dağılım dengelenir.

Plana ek olarak `KAFKA_BOOTSTRAP` değişkeni `.env` / `.env.example` içine eklendi
(host'ta `localhost:29092`, Adım 6'da `kafka:9092`).

→ **DUR, commit.**

---

## Adım 3 — Spark job iskeleti (uçtan uca akış) ✅

`spark/stream_job.py` — bu adımda sadece: Kafka'dan `readStream`, ham `value`'yu string'e
çevir, `console` sink'ine yaz. Amaç transform yazmadan önce Spark↔Kafka bağlantısını
doğrulamak.

Spark **container'da** çalışır (`apache/spark:4.0.0`), host'ta değil: Windows'ta PySpark
checkpoint'i için `winutils.exe`/`hadoop.dll` gerekiyor ve bu Linux'a taşınabilirlik hedefine
de ters. Compose'a `spark` servisi eklenir:

- `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 /app/stream_job.py`
- `./.ivy:/opt/spark/.ivy2` bind mount → connector jar'ları her başlatmada yeniden indirilmesin
- `./spark:/app` bind mount → kod değişince sadece `docker compose restart spark`
- `./checkpoint:/app/checkpoint`, `./output:/app/output`
- `bootstrap.servers = kafka:9092` (container-içi listener)

**Doğrulama sonuçları:**

| Kontrol | Sonuç |
|---|---|
| Spark ↔ Kafka bağlantısı | Kuruldu — Batch 0 topic'teki 233 mesajı okudu |
| Canlı akış | Producer çalışırken 7 mikro-batch, hepsinde satır > 0 |
| Spark 4.0 ↔ connector uyumu | `_2.13:4.0.0` sorunsuz — Spark 3.5'e düşmeye gerek kalmadı |
| Ivy cache | 22 jar / 122 MB indi; `restart` sonrası **0** yeniden indirme |

Risk tablosundaki "Spark 4.0 ↔ kafka connector sürüm uyumsuzluğu" maddesi kapandı.

İki beklenen uyarı (Adım 5'te kapanacak):
- *"Temporary checkpoint location created"* — console sink'in `checkpointLocation`'ı yok,
  Spark geçici bir dizin açıyor.
- Checkpoint olmadığı için `restart` sonrası sorgu yine Batch 0'dan, yani topic'in başından
  okuyor. Adım 5'te kalıcı checkpoint gelince kaldığı offset'ten devam edecek.

→ **DUR, commit.**

---

## Adım 4 — Transform: şema + pencereli agregasyon ✅

`stream_job.py` genişletilir (yeni dosya yok):

1. Açık `StructType` şeması ile `from_json` — schema inference streaming'de zaten çalışmaz,
   ayrıca bozuk kayıtta patlamamak için `mode` kontrolü
2. `trade_time` → timestamp cast
3. `withWatermark("trade_time", "30 seconds")` — geç gelen event toleransı
4. `groupBy(window("trade_time", "5 seconds"), "symbol")` üzerinden:
   - `trade_count`, `volume = sum(qty)`, `quote_volume = sum(price*qty)`
   - `vwap = quote_volume / volume`
   - `high = max(price)`, `low = min(price)`
   - `first_price`, `last_price`
5. `outputMode("append")` — watermark + window kombinasyonu append'i mümkün kılar; pencere
   kapandığında kayıt kesinleşir
6. `trigger(processingTime="5 seconds")`

Bu adımda sink hâlâ `console` — agregasyon çıktısı gözle doğrulanır.

**Doğrulama sonuçları** (121 pencere, 3 sembol):

| Kontrol | Sonuç |
|---|---|
| Pencere süresi | Hepsi tam **5 saniye** |
| VWAP ∈ [low, high] | **0** aykırı satır |
| first/last fiyat ∈ [low, high] | **0** aykırı satır |
| Aynı pencerenin iki kez yayınlanması | **0** (append modu doğru çalışıyor) |
| `trade_count` ↔ Kafka sayımı | Aynı zaman aralığında **1232 = 1232**, fark 0 |

Plandan iki sapma:

1. **`first_price`/`last_price` için `min_by`/`max_by`, `first()`/`last()` değil.** Streaming'de
   `first()`/`last()` grup içi sıra garantisi vermez. Sıralama anahtarı olarak `trade_time`
   yerine `trade_id` seçildi: aynı milisaniyede birden fazla işlem olabiliyor (veride görüldü),
   `trade_id` ise sembol başına kesin artan.
2. **`local[2]` → `local[4]`.** İki thread'le mikro-batch sürekli 5 sn'lik tetiklemeyi aşıyordu
   (5.4–5.6 sn, "batch is falling behind" uyarısı). Dörde çıkınca 36 batch'te 1 uyarı kaldı;
   Adım 5'te eklenecek sink'ler için de pay bırakıyor.

Log okurken dikkat: console sink batch başına en fazla `numRows` satır **basar**, fazlasını
sessizce atar (`only showing top 20 rows`). Yayınlanan pencere sayısı ile basılan satır sayısı
aynı şey değil — ilk batch'te dünden kalan yüzlerce pencere kapandığı için bu fark görülür.

→ **DUR, commit.**

---

## Adım 5 — Sink: MongoDB + geçici JSON dosyası

Tek bir `foreachBatch` fonksiyonu ile **her iki hedefe** yazılır. `mongo-spark-connector`
jar'ı yerine `foreachBatch` + `pymongo` tercih ediliyor: bir bağımlılık ve bir sürü
sürüm-uyumluluk riski daha az, kod da tek yerde toplanıyor.

```python
def write_batch(batch_df, batch_id):
    rows = [r.asDict(recursive=True) for r in batch_df.collect()]
    if not rows:
        return
    # 1) Mongo — idempotent upsert
    coll.bulk_write([
        UpdateOne({"_id": f"{r['symbol']}|{r['window_start'].isoformat()}"},
                  {"$set": r}, upsert=True)
        for r in rows
    ], ordered=False)
    # 2) Geçici JSON — append (JSONL)
    with open(JSON_PATH, "a", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, default=str) + "\n")
```

`_id = symbol|window_start` seçimi bilinçli: Spark bir batch'i failure sonrası yeniden
oynatırsa Mongo'da mükerrer kayıt oluşmaz (`foreachBatch` at-least-once garantisi verir,
idempotent upsert bunu effectively-once'a çevirir).

- Mongo hedefi: `realtime` DB, `trade_ohlc` collection, bağlantı `mongodb://mongo:27017`
  (container-içi) / host'tan bakmak için `mongodb://localhost:27018`
- JSON hedefi: `output/live_ohlc.jsonl` — `.gitignore`'da, geçici çıktı
- `writeStream.format("json")` yerine bilinçli olarak `foreachBatch`: yerleşik JSON sink
  `part-00000-*.json` gibi onlarca parça dosya üretir; istenen ise tek, okunabilir, canlı
  büyüyen bir dosya

**Doğrulama:**
- `docker compose exec mongo mongosh realtime --eval "db.trade_ohlc.countDocuments()"` artıyor
- `Get-Content output\live_ohlc.jsonl -Tail 5 -Wait` ile satırların canlı düştüğü görülüyor
- Spark container'ı `restart` edilip aynı pencereler yeniden işlendiğinde Mongo'daki
  doküman sayısı **artmıyor** (idempotency kanıtı)

→ **DUR, commit.**

---

## Adım 6 — Tam containerize + Linux'a taşıma

- `producer/Dockerfile` (`python:3.12-slim` + requirements) eklenir, producer compose'a
  servis olarak girer; `KAFKA_BOOTSTRAP` artık `kafka:9092`
- Servis bağımlılıkları `depends_on` + healthcheck ile sıraya sokulur
- Tek komutla ayağa kalkış doğrulanır: `docker compose up -d`
- Kısa bir `README.md`: mimari şeması, çalıştırma, durdurma, sıfırlama komutları
- Linux notu: bind-mount edilen `output/` ve `checkpoint/` dizinlerinde root-owned dosya
  oluşmaması için compose'daki ilgili servislere `user:` eklenir

**Doğrulama (uçtan uca, temiz kurulum simülasyonu):**
```bash
docker compose down -v && rm -rf output checkpoint
docker compose up -d
# ~1 dk sonra: Mongo'da doküman var, output/live_ohlc.jsonl büyüyor
```

→ **DUR, commit.**

---

## Nihai Dosya Yapısı

```
realtime-pipeline/
├─ docker-compose.yml
├─ .env                (gitignore — yerel yapılandırma)
├─ .env.example
├─ .gitignore
├─ PLAN.md
├─ README.md
├─ producer/
│  ├─ Dockerfile
│  ├─ requirements.txt
│  └─ producer.py
├─ spark/
│  └─ stream_job.py
├─ output/             (gitignore — geçici JSON çıktısı)
└─ checkpoint/         (gitignore — Spark offset state)
```

Producer ve Spark job'ı birer dosya olarak kalır; adım adım genişletilir, yeni dosya açılmaz.

---

## Riskler ve Önlemler

| Risk | Önlem |
|---|---|
| Binance rate limit (429/418, IP ban) | `aggTrades` (düşük weight) + weight header takibi + `Retry-After` backoff |
| Spark 4.0 ↔ kafka connector sürüm uyumsuzluğu | Adım 3'te izole doğrulanır; gerekirse Spark 3.5 + `_2.12` jar'a düşülür |
| Kafka listener yanlış yapılandırması (host'tan bağlanamama) | İki listener baştan tanımlı: `kafka:9092` (iç) / `localhost:29092` (host) |
| 27017 port çakışması (native Mongo 8.2) | Compose Mongo host portu 27018 |
| İlk `spark-submit` jar indirmesi yavaş/internetsiz | `.ivy` cache bind-mount edilir, bir kez iner |
| Batch replay'de mükerrer Mongo kaydı | `_id = symbol\|window_start` ile idempotent upsert |
