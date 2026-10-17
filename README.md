# Penjelasan Konfigurasi `podman-compose.yml`

File [`podman-compose.yml`](podman-compose.yml) digunakan untuk menjalankan cluster **Apache Kafka** (mode KRaft tanpa ZooKeeper) dan **Kafka UI** (dashboard web monitoring) menggunakan **Podman**.

---

## 🏗️ Arsitektur & Gambaran Umum

```text
[ Host / Laptop Anda ]
       │
       ├─► Port 8080 ────────► [ kafka-ui ] (Web Dashboard)
       │                              │
       │                              │ Komunikasi Internal (kafka-net)
       │                              ▼ Port 29092
       └─► Port 9092 ────────► [ kafka broker (KRaft) ]
```

- **Kafka KRaft Mode**: Menggunakan arsitektur modern Kafka 3.x+ di mana Kafka mengelola metadatanya sendiri via Raft consensus tanpa memerlukan Apache ZooKeeper lagi.
- **Kafka UI**: Dashboard web visual berbasis open-source untuk melihat Topic, Partisi, Consumers, dan isi pesan secara real-time.
- **Bridge Network (`kafka-net`)**: Jaringan internal agar antar container dapat saling berkomunikasi via DNS nama service (`kafka`).

---

## 🔍 Penjelasan Baris demi Baris

### 1. Service `kafka`

```yaml
  kafka:
    image: docker.io/apache/kafka:latest
    container_name: kafka
    ports:
      - "9092:9092"
```

| Parameter | Penjelasan |
| :--- | :--- |
| `image: docker.io/apache/kafka:latest` | Menggunakan image resmi dari Apache Kafka. Prefix `docker.io/` sengaja disertakan agar Podman tidak menampilkan prompt interaktif untuk memilih registry image saat proses pull. |
| `container_name: kafka` | Memberi nama tetap pada container sebagai `kafka`. |
| `ports: - "9092:9092"` | Memetakan port `9092` dari container ke port `9092` di host (komputer Anda), sehingga aplikasi di luar container (misal: Go, Java, Python, Node.js) dapat terhubung ke `localhost:9092`. |

---

#### Konfigurasi Environment `kafka`:

```yaml
    environment:
      # KRaft mode (tanpa ZooKeeper)
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
```

| Environment Variable | Penjelasan |
| :--- | :--- |
| `KAFKA_NODE_ID: 1` | ID unik untuk node Kafka ini di dalam cluster. |
| `KAFKA_PROCESS_ROLES: broker,controller` | Menentukan peran container ini dalam mode KRaft. Container ini bertindak sebagai **broker** (melayani data/pesan) sekaligus **controller** (mengatur metadata cluster pengganti ZooKeeper). |

---

```yaml
      KAFKA_LISTENERS: PLAINTEXT://:29092,CONTROLLER://:9093,EXTERNAL://:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,EXTERNAL://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
```

> [!NOTE]
> **Mengapa ada `LISTENERS` dan `ADVERTISED_LISTENERS`?**  
> Kafka memerlukan alamat khusus yang diumumkan (*advertised*) kepada client agar client tahu harus tersambung ke host mana, baik dari dalam jaringan container maupun dari komputer lokal.

| Environment Variable | Penjelasan |
| :--- | :--- |
| `KAFKA_LISTENERS` | Port-port jaringan yang dibuka oleh Kafka: <br>• `EXTERNAL://:9092`: Untuk koneksi dari host/laptop luar container.<br>• `PLAINTEXT://:29092`: Untuk koneksi internal antar container (seperti Kafka UI).<br>• `CONTROLLER://:9093`: Untuk komunikasi internal controller KRaft quorum. |
| `KAFKA_ADVERTISED_LISTENERS` | Alamat yang diberikan Kafka ke client saat terhubung: <br>• `localhost:9092`: Diberikan kepada client yang mengakses dari laptop Anda.<br>• `kafka:29092`: Diberikan kepada client di dalam container network yang sama. |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` | Menentukan protokol keamanan tiap listener (di sini semuanya disetel `PLAINTEXT` tanpa enkripsi/SSL untuk kemudahan development lokal). |
| `KAFKA_CONTROLLER_LISTENER_NAMES` | Memberitahu Kafka bahwa listener bernama `CONTROLLER` dipakai untuk sinkronisasi metadata controller. |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | Menentukan voter consensus KRaft. `1@kafka:9093` berarti node ID `1` pada host `kafka` port `9093` adalah voter quorum-nya. |

---

```yaml
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
```

| Environment Variable | Penjelasan |
| :--- | :--- |
| `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0` | Mengatur delay rebalance consumer group menjadi `0` detik sehingga consumer langsung aktif tanpa menunggu node lain (sangat ideal untuk development). |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` | Faktor replikasi untuk topic internal `__consumer_offsets`. Disetel ke `1` karena kita hanya menjalankan 1 broker (standalone/single-node). |
| `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1` | Faktor replikasi untuk log transaksi (disetel `1` untuk single-node). |
| `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1` | Minimal in-sync replica untuk transaksi (disetel `1` untuk single-node). |

---

### 2. Service `kafka-ui`

```yaml
  kafka-ui:
    image: docker.io/provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    ports:
      - "8080:8080"
    environment:
      DYNAMIC_CONFIG_ENABLED: 'true'
      KAFKA_CLUSTERS_0_NAME: local-kafka
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
    depends_on:
      - kafka
    networks:
      - kafka-net
```

| Parameter / Variabel | Penjelasan |
| :--- | :--- |
| `image: docker.io/provectuslabs/kafka-ui:latest` | Image antarmuka web Kafka UI dari Provectus Labs. |
| `ports: - "8080:8080"` | Membuka dashboard web di browser Anda pada port `8080` ([http://localhost:8080](http://localhost:8080)). |
| `DYNAMIC_CONFIG_ENABLED: 'true'` | Mengizinkan perubahan konfigurasi cluster secara dinamis melalui UI. |
| `KAFKA_CLUSTERS_0_NAME: local-kafka` | Nama label cluster yang akan tampil di dashboard Kafka UI. |
| `KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092` | Alamat Kafka yang dihubungi oleh Kafka UI. Karena sesama container dalam network `kafka-net`, ia menggunakan nama host `kafka` dan port internal `29092`. |
| `depends_on: - kafka` | Memastikan container Kafka dijalankan terlebih dahulu sebelum Kafka UI menyala. |

---

### 3. Jaringan `networks`

```yaml
networks:
  kafka-net:
    driver: bridge
```

| Bagian | Penjelasan |
| :--- | :--- |
| `kafka-net` (driver: `bridge`) | Membuat user-defined network virtual. Ini penting di Podman (terutama mode rootless) agar container `kafka-ui` dapat mengenali host `kafka` melalui DNS internal. |

---

## 🚀 Perintah Dasar Podman

| Aksi | Perintah |
| :--- | :--- |
| **Menjalankan** | `podman compose -f podman-compose.yml up -d` |
| **Melihat Log** | `podman compose -f podman-compose.yml logs -f` |
| **Buka Web UI** | Akses `http://localhost:8080` di browser |
| **Menghentikan** | `podman compose -f podman-compose.yml down` |
