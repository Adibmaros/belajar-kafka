# Demo 1: Producer & Consumer Kafka Sederhana (Node.js)

Dokumentasi ini menjelaskan implementasi dasar Kafka **Producer** dan **Consumer** dalam satu aplikasi Node.js menggunakan library [`kafkajs`](https://kafka.js.org/).

---

## 🎯 Tujuan Demo

1. Memahami cara menginisialisasi Kafka client di Node.js (ES Module & Top-level `await`).
2. Mengirim pesan ke topic dengan konfigurasi idempotensi (`Producer`).
3. Menerima dan membaca pesan secara real-time menggunakan Consumer Group (`Consumer`).
4. Memahami mekanisme auto-commit offset pada KafkaJS.

---

## 🏗️ Alur Kerja (Workflow)

```text
┌─────────────────┐       kirim pesan (orderId, total)       ┌────────────────────────┐
│ Producer        │ ───────────────────────────────────────► │ Topic: 'orders'        │
│ (idempotent)    │         (acks: -1 / all)                 │ (Partition 0)          │
└─────────────────┘                                          └───────────┬────────────┘
                                                                         │
                                                                         │ stream / consume
                                                                         ▼
                                                             ┌────────────────────────┐
                                                             │ Consumer (groupId)     │
                                                             │ 'grup-a'               │
                                                             └────────────────────────┘
```

1. **Producer** terhubung ke broker, lalu mengirim satu pesan JSON transaksi pesanan ke topic `orders`.
2. **Consumer** yang berada dalam grup `grup-a` berlangganan (*subscribe*) ke topic `orders`.
3. Pesan diterima oleh `eachMessage` handler, dicetak ke console, lalu offset dicatat (*commit*) otomatis.

---

## 📦 Struktur File

```text
demo-1/
├── index.js          # Skrip utama berisi Producer dan Consumer
├── package.json      # Konfigurasi dependensi dan tipe modul ("type": "module")
├── package-lock.json # Lockfile dependensi npm
└── README.md         # Dokumentasi petunjuk penggunaan
```

---

## 🔍 Penjelasan Kode (`index.js`)

Berikut adalah bedah baris kode utama pada [`index.js`](index.js):

### 1. Inisialisasi Kafka Client

```javascript
import { Kafka } from 'kafkajs';

const kafka = new Kafka({ 
  clientId: 'belajar', 
  brokers: ['192.168.54.202:9092'] 
});
```

| Parameter | Penjelasan |
| :--- | :--- |
| `clientId` | Identitas logis client aplikasi untuk memudahkan tracing dan monitoring di sisi broker / Kafka UI. |
| `brokers` | Daftar alamat bootstrap broker Kafka. Di Windows dengan Podman WSL, alamat ini diarahkan ke IP virtual machine WSL (`podman-machine-default`). |

---

### 2. Kafka Producer

```javascript
// Producer
const producer = kafka.producer({ idempotent: true });
await producer.connect();
await producer.send({
    topic: 'orders',
    acks: -1, // all
    messages: [{ key: 'user1', value: JSON.stringify({ orderId: 'A', total: 50000 }) }],
});
```

| Opsi / Metode | Penjelasan |
| :--- | :--- |
| `idempotent: true` | Memastikan tidak terjadi duplikasi pesan (*Exactly-Once delivery per partition*) jika terjadi retry akibat kendala jaringan. |
| `producer.connect()` | Membuka koneksi socket ke Kafka cluster. |
| `topic: 'orders'` | Target topic pengiriman pesan. Jika topic belum ada, Kafka akan otomatis membuatnya (jika auto topic creation aktif). |
| `acks: -1` | Sama dengan `acks: all`. Broker hanya akan mengembalikan respon sukses setelah pesan tersimpan di replika aktif (*in-sync replicas*). |
| `key: 'user1'` | Kunci pesan. Kafka menggunakan hashing pada key untuk menentukan partisi tujuan. Semua pesan dengan key yang sama akan selalu masuk ke partisi yang sama (menjamin urutan/ordering). |
| `value` | Data muatan pesan. Format Kafka murni berupa buffer/string, sehingga object JavaScript perlu di-`JSON.stringify()`. |

---

### 3. Kafka Consumer

```javascript
// Consumer
const consumer = kafka.consumer({ groupId: 'grup-a' });
await consumer.connect();
await consumer.subscribe({ topic: 'orders', fromBeginning: true });
await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
        console.log({ 
            partition, 
            key: message.key?.toString(), 
            value: message.value?.toString() 
        });
        // offset di-commit otomatis setelah handler selesai tanpa error
    },
});
```

| Opsi / Metode | Penjelasan |
| :--- | :--- |
| `groupId: 'grup-a'` | Menentukan Consumer Group. Partisi topic akan dibagi secara merata kepada semua consumer dalam group ini. |
| `fromBeginning: true` | Jika consumer baru pertama kali bergabung (belum memiliki saved offset di broker), ia akan membaca pesan mulai dari offset paling awal. |
| `consumer.run()` | Memulai polling event loop untuk menerima batch pesan secara terus-menerus (*continuous listening*). |
| `eachMessage` | Callback fungsi yang dipanggil untuk setiap pesan yang masuk. |
| `message.key?.toString()` & `message.value?.toString()` | Karena Kafka menyimpan key dan value dalam format `Buffer` biner, kita perlu mengubahnya menjadi string teks. |
| **Auto Commit** | KafkaJS secara otomatis menandai (*commit*) offset pesan saat fungsi `eachMessage` selesai dieksekusi tanpa error. |

---

## 🚀 Cara Menjalankan

### 1. Prasyarat
Pastikan cluster Kafka sudah berjalan melalui Podman di root project:
```powershell
# Jalankan dari folder root 'belajar kafka'
podman compose up -d
```

### 2. Instalasi Dependensi
Masuk ke folder `demo-1` dan install library yang dibutuhkan:
```powershell
cd demo-1
npm install
```

### 3. Eksekusi Script
Jalankan skrip menggunakan Node.js:
```powershell
node index.js
```

### Output yang Diharapkan:
```text
{"level":"INFO","timestamp":"...","logger":"kafkajs","message":"[ConsumerGroup] Consumer has joined the group","groupId":"grup-a",...}
{ partition: 0, key: 'user1', value: '{"orderId":"A","total":50000}' }
```

> [!NOTE]
> Program tidak akan langsung keluar (*exit*) ke prompt terminal karena `consumer.run()` bekerja sebagai listener yang terus aktif memantau pesan baru.  
> Tekan **`Ctrl + C`** di terminal untuk menghentikan program.

---

## 💡 Tips & Troubleshooting

### 1. `ECONNREFUSED` saat menghubungkan ke Broker
- Jika menggunakan **Podman di Windows**, port forwarding Kafka mengikat ke IP WSL `podman-machine-default`.
- Cari tahu IP WSL dengan:
  ```powershell
  wsl -d podman-machine-default ip addr show eth0
  ```
- Pastikan IP tersebut cocok dengan nilai `brokers` di [`index.js`](index.js) dan `KAFKA_ADVERTISED_LISTENERS` di [`../podman-compose.yml`](../podman-compose.yml).
- Jika baru mengubah konfigurasi `podman-compose.yml`, pastikan container di-recreate dengan:
  ```powershell
  podman compose down
  podman compose up -d
  ```

### 2. Menghilangkan Partitioner Warning
Jika muncul pesan peringatan default partitioner KafkaJS v2.0.0, Anda dapat menambahkan variabel lingkungan di baris paling atas [`index.js`](index.js):
```javascript
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1';
```
