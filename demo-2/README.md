# Demo 2: Simulasi Multi-Topic, Bulk Produce & Consumer Lag

Dokumentasi ini menjelaskan skenario pengujian performa, distribusi partisi, dan simulasi **Consumer Lag** di Kafka menggunakan script shell bawaan Kafka CLI (`kafka-topics.sh`, `kafka-console-producer.sh`, `kafka-console-consumer.sh`).

---

## 🎯 Tujuan Demo

1. **Multi-Partition Topics**: Membuat topic dengan jumlah partisi yang bervariasi (3, 4, 6, hingga 8 partisi).
2. **Bulk Keyed Message Production**: Mengirim ribuan pesan terformat JSON dengan message key unik secara efisien menggunakan `awk` stream.
3. **Simulasi Consumer Group**:
   - **Zero Lag**: Consumer membaca seluruh pesan sampai habis.
   - **Consumer Lag**: Consumer berhenti di tengah jalan sehingga menyisakan pesan yang belum dibaca (*lag*).
   - **Multi-Consumer Rebalance**: 2 consumer berjalan serentak dalam 1 consumer group untuk berbagi partisi secara otomatis.
4. **Visualisasi di Kafka UI**: Memantau pembagian partisi dan metrik consumer lag secara visual di web dashboard.

---

## 📦 Struktur File

```text
demo-2/
├── 1-produce.sh  # Script membuat topic & mengirim 22.500 pesan secara bulk
├── 2-consume.sh  # Script menjalankan berbagai skenario consumer group
└── README.md     # Dokumentasi petunjuk penggunaan
```

---

## 📊 Detail Skenario Simulasi

### 1. Topic & Pesan yang Dihasilkan (`1-produce.sh`)

| Topic | Jumlah Partisi | Jumlah Pesan | Format Key | Keterangan |
| :--- | :---: | :---: | :--- | :--- |
| `orders` | 6 | 5.000 | `orders-key-[0-49]` | Data transaksi order |
| `payments` | 4 | 3.000 | `payments-key-[0-49]` | Data pembayaran |
| `users` | 3 | 2.000 | `users-key-[0-49]` | Data aktivitas user |
| `notifications` | 3 | 2.500 | `notifications-key-[0-49]` | Data antrean notifikasi |
| `logs` | 8 | 10.000 | `logs-key-[0-49]` | Data log sistem throughput tinggi |

> **Format Payload Pesan:**  
> Key dan Value dipisahkan dengan titik dua (`:`), contoh:  
> `orders-key-12:{"id":1,"topic":"orders","amount":450,"status":"ok"}`

---

### 2. Skenario Consumer Group (`2-consume.sh`)

Script ini menjalankan beberapa consumer secara paralel di background selama maksimal 120 detik (`T=120`):

| Consumer Group | Topic | Target Pembacaan | Hasil Consumer Lag |
| :--- | :--- | :--- | :--- |
| `order-service` | `orders` | Membaca semua pesan (5.000) | **Lag = 0** (Semua pesan terproses) |
| `payment-service` | `payments` | Dibatasi hanya 1.000 pesan (`--max-messages 1000`) | **Lag ~ 2.000 pesan** (Sengaja tertinggal) |
| `user-service` | `users` | Dijalankan oleh **2 instance consumer** bersamaan | **Partisi terbagi** (1 instance dapat 2 partisi, 1 lagi dapat 1 partisi) |
| `notification-service` | `notifications` | Membaca semua pesan (2.500) | **Lag = 0** |
| `log-analyzer` | `logs` | Dibatasi hanya 500 pesan (`--max-messages 500`) | **Lag ~ 9.500 pesan** (Simulasi lag ekstrem) |

---

## 🚀 Panduan Menjalankan

Buka terminal **PowerShell** dan pastikan direktori aktif berada di folder `demo-2`.

### Langkah 1: Pastikan Container Kafka Aktif
Jika cluster belum menyala, jalankan dari root project:
```powershell
podman compose up -d
```

---

### Langkah 2: Jalankan Producer
Salin script [`1-produce.sh`](1-produce.sh) ke dalam container Kafka lalu jalankan:

```powershell
podman cp .\1-produce.sh kafka:/tmp/1-produce.sh
podman exec kafka bash -c "tr -d '\r' < /tmp/1-produce.sh | bash"
```

Tunggu hingga seluruh pesan selesai dikirim dan muncul pesan:
```text
Selesai. Cek topic di http://localhost:8080
```

> [!TIP]
> Buka browser dan akses **[http://localhost:8080](http://localhost:8080)** lalu masuk ke menu **Topics**. Anda akan melihat grafik pembagian pesan di setiap partisi.

---

### Langkah 3: Jalankan Consumer
Salin script [`2-consume.sh`](2-consume.sh) ke dalam container Kafka lalu jalankan:

```powershell
podman cp .\2-consume.sh kafka:/tmp/2-consume.sh
podman exec kafka bash -c "tr -d '\r' < /tmp/2-consume.sh | bash"
```

Proses akan berjalan di background dan selesai setelah seluruh batasan pesan tercapai atau durasi 120 detik berakhir.

---

## 🖥️ Apa yang Perlu Dicek di Kafka UI?

Setelah Langkah 3 selesai, buka **[http://localhost:8080](http://localhost:8080)**:

1. **Menu Consumers**:
   - Lihat daftar consumer group: `order-service`, `payment-service`, `user-service`, `notification-service`, dan `log-analyzer`.
   - Perhatikan kolom **Consumer Lag**:
     - `order-service` dan `notification-service` akan memiliki lag `0`.
     - `payment-service` akan memiliki lag sekitar `2000`.
     - `log-analyzer` akan memiliki lag sekitar `9500`.
2. **Detail `user-service`**:
   - Klik grup `user-service`, Anda akan melihat 2 consumer aktif membagi 3 partisi dari topic `users`.
3. **Menu Topics**:
   - Klik salah satu topic (misalnya `orders`), pilih tab **Messages** untuk melihat distribusi key dan isi JSON yang dihasilkan.

---

## 💡 Catatan Teknis

### Mengapa menggunakan `tr -d '\r'`?
Di lingkungan Windows, file teks sering kali disimpan dengan format line ending CRLF (`\r\n`). Jika file script bash Windows langsung dieksekusi di dalam container Linux, bash akan menampilkan error seperti `\r: command not found`.  
Perintah `tr -d '\r' < /tmp/script.sh | bash` bertindak membersihkan karakter carriage return (`\r`) secara on-the-fly sebelum dieksekusi oleh bash.
