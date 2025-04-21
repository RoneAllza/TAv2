---

## 📡 Sensor MQTT to MySQL Logger

Proyek ini adalah aplikasi Python yang menerima data dari sensor melalui MQTT, menghitung rata-rata data setiap 1 menit dari 10 data terakhir, lalu menyimpannya ke MySQL (Aiven).

---

### 🔧 Fitur Utama

- Menerima data CH4 dan CO2 via MQTT
- Mengelola buffer data terakhir menggunakan `deque`
- Rata-rata data setiap 1 jam
- Menyimpan hasil ke MySQL dengan `entry_id` unik berbasis waktu
- Support multi sensor (`sensor_id`)
- Siap di-deploy dan di-scale

---

### 🛠️ Struktur File

```
.
├── main.py              # MQTT Subscriber + DB Logger
├── publisher.py         # Simulasi publisher sensor
├── .env.example         # Variabel environment (MQTT & DB) sebagai template, tinggal dicopas terus ganti nama jadi .env
├── requirements.txt     # Python dependencies
├── README.md            # Dokumentasi ini
```

---

### 📦 Instalasi

#### 1. Clone Repository

```bash
git clone https://github.com/username/nama-repo.git
cd nama-repo
```

#### 2. Install Dependency

```bash
pip install -r requirements.txt
```

#### 3. Buat file `.env`

Buat file `.env` di root project:

```env
# MQTT
MQTT_BROKER=broker.emqx.io
MQTT_PORT=1883
MQTT_USER=username
MQTT_PASSWORD=password
MQTT_TOPIC=sensor/data

# MySQL Aiven
DB_HOST=your-aiven-hostname
DB_PORT=your-port
DB_USER=your-user
DB_PASSWORD=your-password
DB_NAME=your-db-name
```

---

### 🚀 Menjalankan Aplikasi

#### 1. Jalankan Subscriber

```bash
python main.py
```

> Ini akan mendengarkan data dari MQTT broker dan menyimpan data ke database.

#### 2. Jalankan Publisher (untuk testing)

```bash
python publisher.py
```

> Script ini akan mengirimkan data sensor acak (CH4 & CO2) setiap detik ke MQTT broker.

---

### 🧪 Contoh Data JSON (dikirim via MQTT)

```json
{
  "sensor_id": "sensor_gas_01",
  "ch4": 120.5,
  "co2": 400.3
}
```

---

### 🗃️ Struktur Tabel MySQL

```sql
CREATE TABLE sensor_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    entry_id VARCHAR(100),
    sensor_id VARCHAR(255),
    ch4_value FLOAT,
    co2_value FLOAT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

### 📊 Output (Contoh Log)

```bash
Received: {'sensor_id': 'sensor_gas_01', 'ch4': 120.3, 'co2': 410.1}
[2025-04-21 14:33:00] Inserted entry 20250421143300: CH4=119.25, CO2=405.87 from 10 data points
Last 10 data from DB: [...]
```

---

### 🤝 Kontribusi

Pull request dan issue sangat diterima! Silakan fork dan kembangkan.

---

### 📄 Lisensi

MIT License © 2025 Rifqi Abdulaziz

---