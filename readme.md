---

## 🌬️ Sensor Data Fetcher & Emission Pipeline

Aplikasi Python ini mengambil data sensor dari ThingSpeak API, menyimpannya ke MySQL, lalu melakukan sinkronisasi dan perhitungan otomatis untuk pelaporan emisi (terintegrasi dengan Laravel DB).

---

### 🔧 Fitur Utama

- Fetch data sensor dari ThingSpeak API (otomatis setiap 1 jam)
- Sinkronisasi data sensor ke database Laravel
- Perhitungan otomatis fugitive emission (CH4 & CO2) harian
- Perhitungan dan insert otomatis aktivitas fuel combustion
- Generate laporan harian, bulanan, tahunan secara otomatis
- Logging ke file dan console (`emission_pipeline.log`)
- Siap di-deploy dan di-scale (multi-threaded)

---

### 🛠️ Struktur File

```
.
├── main.py              # Pipeline utama (fetch, sync, emission, report)
├── .env.example         # Template variabel environment (API & DB)
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
# ThingSpeak API
THINGSPEAK_URL=https://api.thingspeak.com/channels/CHANNEL_ID/feeds.json
THINGSPEAK_API_KEY=your-api-key

# MySQL Sensor DB
DB_SENSOR_HOST=your-sensor-db-host
DB_SENSOR_PORT=your-sensor-db-port
DB_SENSOR_USER=your-sensor-db-user
DB_SENSOR_PASSWORD=your-sensor-db-password
DB_SENSOR_NAME=your-sensor-db-name

# MySQL Laravel DB
DB_LARAVEL_HOST=your-laravel-db-host
DB_LARAVEL_PORT=your-laravel-db-port
DB_LARAVEL_USER=your-laravel-db-user
DB_LARAVEL_PASSWORD=your-laravel-db-password
DB_LARAVEL_NAME=your-laravel-db-name
```

---

### 🚀 Menjalankan Aplikasi

```bash
python main.py
```

> Semua proses berjalan otomatis: fetch sensor, sync ke Laravel, insert emission, fuel combustion, dan laporan.

---

### 🧪 Contoh Data JSON (dari ThingSpeak)

```json
{
  "entry_id": 123,
  "created_at": "2024-06-01T12:00:00Z",
  "field1": "2.5",
  "field2": "180",
  "field3": "29.1",
  "field4": "70",
  "field5": "12.3",
  "field6": "25.7",
  "field7": "400",
  "field8": "1.2"
}
```

---

### 🗃️ Struktur Tabel Sensor (MySQL)

```sql
CREATE TABLE sensor_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    entry_id INT,
    created_at DATETIME,
    wind_speed FLOAT,
    wind_direction FLOAT,
    temperature FLOAT,
    humidity FLOAT,
    pm25 FLOAT,
    pm10 FLOAT,
    co2 FLOAT,
    ch4 FLOAT
);
```

---

### 📊 Output (Contoh Log)

```bash
[2024-06-01 12:00:00] [INFO] [Sensor] Data entry_id 123 inserted.
[2024-06-01 12:01:00] [INFO] [Sync] Sensor entry_id 123 synced to Laravel DB.
[2024-06-01 12:02:00] [INFO] [FugitiveEmission] Inserted for 2024-06-01
[2024-06-01 12:03:00] [INFO] [FuelCombustion] Inserted for 2024-06-01 - sumber 1
[2024-06-01 12:04:00] [INFO] [Report] Harian inserted for 2024-06-01
```

---

### 🤝 Kontribusi

Pull request dan issue sangat diterima! Silakan fork dan kembangkan.

---

### 📄 Lisensi

MIT License © 2025 Rifqi Abdulaziz

---
