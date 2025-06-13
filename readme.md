---

## 🌬️ Sensor Data Fetcher: ThingSpeak to MySQL

Aplikasi Python ini mengambil data sensor dari ThingSpeak API, memfilter data yang terlalu banyak nilai null, lalu menyimpannya ke MySQL (Aiven).

---

### 🔧 Fitur Utama

- Mengambil data sensor dari ThingSpeak API (otomatis setiap 1 jam)
- Mendukung field: wind_speed, wind_direction, temperature, humidity, pm25, pm10, co2, ch4
- Memfilter data: jika lebih dari 4 field null, data dilewati
- Menyimpan hasil ke MySQL dengan `entry_id` dan `created_at`
- Siap di-deploy dan di-scale

---

### 🛠️ Struktur File

```
.
├── main.py              # Fetcher ThingSpeak + DB Logger
├── .env.example         # Variabel environment (API & DB) sebagai template
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

# MySQL Aiven
DB_HOST=your-aiven-hostname
DB_PORT=your-port
DB_USER=your-user
DB_PASSWORD=your-password
DB_NAME=your-db-name
```

---

### 🚀 Menjalankan Aplikasi

```bash
python main.py
```

> Script akan mengambil data dari ThingSpeak API setiap 1 jam dan menyimpannya ke database.

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

### 🗃️ Struktur Tabel MySQL

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
Data entry_id 123 inserted.
Data entry_id 124 dilewati karena terlalu banyak data null (5/8).
Waiting for 1 hour before next fetch...
```

---

### 🤝 Kontribusi

Pull request dan issue sangat diterima! Silakan fork dan kembangkan.

---

### 📄 Lisensi

MIT License © 2025 Rifqi Abdulaziz

---
