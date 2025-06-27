# 🌱 Sistem Informasi Pelaporan Emisi GRK Scope 1 (TAv2)

Proyek ini mengembangkan sistem otomatisasi pelaporan emisi gas rumah kaca (GRK) **Scope 1** berbasis **IoT dan Machine Learning**. Sistem ini secara rutin mengambil data dari sumber sensor dan data kegiatan pembakaran bahan bakar, lalu menghitung emisi, menyimpannya dalam basis data, dan menghasilkan laporan harian, bulanan, dan tahunan.

---

## 📁 Struktur Folder

```plaintext
TAv2/
│
├── main.py                      # Entry point: Menjalankan seluruh proses sinkronisasi dan pelaporan
│
├── db/
│   └── connection.py            # Fungsi koneksi ke database sensor dan Laravel
│
├── models/
│   ├── emission_calculator.py  # Fungsi perhitungan emisi CO2, CH4, N2O
│   └── report_generator.py     # Kelas untuk generate laporan harian, bulanan, tahunan
│
├── services/
│   ├── fuel_combustion.py      # Sinkronisasi & perhitungan emisi dari data aktivitas pembakaran
│   └── sensor_sync.py          # Sinkronisasi data sensor dari database ThingSpeak ke Laravel
│
└── README.md                   # Dokumentasi proyek
'''

# ⚙️ Ketergantungan
## 1. Database
MySQL / MariaDB

Database Laravel (laravel_db)

Database Sensor (sensor_db - misalnya dari ThingSpeak)

Tabel penting:
'''plaintext
sensor_entries

fuel_combustion_activities

sumber_emisis

fuel_properties

reports
'''

## 2. Python Packages
Instalasi:

'''bash
pip install -r requirements.txt
cp .env.example .env
'''

Lalu isi .env sesuai dengan kebutuhan masing-masing.

#⚡ Konfigurasi Database
Edit file db/connection.py:

'''python
import mysql.connector

def get_sensor_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="username_sensor",
        password="password_sensor",
        database="sensor_db"
    )

def get_laravel_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="username_laravel",
        password="password_laravel",
        database="laravel_db"
    )
'''

# 🚀 Cara Menjalankan
## 1. Jalankan seluruh sistem

'''bash
python main.py
'''

## 2. Penjadwalan otomatis
Secara default, main.py akan:

- Menarik data sensor tiap 1 jam
- Menjalankan sinkronisasi fuel_combustion
- Menghasilkan laporan report_generator

# 📘 Penjelasan Modul
Modul	Fungsi
'''plaintext
sensor_sync.py	Sinkronisasi data sensor dari database sensor ke Laravel
fuel_combustion.py	Mengambil data sumber emisi & menghitung emisi berdasarkan fuel_properties
report_generator.py	Menghitung total dan rata-rata emisi untuk laporan harian, bulanan, tahunan
connection.py	Koneksi database Laravel & Sensor
main.py	Menjalankan ketiga proses di atas secara paralel
'''

# 🧠 Logika Perhitungan Emisi
'''text
Energi (TJ) = Jumlah Konsumsi x Conversion Factor
Emisi (ton) = Energi x Emission Factor / 1000
'''

Contoh struktur JSON kolom emission_factor dan total_emission_ton:

'''json
{
  "co2": 74.1,
  "ch4": 3.2,
  "n2o": 1.5
}
'''
# 📊 Format Laporan
Laporan otomatis diinsert ke tabel reports

Terdiri dari:
'''plaintext
period_type: harian, bulanan, tahunan

period_date: tanggal awal periode

report_name: e.g., GRK_2025_06, GRK_2025_06_28

total_*, avg_* dari sensor dan hasil kalkulasi

sensor_id, komentar, sumber_emisi_id, perusahaan_id
'''
# 🛠️ Troubleshooting
Masalah	Solusi
ImportError pada get_sensor_db_connection	Pastikan connection.py berisi fungsi yang benar
Data fuel_properties kosong	Pastikan semua sumber_emisis memiliki ID fuel_properties valid
Tidak muncul laporan	Cek apakah data sensor_entries dan fuel_combustion_activities tersedia untuk tanggal tersebut

# 🧪 Testing Manual
Untuk menjalankan satu bagian saja:

'''python
# Dalam Python REPL
from services.fuel_combustion import FuelCombustionInserter
FuelCombustionInserter().run()
'''

🧾 Lisensi
Proyek ini dikembangkan sebagai bagian dari Tugas Akhir oleh @roneallza, dan juga kontribusi dari @ryanmoehs.