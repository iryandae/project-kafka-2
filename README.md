# Big Data System

![image](https://github.com/user-attachments/assets/44298573-6823-4c39-8622-8ae7bc36d819)


Terdapat sebuah sistem Big Data dengan arsitektur seperti gambar di atas. 
Sistem tersebut berfungsi untuk menyimulasikan pemrosesan data stream menggunakan Kafka dan Apache Spark.

## Prasyarat
- Python 3.8+
- Python Numpy
- Apache Kafka
- Apache Spark 3.5.5
- Java 8 atau 11

## Struktur Proyek
```
├── project/
    ├── api/
      ├── __pycache__/
        ├──main.cpython-312.pyc
      ├── main.py
    ├── data/
      ├── batch/
      ├── raw/
    ├── kafka/
      ├── consumer.py
      ├── producer.py
    ├── models/
    ├── spark/
      ├── train.py
```

## Menjalankan Program
Langkah awal dalam menjalankan program adalah dengan menjalankan Kafka dan Zookeeper pada docker.
Tahapan dalam memulai Kafka dan Zookeeper pada docker dapat dilihat pada repositori [project kafka sebelumnya](https://github.com/iryandae/kafka-projec)

Saya menggunakan **virtual environment** untuk menjalankan program, untuk memastikan dependensi tidak tercampur dengan sistem global.
### Producer
Setelah menjalankan Kafka dan membuat topik baru sesuai dengan nama topik pada `producer.py` dan `consumer.py`, kita dapat melanjutkan ke tahap selanjutnya untuk menjalankan program yang sudah ada dengan command berikut:
```bash
python3 kafka/producer.py
```
Ketika berhasil dijalankan, program producer seharusnya menampilkan output seperti ini:
![Cuplikan layar 2025-05-31 001431](https://github.com/user-attachments/assets/8721d76d-5d02-439e-9800-ef0c576db461)

Jika program belum berhasil dijalankan, pastikan path yang ada pada program sudah sesuai dengan path lokal perangkat anda.

### Consumer
Jika tidak masalah yang terjadi ketika menjalankan program `producer.py`, selanjutnya kita akan melakukan hal yang serupa untuk menjalankan `consumer.py`
```bash
python3 kafka/consumer.py
```
Ketika berhasil berjalan, `consumer.py` akan menerima inputan dari `producer.py` dan untuk setiap 100 data yang diterima akan disimpan ke dalam satu file sebagai satu batch. Output akan terlihat seperti gambar di bawah:

![Cuplikan layar 2025-05-31 001616](https://github.com/user-attachments/assets/26788a52-b18a-4de0-9118-4ee267eaa92b)

Sama seperti sebelumnya, jika program tidak berjalan dengan baik, pastikan path pada program sudah sesuai.

### Model Training
Setelah sudah terkumpul beberapa batch data melalui `consumer.py` kita bisa menjalankan program `train.py` pada direktori spark dengan command seperti di bawah:
```bash
python3 spark/train.py
```
Ketika program berhasil dijalankan, akan didapatkan output seperti gambar di bawah:

![Cuplikan layar 2025-05-31 001511](https://github.com/user-attachments/assets/0baff482-e364-4f1e-bf8c-5420683131eb)

Jika tidak ditemukan output serupa, mungkin ada beberapa dependensi yang belum terinstall. Dependesin dapat diinstal dan kemudian program dijalankan ulang.
Setelah berhasil dijalankan, program `train.py` akan menyimpan beberapa model yang sudah di*training* dalam bentuk direktori untuk setiap batch yang ada.

### API
Dengan model yang sudah ada, kita bisa menjalankan API untuk memeriksa apakah model yang bekerja. Jalankan command berikut untuk memulai program API:
```bash
uvicorn main:app --reload
```
Ketika program berhasil dijalankan, akan didapatkan output dengan tampilan seperti di bawah:

![Cuplikan layar 2025-05-31 001540](https://github.com/user-attachments/assets/adfa0e5d-5b5e-4c57-985e-b3cff227ffab)

Setelah didapatkan output seperti yang tertera, kita bisa mengunjungi IP yang sudah diassign untuk API tersebut.
Saya menggunakan `http://127.0.0.1:8000/docs` dan ketika mengunjungi alamat IP tersebut, akan didapatkan tampilan FastAPI seperti di bawah ini:

![Cuplikan layar 2025-06-01 201146](https://github.com/user-attachments/assets/c36f3b6b-60da-478d-84c9-0300ca03a486)

Langkah selanjutnya, tekan pada tombol "Try it out" dan kemudian pada kolom request body, masukan inputan seperi ini:

![Cuplikan layar 2025-05-30 225827](https://github.com/user-attachments/assets/5ae71bcb-bf85-4f36-ba92-8aa9aab57bab)

Tekan tombol execute dan kemudian pada bagian "Response" di bawah akan didapatkan output dengan format seperti ini:
```
{
  "cluster": [angka cluster berdasarkan data dan model]
}
```
Salah satu contohnya, untuk gambaran yang lebih jelas, seperti gambar di bawah:

![Cuplikan layar 2025-05-30 225843](https://github.com/user-attachments/assets/bc00cf30-af55-46ab-8ebf-27e745b46969)

Output dari API bisa dikostumisasi sesuai clustering yang kita lakukan ataupun data yang kita pakai. Pada kasus ini, dengan data kriminalitas yang saya pakai, cluster dibagi menjadi waktu kejadian, tempat lokasi kejadian dan jenis kasus.
