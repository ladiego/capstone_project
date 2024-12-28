Tugas Capstone :

Membuat 2 dag Airflow, dengan spesifikasi pengerjaan sebagai berikut :
1. Buatlah Dag Airflow untuk insert data otomapurwadikalam postgres database. dan running per jam. Dengan kriteria sebagai berikut :
- Buatlah minimal 3 tabel, dan tabel saling keterkaitan. Dimana memiliki primary key dan foreign key, serta memiliki tipe kolom data yang jelas. Misal id (type = INTEGER), nama (type = STRING). Dan seterusnya.
- Setiap table wajib memiliki kolom created_at.
- Tema tabel bebasl.
- Data bisa di isi dengan random value, asalkan tetap sesuai dengan type dari kolom data tabel.
- Data di bangun menggunakan Python, dan di schedule setiap 1 jam sekali, kemudian load ke dalam postgre database.
- Database di buat menggunakan docker compose, dengan image postgres, dan di koneksikan ke dalam aplikasi DBeaver.

2. Buatlah Dag Airflow otomatis untuk ingest data dari database ke dalam Data Warehouse, secara daily. Dan kriteria pengerjaan sebagai berikut :
- Airflow di install menggunakan docker-compose.
- Data sources di ambil dari hasil jawaban no 1, dimana database dijadikan sebagai sumber data yang akan di ingest ke dalam data warehouse.
- Data pada setiap table di filter per hari, dengan kriteria current day - 1 / H-1 ketika schedule dag di jalankan. Data dapat di filter menggunakan kolom created_at.
- Buatlah 1 operator fungsi yang dapat meng-extract semua tabel dari 1 sumber database, dan dijadikan 1 task di dalam dag. Kemudian dilanjutkan dengan 1 task terpisah untuk proses load data ke dalam tabel BigQuery.
Contoh referensi : under task customers terdapat sub-task proses extract data, dan load data / insert data kedalam BigQuery.
- Buatlah schema incremental ketika load data ke dalam BigQuery.
- Buatlah schema partition pada setiap tabel ketika load data kedalam BigQuery.
- Buatlah 1 dataset untuk 1 database source di dalam BigQuery. 
- Load data table ke dalam BigQuery project 
