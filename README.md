SOAL !!!

A. Hitunglah ada berapa banyak transaksi di table Order? Aggregate COUNT pada kolom Id.

B. Hitunglah berapa jumlah Order dari seluruh transaksi di T able Order? Aggregate SUM pada kolom Total.

C. Hitung 10 product yang sering memberikan discount di tabel Order berdasarkan produk Title dari table Products?
- Join table Order dengan table produk untuk mendapatkan produk Title.
- Aggregate Count untuk mendapatkan total order dan grouping berdasarkan kolom Title dari T able Products. 
- Urutkan dari total transaksi tertinggi - terendah.
- Tampilkan 10 product teratas.

D. Hitung berapa jumlah Order dari transaksi di Table Order, berdasarkan kolom category dari tabel produk?
- Gunakan Konsep CTE, Join table Order dengan table Product, 
- Hitung total menggunakan aggregate SUM, dan grouping berdasarkan kolom Category dari table Product. 
- Urutkan dari total transaksi tertinggi - terendah.

E. Hitung berapa jumlah total Order di table Order, dari setiap title di table Product yang memiliki rating >=4?
- Gunakan Konsep CTE, Join table Order dengan table Product. 
- Hitung total menggunakan aggregate SUM, dan grouping berdasarkan kolom Title dari table Product. 
- Urutkan dari total transaksi tertinggi - terendah.

F. Dapatkan list reviews berdasarkan kategori produk = ‘Doohickey’, dimana rating dari table reviews <= 3? Dan urutkan berdasarkan table created_at di table reviews.
- Gunakan Konsep CTE, Join table Reviews dengan table Product, select kolom created_at, body dan rating dari table reviews, dan filter product category dan rating reviews <= 3. 
- Urutkan dari kolom created terbaru - terlama.

G. Ada berapa source di table Users?
- Ambil unik data di kolom source dari table Users.

H. Hitung total user di table Users yang memiliki email dari gmail.com.
- Rename column alias total_user_gmail.

I. Dapatkan list id, title, price, and created at dari table Products, dengan kriteria price antara 30 - 50, dan urutkan dari transaksi terbaru - terlama?

J. Dapatkan list Name, Email, Address, Birthdate dari table Users yang lahir diatas tahun 1997? 
- Buatlah dalam format database views.

K. Dapatkan list id, created_at, title, category, dan vendor dari table products, yang memiliki title yang sama / title muncul dengan value yang sama sebanyak lebih dari 1 kali.
- Gunakan format cte dan row_number dalam mengerjakan soal berikut.
