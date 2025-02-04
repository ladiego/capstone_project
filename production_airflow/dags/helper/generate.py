import random
from faker import Faker
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import psycopg2


fake = Faker()

def insert_data():
    conn = (psycopg2.connect(
            dbname='project_caps3',
            user='dewa_capstone',
            password='dewa_capstone',
            host='host.docker.internal',
            port='5435'
        ))
    if conn is None:
        return

    cursor = conn.cursor()

    try:
        # Insert users
        users = []
        for _ in range(5):
            name = fake.name()
            email = name.lower().replace(" ", ".") + "@example.com"
            address = fake.address().replace("\n", ", ")
            gender = random.choice(['Male', 'Female'])
            created_at = datetime.now(ZoneInfo('Asia/Jakarta')).strftime('%Y-%m-%d %H:%M:%S')
            users.append((name, email, address, gender, created_at))
        
        cursor.executemany("INSERT INTO library.users (name, email, address, gender) VALUES (%s, %s, %s, %s)", users)

        # Insert books
        books = []
        for _ in range(5):
            title = fake.sentence(nb_words=3)
            author = fake.name()
            publisher = fake.company()
            release_year = random.randint(2000, 2023)
            stock = random.randint(1, 10)  # Ensure stock is at least 1 for availability
            created_at = datetime.now(ZoneInfo('Asia/Jakarta')).strftime('%Y-%m-%d %H:%M:%S')
            books.append((title, author, publisher, release_year, stock, created_at))
        
        cursor.executemany("INSERT INTO library.books (title, author, publisher, release_year, stock) VALUES (%s, %s, %s, %s, %s)", books)

        # Insert rents
        cursor.execute("SELECT user_id FROM library.users")
        user_ids = [row[0] for row in cursor.fetchall()]

        cursor.execute("SELECT book_id, stock FROM library.books WHERE stock > 0")
        available_books = cursor.fetchall()

        if not user_ids:
            print("No users found in the database. Skipping rents insertion.")
            return

        if not available_books:
            print("No available books with stock > 0. Skipping rents insertion.")
            return

        rents = []
        for _ in range(5):
            user_id = random.choice(user_ids)
            book_id, stock = random.choice(available_books)
            rent_date = datetime.now(ZoneInfo('Asia/Jakarta')) - timedelta(days=random.randint(3, 7))
            return_date = rent_date + timedelta(days=random.randint(1, 3))
            created_at = datetime.now(ZoneInfo('Asia/Jakarta')).strftime('%Y-%m-%d %H:%M:%S')
            rents.append((user_id, book_id, rent_date, return_date, created_at))

            # update book stock
            cursor.execute("UPDATE library.books SET stock = stock - 1 WHERE book_id = %s", (book_id,))

        cursor.executemany("INSERT INTO library.rents (user_id, book_id, rent_date, return_date, created_at) VALUES (%s, %s, %s, %s, %s)", rents)

        # Commit transaksi
        conn.commit()
        print("Data berhasil disisipkan ke dalam tabel!")

    except Exception as e:
        print(f"Error saat menyisipkan data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    insert_data()