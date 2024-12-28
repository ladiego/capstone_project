import psycopg2
from psycopg2 import sql

def create_connection():
    """Membuat koneksi ke database PostgreSQL."""
    try:
        conn = psycopg2.connect(
            dbname='project_caps3',
            user='dewa_capstone',
            password='dewa_capstone',
            host='host.docker.internal',
            port='5435'
        )
        print("Koneksi ke database berhasil!")
        return conn
    except Exception as e:
        print(f"Error saat koneksi ke database: {e}")
        return None

def create_schema_and_tables():
    """Membuat schema dan tabel yang diperlukan di PostgreSQL."""
    conn = create_connection()
    if conn is None:
        return

    cursor = conn.cursor()
    
    try:
        # create schema if doesnt exist
        cursor.execute("CREATE SCHEMA IF NOT EXISTS library;")
        
        # user tables
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS library.users (
            user_id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            email VARCHAR(100) UNIQUE,
            address TEXT,
            gender VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        # book tables
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS library.books (
            book_id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            author VARCHAR(100),
            publisher VARCHAR(100),
            release_year INT,
            stock INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        # rent tables
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS library.rents (
            rent_id SERIAL PRIMARY KEY,
            user_id INT REFERENCES library.users(user_id),
            book_id INT REFERENCES library.books(book_id),
            rent_date TIMESTAMP,
            return_date TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        conn.commit()
        print("Schema dan tabel berhasil dibuat!")
        
    except Exception as e:
        print(f"Error saat membuat schema atau tabel: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    create_schema_and_tables()