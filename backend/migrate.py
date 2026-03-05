import sqlite3
import os

print("Applying migration...")
db_path = os.path.join(os.path.dirname(__file__), 'pyspark_local.db')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

try:
    cursor.execute("ALTER TABLE users ADD COLUMN name VARCHAR")
    cursor.execute("ALTER TABLE users ADD COLUMN age INTEGER")
    cursor.execute("ALTER TABLE users ADD COLUMN bio TEXT")
    conn.commit()
    print("Successfully added name, age, and bio columns to users table.")
except sqlite3.OperationalError as e:
    if "duplicate column name" in str(e):
        print("Columns already exist.")
    else:
        print("Error:", e)

conn.close()
