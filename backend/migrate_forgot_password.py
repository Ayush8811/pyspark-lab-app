"""
Lightweight migration script to add new columns and tables.
Safe to run multiple times (all operations are idempotent).

Usage:
  python migrate_forgot_password.py
"""
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./pyspark_local.db")

engine = create_engine(DATABASE_URL)

# Column additions (ALTER TABLE — skipped if column already exists)
column_migrations = [
    ("email", "ALTER TABLE users ADD COLUMN email VARCHAR"),
    ("reset_code", "ALTER TABLE users ADD COLUMN reset_code VARCHAR"),
    ("reset_code_expires", "ALTER TABLE users ADD COLUMN reset_code_expires VARCHAR"),
    ("language", "ALTER TABLE saved_problems ADD COLUMN language VARCHAR DEFAULT 'pyspark'"),
]

# Table creations (CREATE TABLE IF NOT EXISTS — always safe)
table_migrations = [
    ("challenge_rooms", """
        CREATE TABLE IF NOT EXISTS challenge_rooms (
            id SERIAL PRIMARY KEY,
            room_code VARCHAR UNIQUE NOT NULL,
            creator_id INTEGER REFERENCES users(id),
            joiner_id INTEGER REFERENCES users(id),
            status VARCHAR DEFAULT 'waiting',
            problem_data TEXT,
            language VARCHAR DEFAULT 'pyspark',
            created_at VARCHAR
        )
    """),
]

def run_migrations():
    with engine.connect() as conn:
        # Column migrations
        for col_name, sql in column_migrations:
            try:
                conn.execute(text(sql))
                conn.commit()
                print(f"[OK] Added column '{col_name}'")
            except Exception as e:
                conn.rollback()
                error_msg = str(e).lower()
                if "duplicate" in error_msg or "already exists" in error_msg:
                    print(f"[SKIP] Column '{col_name}' already exists")
                else:
                    print(f"[ERROR] Failed to add '{col_name}': {e}")

        # Table migrations
        for table_name, sql in table_migrations:
            try:
                conn.execute(text(sql))
                conn.commit()
                print(f"[OK] Created table '{table_name}'")
            except Exception as e:
                conn.rollback()
                error_msg = str(e).lower()
                if "already exists" in error_msg:
                    print(f"[SKIP] Table '{table_name}' already exists")
                else:
                    print(f"[ERROR] Failed to create '{table_name}': {e}")

if __name__ == "__main__":
    run_migrations()
    print("Migration complete!")

