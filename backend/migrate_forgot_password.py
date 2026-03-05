"""
Lightweight migration script to add new columns to the users table
for the Forgot Password feature. Safe to run multiple times.

Usage:
  python migrate_forgot_password.py
"""
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./pyspark_local.db")

engine = create_engine(DATABASE_URL)

migrations = [
    ("email", "ALTER TABLE users ADD COLUMN email VARCHAR"),
    ("reset_code", "ALTER TABLE users ADD COLUMN reset_code VARCHAR"),
    ("reset_code_expires", "ALTER TABLE users ADD COLUMN reset_code_expires VARCHAR"),
]

def run_migrations():
    with engine.connect() as conn:
        for col_name, sql in migrations:
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

if __name__ == "__main__":
    run_migrations()
    print("Migration complete!")
