"""
Migration: Add 'language' column to saved_problems table.
Run once: python migrate_add_language.py
"""
from database import engine
from sqlalchemy import text

def migrate():
    with engine.connect() as conn:
        try:
            conn.execute(text("ALTER TABLE saved_problems ADD COLUMN language VARCHAR DEFAULT 'pyspark'"))
            conn.commit()
            print("Migration successful: 'language' column added to saved_problems.")
        except Exception as e:
            if "already exists" in str(e).lower() or "duplicate column" in str(e).lower():
                print("Column 'language' already exists. Skipping.")
            else:
                raise

if __name__ == "__main__":
    migrate()
