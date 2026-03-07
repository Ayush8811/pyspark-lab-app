"""
Migration: Create practice_list_problems and practice_list_progress tables.
Safe to run multiple times (idempotent).

Usage:
  python migrate_practice_list.py
"""
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./pyspark_local.db")

engine = create_engine(DATABASE_URL)

def get_table_migrations(is_postgres):
    """Return migrations appropriate for the database engine."""
    if is_postgres:
        return [
            ("practice_list_problems", """
                CREATE TABLE IF NOT EXISTS practice_list_problems (
                    id SERIAL PRIMARY KEY,
                    track VARCHAR NOT NULL,
                    title VARCHAR NOT NULL,
                    description TEXT NOT NULL,
                    difficulty VARCHAR NOT NULL,
                    window_function_type VARCHAR NOT NULL,
                    use_case_category VARCHAR NOT NULL,
                    order_index INTEGER DEFAULT 0,
                    datasets TEXT NOT NULL,
                    expected_output TEXT NOT NULL,
                    initial_code_pyspark TEXT NOT NULL,
                    initial_code_sql TEXT NOT NULL,
                    solution_code_pyspark TEXT NOT NULL,
                    solution_code_sql TEXT NOT NULL,
                    explanation TEXT NOT NULL
                )
            """),
            ("practice_list_progress", """
                CREATE TABLE IF NOT EXISTS practice_list_progress (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    problem_id INTEGER NOT NULL REFERENCES practice_list_problems(id),
                    language VARCHAR NOT NULL,
                    solved_at VARCHAR NOT NULL
                )
            """),
        ]
    else:
        return [
            ("practice_list_problems", """
                CREATE TABLE IF NOT EXISTS practice_list_problems (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    track VARCHAR NOT NULL,
                    title VARCHAR NOT NULL,
                    description TEXT NOT NULL,
                    difficulty VARCHAR NOT NULL,
                    window_function_type VARCHAR NOT NULL,
                    use_case_category VARCHAR NOT NULL,
                    order_index INTEGER DEFAULT 0,
                    datasets TEXT NOT NULL,
                    expected_output TEXT NOT NULL,
                    initial_code_pyspark TEXT NOT NULL,
                    initial_code_sql TEXT NOT NULL,
                    solution_code_pyspark TEXT NOT NULL,
                    solution_code_sql TEXT NOT NULL,
                    explanation TEXT NOT NULL
                )
            """),
            ("practice_list_progress", """
                CREATE TABLE IF NOT EXISTS practice_list_progress (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL REFERENCES users(id),
                    problem_id INTEGER NOT NULL REFERENCES practice_list_problems(id),
                    language VARCHAR NOT NULL,
                    solved_at VARCHAR NOT NULL
                )
            """),
        ]

def run_migrations():
    is_postgres = "postgresql" in DATABASE_URL or "postgres" in DATABASE_URL
    table_migrations = get_table_migrations(is_postgres)
    with engine.connect() as conn:
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
