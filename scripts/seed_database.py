#!/usr/bin/env python3
"""
Create the analytics schema and fill it with generated e-commerce activity.

    python scripts/seed_database.py                    # local SQLite (data/ecap.db)
    python scripts/seed_database.py --database-url postgresql+psycopg2://...   # e.g. Supabase

The database URL defaults to ECAP_DATABASE_URL (also read from .env), then to a local
SQLite file. Re-running resets the tables unless --keep is given.
"""

import argparse
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from sqlalchemy import create_engine, text  # noqa: E402

from src.database.demo_seed import seed_database  # noqa: E402

DEFAULT_URL = f"sqlite:///{(ROOT / 'data' / 'ecap.db').as_posix()}"


def dotenv_value(name: str) -> str:
    """A value from the process environment or the project's .env file ('' if unset)."""
    if os.environ.get(name):
        return os.environ[name]
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8-sig").splitlines():  # tolerate a BOM
            if line.startswith(f"{name}="):
                return line.split("=", 1)[1].strip()
    return ""


def load_dotenv_url() -> str:
    """ECAP_DATABASE_URL from the environment / .env, else the local SQLite file."""
    return dotenv_value("ECAP_DATABASE_URL") or DEFAULT_URL


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--database-url", default=None)
    parser.add_argument("--supabase", action="store_true", help="use ECAP_SUPABASE_URL from .env")
    parser.add_argument("--customers", type=int, default=4000)
    parser.add_argument("--days", type=int, default=365)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--keep", action="store_true", help="do not drop existing tables first")
    args = parser.parse_args()

    if args.supabase:
        args.database_url = dotenv_value("ECAP_SUPABASE_URL")
        if not args.database_url:
            sys.exit("ECAP_SUPABASE_URL is not set (see README: Supabase)")
    url = args.database_url or load_dotenv_url()
    safe = url.split("@")[-1] if "@" in url else url  # never print credentials
    print(f"Seeding {safe} ...")
    engine = create_engine(url, pool_pre_ping=True)
    started = time.time()
    seed_database(engine, customers=args.customers, days=args.days, seed=args.seed, reset=not args.keep)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    print(f"done in {time.time() - started:.1f}s")


if __name__ == "__main__":
    main()
