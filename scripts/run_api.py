#!/usr/bin/env python3
"""
Start the API against the local SQLite database or the Supabase Postgres database.

    python scripts/run_api.py                 # local SQLite (data/ecap.db)
    python scripts/run_api.py --db supabase   # ECAP_SUPABASE_URL from .env
    python scripts/run_api.py --port 8001
"""

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from seed_database import DEFAULT_URL, dotenv_value  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--db", choices=["local", "supabase"], default="local")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()

    if args.db == "supabase":
        url = dotenv_value("ECAP_SUPABASE_URL")
        if not url:
            sys.exit("ECAP_SUPABASE_URL is not set in .env (see README: Supabase)")
    else:
        url = DEFAULT_URL
    os.environ["ECAP_DATABASE_URL"] = url
    print(f"API on http://{args.host}:{args.port} using {args.db} database "
          f"({url.split('@')[-1] if '@' in url else url})")

    import uvicorn

    os.chdir(ROOT)
    uvicorn.run("src.api.main:app", host=args.host, port=args.port)


if __name__ == "__main__":
    main()
