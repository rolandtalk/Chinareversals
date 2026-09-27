"""Railway Cron entrypoint for the after-market Chinareversals scan."""

from __future__ import annotations

from datetime import datetime, timezone
import sys

from sqlalchemy import text

from app import (
    ENGINE,
    ensure_database,
    persist_scan,
    scan_stocks_with_prices,
    sync_stocks_from_source,
)


LOCK_ID = 240815


def main() -> int:
    started_at = datetime.now(timezone.utc)
    print(f"[{started_at.isoformat()}] Starting after-close scan", flush=True)
    if not ensure_database():
        print("Database initialization failed", file=sys.stderr, flush=True)
        return 1

    lock_connection = None
    try:
        if ENGINE.dialect.name == "postgresql":
            lock_connection = ENGINE.connect()
            acquired = lock_connection.scalar(
                text("SELECT pg_try_advisory_lock(:lock_id)"), {"lock_id": LOCK_ID}
            )
            if not acquired:
                print("Another scan is already running; exiting cleanly", flush=True)
                return 0

        stocks, source_mode = sync_stocks_from_source()
        print(f"Loaded {len(stocks)} stocks from {source_mode}", flush=True)
        rows, histories = scan_stocks_with_prices(stocks)
        available = sum(row.get("price") is not None for row in rows)
        run_id = persist_scan(rows, source_mode, histories)
        if run_id is None:
            raise RuntimeError("Scan could not be persisted")
        elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
        print(
            f"Completed scan #{run_id}: {available}/{len(rows)} quotes in {elapsed:.1f}s",
            flush=True,
        )
        return 0
    except Exception as exc:
        print(f"After-close scan failed: {exc}", file=sys.stderr, flush=True)
        return 1
    finally:
        if lock_connection is not None:
            try:
                lock_connection.execute(
                    text("SELECT pg_advisory_unlock(:lock_id)"), {"lock_id": LOCK_ID}
                )
            finally:
                lock_connection.close()
        ENGINE.dispose()


if __name__ == "__main__":
    raise SystemExit(main())
