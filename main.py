#!/usr/bin/env python3
"""
Import user dari DB lama (table: broadcast, kolom: id, user_name)
ke DB baru (table: users, kolom: user_id, username, first_name, last_name, last_start)

Fitur aman:
- dry-run (tidak nulis)
- batching (default 2000)
- keyset pagination (id > last_id) biar stabil untuk data banyak
- upsert ON CONFLICT (tidak duplikat)
- commit per N batch
- progress + estimasi rate
- resume token (start-from)
- limit untuk test
- logging ke file (opsional)
"""

import sys
import time
import argparse
from typing import Optional, List, Tuple

import psycopg


# =========================
# ISI URL DI SINI
# =========================
OLD_DATABASE_URL = "postgresql://postgres:kPzJwRFoLcMiGwWbIzXjteUmaJHTBdny@viaduct.proxy.rlwy.net:59912/railway"  # DB lama (ada table broadcast)
NEW_DATABASE_URL = "postgresql://postgres:dgpQXfnVKQcDNnRgOIuZBYnVaXmqHOGF@interchange.proxy.rlwy.net:52980/railway"  # DB baru (ada table users)

# Default table/kolom DB lama
OLD_TABLE = "broadcast"
OLD_ID_COL = "id"
OLD_USERNAME_COL = "user_name"

# Default table DB baru
NEW_TABLE = "users"


def clean_username(u: Optional[str]) -> Optional[str]:
    """
    Normalisasi username dari DB lama:
    - None/empty -> None
    - trim whitespace
    - hapus '@' di depan (kalau ada)
    """
    if u is None:
        return None
    u = str(u).strip()
    if not u:
        return None
    if u.startswith("@"):
        u = u[1:].strip()
        if not u:
            return None
    return u


def ensure_new_schema(conn: psycopg.Connection) -> None:
    """
    Pastikan schema table users ada. Aman walau sudah ada.
    """
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {NEW_TABLE} (
      user_id BIGINT PRIMARY KEY,
      username TEXT,
      first_name TEXT,
      last_name TEXT,
      last_start TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """)
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_users_last_start ON {NEW_TABLE}(last_start);")
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_users_user_id ON {NEW_TABLE}(user_id);")
    conn.commit()


def table_exists(conn: psycopg.Connection, table_name: str) -> bool:
    row = conn.execute("SELECT to_regclass(%s)::text;", (f"public.{table_name}",)).fetchone()
    return bool(row and row[0] == table_name)


def get_old_count(conn: psycopg.Connection, start_from: int) -> Optional[int]:
    try:
        row = conn.execute(
            f"SELECT COUNT(*) FROM {OLD_TABLE} WHERE {OLD_ID_COL} > %s;",
            (start_from,)
        ).fetchone()
        return int(row[0]) if row else None
    except Exception:
        return None


def main():
    p = argparse.ArgumentParser(description="Import users broadcast(id,user_name) -> users(user_id,username)")
    p.add_argument("--batch", type=int, default=2000, help="Batch size fetch+insert (default 2000).")
    p.add_argument("--dry-run", action="store_true", help="Simulasi saja, tidak insert.")
    p.add_argument("--limit", type=int, default=0, help="Batasi jumlah row yang diimport (0=no limit).")
    p.add_argument("--start-from", type=int, default=0, help="Resume: mulai dari id > N (default 0).")
    p.add_argument("--commit-every", type=int, default=1, help="Commit tiap N batch (default 1).")
    p.add_argument("--log", type=str, default="", help="Path log file (opsional).")
    args = p.parse_args()

    if "USER:PASS@HOST:PORT/DBNAME" in OLD_DATABASE_URL or "USER:PASS@HOST:PORT/DBNAME" in NEW_DATABASE_URL:
        print("❌ Isi OLD_DATABASE_URL dan NEW_DATABASE_URL dulu di bagian atas script.")
        sys.exit(1)

    batch_size = max(100, args.batch)
    commit_every = max(1, args.commit_every)

    log_fp = None
    if args.log:
        log_fp = open(args.log, "a", encoding="utf-8")

    def log(msg: str):
        print(msg)
        if log_fp:
            log_fp.write(msg + "\n")
            log_fp.flush()

    log("🔌 Connecting DB lama...")
    old_conn = psycopg.connect(OLD_DATABASE_URL, autocommit=False)

    log("🔌 Connecting DB baru...")
    new_conn = psycopg.connect(NEW_DATABASE_URL, autocommit=False)

    try:
        # validasi table lama
        if not table_exists(old_conn, OLD_TABLE):
            raise RuntimeError(f"Table '{OLD_TABLE}' tidak ditemukan di DB lama.")

        # pastikan schema baru
        ensure_new_schema(new_conn)
        log("✅ Schema DB baru OK (users).")

        total = get_old_count(old_conn, args.start_from)
        if total is not None:
            log(f"📊 Total kandidat import (id > {args.start_from}): {total}")

        # upsert ke DB baru
        # - insert user_id, username, last_start NOW()
        # - jika user_id sudah ada: update username hanya kalau username baru tidak NULL
        upsert_sql = f"""
        INSERT INTO {NEW_TABLE} (user_id, username, first_name, last_name, last_start)
        VALUES (%s, %s, NULL, NULL, NOW())
        ON CONFLICT (user_id) DO UPDATE SET
          username = COALESCE(EXCLUDED.username, {NEW_TABLE}.username),
          last_start = GREATEST({NEW_TABLE}.last_start, EXCLUDED.last_start);
        """

        imported = 0
        scanned = 0
        last_id = args.start_from
        batch_no = 0
        t0 = time.time()

        while True:
            rows = old_conn.execute(
                f"""
                SELECT {OLD_ID_COL}, {OLD_USERNAME_COL}
                FROM {OLD_TABLE}
                WHERE {OLD_ID_COL} > %s
                ORDER BY {OLD_ID_COL} ASC
                LIMIT %s;
                """,
                (last_id, batch_size)
            ).fetchall()

            if not rows:
                break

            batch_no += 1

            payload: List[Tuple[int, Optional[str]]] = []
            for uid, uname in rows:
                uid_int = int(uid)
                uname_clean = clean_username(uname)
                payload.append((uid_int, uname_clean))

                last_id = uid_int
                scanned += 1
                if args.limit and scanned >= args.limit:
                    break

            if args.dry_run:
                imported += len(payload)
            else:
                with new_conn.cursor() as cur:
                    cur.executemany(upsert_sql, payload)

                if (batch_no % commit_every) == 0:
                    new_conn.commit()

                imported += len(payload)

            elapsed = max(0.001, time.time() - t0)
            rate = int(imported / elapsed)
            if total is not None:
                pct = (imported / max(total, 1)) * 100.0
                log(f"✅ Batch {batch_no} | imported={imported} | last_id={last_id} | {pct:.2f}% | {rate}/s")
            else:
                log(f"✅ Batch {batch_no} | imported={imported} | last_id={last_id} | {rate}/s")

            if args.limit and scanned >= args.limit:
                break

        if not args.dry_run:
            new_conn.commit()

        log("")
        log("🎉 IMPORT SELESAI")
        log(f"   Imported/Upserted: {imported}")
        log(f"   Resume token: --start-from {last_id}")
        if args.dry_run:
            log("   Mode: DRY-RUN (tidak ada data yang ditulis)")

    finally:
        try:
            old_conn.close()
        except Exception:
            pass
        try:
            new_conn.close()
        except Exception:
            pass
        if log_fp:
            log_fp.close()


if __name__ == "__main__":
    main()
