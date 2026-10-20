"""
migrate_sqlite.py — Migrate existing SQLite warehouse.db data to PostgreSQL

Usage:
    set DATABASE_URL=postgresql://user:pass@host:5432/dbname
    python migrate_sqlite.py

This script reads from ~/dev/warehouse_app/warehouse.db (or backend/warehouse.db)
and copies all data into the PostgreSQL database, preserving IDs and timestamps.
"""
import os
import sys
import sqlite3
import psycopg2
import psycopg2.extras
from pathlib import Path

SQLITE_CANDIDATES = [
    Path.home() / "dev" / "warehouse_app" / "warehouse.db",
    Path(__file__).parent / "warehouse.db",
]

def find_sqlite() -> Path:
    for p in SQLITE_CANDIDATES:
        if p.exists():
            return p
    raise FileNotFoundError(
        f"SQLite database not found. Checked: {[str(p) for p in SQLITE_CANDIDATES]}"
    )

def migrate():
    db_url = os.getenv("DATABASE_URL", "")
    if not db_url:
        print("ERROR: DATABASE_URL environment variable is not set.")
        sys.exit(1)
    if db_url.startswith("postgres://"):
        db_url = "postgresql://" + db_url[len("postgres://"):]

    sqlite_path = find_sqlite()
    print(f"Source SQLite:  {sqlite_path}")
    print(f"Target Postgres: {db_url[:40]}...")

    sq = sqlite3.connect(str(sqlite_path))
    sq.row_factory = sqlite3.Row
    pg = psycopg2.connect(db_url, cursor_factory=psycopg2.extras.RealDictCursor)
    pg.autocommit = False

    try:
        cur = pg.cursor()

        # ── items ─────────────────────────────────────────────────────────────
        items = sq.execute("SELECT * FROM items").fetchall()
        print(f"Migrating {len(items)} items...")
        for r in items:
            cur.execute("""
                INSERT INTO items (id,name,category,quantity,unit,brand,location,min_stock,
                    notes,glass_volume,glass_dimensions,custom_spec,expiry_date,image_path,
                    unit_size,created_at,updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO NOTHING
            """, (
                r["id"], r["name"], r["category"], r["quantity"], r["unit"],
                r["brand"], r["location"], r["min_stock"], r["notes"],
                r["glass_volume"], r["glass_dimensions"], r["custom_spec"],
                r["expiry_date"], r["image_path"],
                dict(r).get("unit_size"),
                r["created_at"], r["updated_at"],
            ))
        # Reset sequence
        cur.execute("SELECT setval('items_id_seq', (SELECT MAX(id) FROM items))")

        # ── users ─────────────────────────────────────────────────────────────
        users = sq.execute("SELECT * FROM users").fetchall()
        print(f"Migrating {len(users)} users...")
        for r in users:
            cur.execute("""
                INSERT INTO users (id,username,password_hash,full_name,role,
                    can_checkin,can_checkout,is_active,created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO NOTHING
            """, (
                r["id"], r["username"], r["password_hash"], r["full_name"],
                r["role"], bool(r["can_checkin"]), bool(r["can_checkout"]),
                bool(r["is_active"]), r["created_at"],
            ))
        cur.execute("SELECT setval('users_id_seq', (SELECT MAX(id) FROM users))")

        # ── batches ───────────────────────────────────────────────────────────
        batches = sq.execute("SELECT * FROM batches").fetchall()
        print(f"Migrating {len(batches)} batches...")
        for r in batches:
            cur.execute("""
                INSERT INTO batches (id,item_id,quantity,expiry_date,batch_ref,created_at)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO NOTHING
            """, (
                r["id"], r["item_id"], r["quantity"],
                r["expiry_date"], r["batch_ref"], r["created_at"],
            ))
        cur.execute("SELECT setval('batches_id_seq', (SELECT MAX(id) FROM batches))")

        # ── transactions ──────────────────────────────────────────────────────
        txns = sq.execute("SELECT * FROM transactions").fetchall()
        print(f"Migrating {len(txns)} transactions...")
        for r in txns:
            cur.execute("""
                INSERT INTO transactions (id,item_id,transaction_type,quantity,
                    previous_qty,new_qty,notes,performed_by,created_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (id) DO NOTHING
            """, (
                r["id"], r["item_id"], r["transaction_type"],
                r["quantity"], r["previous_qty"], r["new_qty"],
                r["notes"], r["performed_by"], r["created_at"],
            ))
        cur.execute("SELECT setval('transactions_id_seq', (SELECT MAX(id) FROM transactions))")

        pg.commit()
        print("\nMigration complete!")
        print(f"  Items:        {len(items)}")
        print(f"  Users:        {len(users)}")
        print(f"  Batches:      {len(batches)}")
        print(f"  Transactions: {len(txns)}")

    except Exception as e:
        pg.rollback()
        print(f"\nERROR: {e}")
        sys.exit(1)
    finally:
        sq.close()
        pg.close()


if __name__ == "__main__":
    migrate()
