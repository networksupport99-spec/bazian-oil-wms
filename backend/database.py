"""database.py — Dual-mode: PostgreSQL (cloud) via DATABASE_URL, else SQLite (local)."""
from __future__ import annotations
import contextlib
import os
import sys
from datetime import datetime, date as _date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

# ── DB mode detection ─────────────────────────────────────────────────────────
_DB_URL = os.getenv('DATABASE_URL', '')
if _DB_URL.startswith('postgres://'):          # Railway uses postgres:// prefix
    _DB_URL = 'postgresql://' + _DB_URL[len('postgres://'):]
USE_PG = bool(_DB_URL)

# ── PostgreSQL mode ───────────────────────────────────────────────────────────
if USE_PG:
    import psycopg2
    import psycopg2.pool
    import psycopg2.extras
    _pool: Optional[Any] = None

    def _get_pool():
        global _pool
        if _pool is None:
            _pool = psycopg2.pool.ThreadedConnectionPool(
                2, 20, _DB_URL,
                cursor_factory=psycopg2.extras.RealDictCursor,
            )
        return _pool

    @contextlib.contextmanager
    def get_connection():
        p = _get_pool()
        conn = p.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            p.putconn(conn)

    def _run(conn, sql: str, params=()):
        c = conn.cursor()
        c.execute(sql, params)
        return c

    def _ins(conn, sql: str, params=()) -> int:
        c = conn.cursor()
        c.execute(sql + ' RETURNING id', params)
        return c.fetchone()['id']

    def _q(s: str) -> str:
        return s.replace('?', '%s')

    def _fefo() -> str:
        return 'ORDER BY expiry_date ASC NULLS LAST'

    def _expired_where() -> str:
        return 'expiry_date < CURRENT_DATE'

    def _near_expiry_where(days: int) -> str:
        return f"expiry_date >= CURRENT_DATE AND expiry_date <= CURRENT_DATE + INTERVAL '{days} days'"

    def _today_tx_where() -> str:
        return "created_at::DATE = CURRENT_DATE"

    def _day_expr() -> str:
        return "created_at::DATE"

    def _count_one(cur) -> int:
        return cur.fetchone()['count']

# ── SQLite mode ───────────────────────────────────────────────────────────────
else:
    import sqlite3

    def _base_dir() -> Path:
        if getattr(sys, 'frozen', False):
            return Path(sys.executable).resolve().parent
        return Path(__file__).resolve().parent

    _DESKTOP_DB = Path.home() / 'dev' / 'warehouse_app' / 'warehouse.db'
    DB_PATH     = _DESKTOP_DB if _DESKTOP_DB.exists() else _base_dir() / 'warehouse.db'

    @contextlib.contextmanager
    def get_connection():
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA foreign_keys = ON')
        conn.execute('PRAGMA journal_mode  = WAL')
        conn.execute('PRAGMA synchronous   = NORMAL')
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _run(conn, sql: str, params=()):
        return conn.execute(sql, params)

    def _ins(conn, sql: str, params=()) -> int:
        return conn.execute(sql, params).lastrowid

    def _q(s: str) -> str:
        return s  # ? placeholders are native

    def _fefo() -> str:
        return "ORDER BY CASE WHEN expiry_date IS NULL THEN 1 ELSE 0 END, expiry_date ASC"

    def _expired_where() -> str:
        return "expiry_date < date('now','localtime')"

    def _near_expiry_where(days: int) -> str:
        return f"expiry_date >= date('now','localtime') AND expiry_date <= date('now','localtime','+{days} days')"

    def _today_tx_where() -> str:
        return "created_at LIKE date('now','localtime') || '%'"

    def _day_expr() -> str:
        return "date(created_at)"

    def _count_one(cur) -> int:
        return cur.fetchone()[0]


# ── Shared ────────────────────────────────────────────────────────────────────
ASSETS_DIR = Path(__file__).resolve().parent / 'assets'
ASSETS_DIR.mkdir(exist_ok=True)

def _today() -> str:
    return datetime.now().strftime('%Y-%m-%d')

def _fmt(v) -> str:
    if v is None: return '0'
    v = float(v)
    if v == int(v): return f'{int(v):,}'
    return f'{v:.4f}'.rstrip('0').rstrip('.')

def _row(r) -> Optional[Dict]:
    return dict(r) if r is not None else None

def _rows(rs) -> List[Dict]:
    return [dict(r) for r in rs] if rs else []


# ── Schema / init ─────────────────────────────────────────────────────────────
def init_db() -> None:
    ASSETS_DIR.mkdir(parents=True, exist_ok=True)
    with get_connection() as conn:
        if USE_PG:
            for ddl in [
                """CREATE TABLE IF NOT EXISTS items (
                    id               SERIAL PRIMARY KEY,
                    name             TEXT NOT NULL,
                    category         TEXT NOT NULL CHECK(category IN ('Liquid','Powder','Glassware','Others')),
                    quantity         NUMERIC NOT NULL DEFAULT 0,
                    unit             TEXT NOT NULL,
                    brand            TEXT,
                    location         TEXT,
                    min_stock        NUMERIC DEFAULT 0,
                    notes            TEXT,
                    glass_volume     NUMERIC,
                    glass_dimensions TEXT,
                    custom_spec      TEXT,
                    expiry_date      DATE,
                    image_path       TEXT,
                    unit_size        NUMERIC,
                    created_at       TIMESTAMPTZ DEFAULT NOW(),
                    updated_at       TIMESTAMPTZ DEFAULT NOW()
                )""",
                """CREATE TABLE IF NOT EXISTS transactions (
                    id               SERIAL PRIMARY KEY,
                    item_id          INTEGER NOT NULL REFERENCES items(id),
                    transaction_type TEXT NOT NULL CHECK(transaction_type IN ('IN','OUT')),
                    quantity         NUMERIC NOT NULL,
                    previous_qty     NUMERIC NOT NULL,
                    new_qty          NUMERIC NOT NULL,
                    notes            TEXT,
                    performed_by     TEXT DEFAULT 'Warehouse Staff',
                    created_at       TIMESTAMPTZ DEFAULT NOW()
                )""",
                """CREATE TABLE IF NOT EXISTS users (
                    id            SERIAL PRIMARY KEY,
                    username      TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    full_name     TEXT,
                    role          TEXT NOT NULL DEFAULT 'user' CHECK(role IN ('admin','user')),
                    can_checkin   BOOLEAN NOT NULL DEFAULT TRUE,
                    can_checkout  BOOLEAN NOT NULL DEFAULT TRUE,
                    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at    TIMESTAMPTZ DEFAULT NOW()
                )""",
                """CREATE TABLE IF NOT EXISTS batches (
                    id          SERIAL PRIMARY KEY,
                    item_id     INTEGER NOT NULL REFERENCES items(id),
                    quantity    NUMERIC NOT NULL DEFAULT 0,
                    expiry_date DATE,
                    batch_ref   TEXT,
                    created_at  TIMESTAMPTZ DEFAULT NOW()
                )""",
                "CREATE INDEX IF NOT EXISTS idx_tx_item   ON transactions(item_id)",
                "CREATE INDEX IF NOT EXISTS idx_tx_date   ON transactions(created_at)",
                "CREATE INDEX IF NOT EXISTS idx_ba_item   ON batches(item_id)",
                "CREATE INDEX IF NOT EXISTS idx_ba_expiry ON batches(expiry_date)",
                "CREATE INDEX IF NOT EXISTS idx_it_name   ON items(name)",
                "CREATE INDEX IF NOT EXISTS idx_it_cat    ON items(category)",
            ]:
                _run(conn, ddl)
        else:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS items (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    name             TEXT    NOT NULL,
                    category         TEXT    NOT NULL CHECK(category IN ('Liquid','Powder','Glassware','Others')),
                    quantity         REAL    NOT NULL DEFAULT 0,
                    unit             TEXT    NOT NULL,
                    brand            TEXT,
                    location         TEXT,
                    min_stock        REAL    DEFAULT 0,
                    notes            TEXT,
                    glass_volume     REAL,
                    glass_dimensions TEXT,
                    custom_spec      TEXT,
                    created_at       TEXT    DEFAULT (datetime('now','localtime')),
                    updated_at       TEXT    DEFAULT (datetime('now','localtime'))
                );
                CREATE TABLE IF NOT EXISTS transactions (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id          INTEGER NOT NULL,
                    transaction_type TEXT    NOT NULL CHECK(transaction_type IN ('IN','OUT')),
                    quantity         REAL    NOT NULL,
                    previous_qty     REAL    NOT NULL,
                    new_qty          REAL    NOT NULL,
                    notes            TEXT,
                    performed_by     TEXT    DEFAULT 'Warehouse Staff',
                    created_at       TEXT    DEFAULT (datetime('now','localtime')),
                    FOREIGN KEY (item_id) REFERENCES items(id)
                );
                CREATE TABLE IF NOT EXISTS users (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    username      TEXT    UNIQUE NOT NULL COLLATE NOCASE,
                    password_hash TEXT    NOT NULL,
                    full_name     TEXT,
                    role          TEXT    NOT NULL DEFAULT 'user' CHECK(role IN ('admin','user')),
                    can_checkin   INTEGER NOT NULL DEFAULT 1,
                    can_checkout  INTEGER NOT NULL DEFAULT 1,
                    is_active     INTEGER NOT NULL DEFAULT 1,
                    created_at    TEXT    DEFAULT (datetime('now','localtime'))
                );
                CREATE TABLE IF NOT EXISTS batches (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id     INTEGER NOT NULL,
                    quantity    REAL    NOT NULL DEFAULT 0,
                    expiry_date TEXT,
                    batch_ref   TEXT,
                    created_at  TEXT    DEFAULT (datetime('now','localtime')),
                    FOREIGN KEY (item_id) REFERENCES items(id)
                );
                CREATE INDEX IF NOT EXISTS idx_transactions_item    ON transactions(item_id);
                CREATE INDEX IF NOT EXISTS idx_transactions_created ON transactions(created_at);
                CREATE INDEX IF NOT EXISTS idx_batches_item         ON batches(item_id);
                CREATE INDEX IF NOT EXISTS idx_batches_expiry       ON batches(expiry_date);
                CREATE INDEX IF NOT EXISTS idx_items_name           ON items(name COLLATE NOCASE);
                CREATE INDEX IF NOT EXISTS idx_items_category       ON items(category);
            """)
            existing = {r[1] for r in conn.execute("PRAGMA table_info(items)")}
            for col, typ in [("expiry_date","TEXT"),("image_path","TEXT"),("unit_size","REAL")]:
                if col not in existing:
                    conn.execute(f"ALTER TABLE items ADD COLUMN {col} {typ}")
            for r in conn.execute(
                "SELECT id,quantity,expiry_date FROM items "
                "WHERE id NOT IN (SELECT DISTINCT item_id FROM batches)"
            ).fetchall():
                conn.execute(
                    "INSERT INTO batches (item_id,quantity,expiry_date,batch_ref) VALUES (?,?,?,?)",
                    (r['id'], r['quantity'], r['expiry_date'], 'Initial'),
                )

        if USE_PG:
            n = _run(conn, "SELECT COUNT(*) AS count FROM users").fetchone()['count']
        else:
            n = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        if n == 0:
            from auth import hash_password
            _ins(conn, _q(
                "INSERT INTO users (username,password_hash,full_name,role,can_checkin,can_checkout,is_active) "
                "VALUES (?,?,?,?,?,?,?)"
            ), ('admin', hash_password('admin123'), 'Administrator', 'admin', True, True, True))


# ── Auth ──────────────────────────────────────────────────────────────────────
def get_user_by_username(username: str) -> Optional[Dict]:
    with get_connection() as conn:
        if USE_PG:
            row = _run(conn, _q("SELECT * FROM users WHERE LOWER(username)=LOWER(?) AND is_active=TRUE"),
                       (username,)).fetchone()
        else:
            row = _run(conn, "SELECT * FROM users WHERE username=? COLLATE NOCASE AND is_active=1",
                       (username,)).fetchone()
        return _row(row)


# ── Users CRUD ────────────────────────────────────────────────────────────────
def get_users() -> List[Dict]:
    with get_connection() as conn:
        rows = _rows(_run(conn,
            "SELECT id,username,full_name,role,can_checkin,can_checkout,is_active,created_at "
            "FROM users ORDER BY role DESC, username"
        ).fetchall())
        for r in rows:
            r['can_checkin']  = bool(r['can_checkin'])
            r['can_checkout'] = bool(r['can_checkout'])
            r['is_active']    = bool(r['is_active'])
        return rows


def add_user(data: Dict) -> int:
    from auth import hash_password
    with get_connection() as conn:
        return _ins(conn, _q(
            "INSERT INTO users (username,password_hash,full_name,role,can_checkin,can_checkout,is_active) "
            "VALUES (?,?,?,?,?,?,?)"
        ), (
            data['username'], hash_password(data['password']),
            data.get('full_name') or None, data.get('role','user'),
            bool(data.get('can_checkin', True)),
            bool(data.get('can_checkout', True)),
            bool(data.get('is_active', True)),
        ))


def update_user(user_id: int, data: Dict) -> None:
    from auth import hash_password
    with get_connection() as conn:
        if data.get('password'):
            _run(conn, _q(
                "UPDATE users SET full_name=?,role=?,can_checkin=?,can_checkout=?,is_active=?,password_hash=? WHERE id=?"
            ), (
                data.get('full_name') or None, data.get('role','user'),
                bool(data.get('can_checkin', True)), bool(data.get('can_checkout', True)),
                bool(data.get('is_active', True)), hash_password(data['password']), user_id,
            ))
        else:
            _run(conn, _q(
                "UPDATE users SET full_name=?,role=?,can_checkin=?,can_checkout=?,is_active=? WHERE id=?"
            ), (
                data.get('full_name') or None, data.get('role','user'),
                bool(data.get('can_checkin', True)), bool(data.get('can_checkout', True)),
                bool(data.get('is_active', True)), user_id,
            ))


def delete_user(user_id: int) -> None:
    with get_connection() as conn:
        _run(conn, _q("DELETE FROM users WHERE id=?"), (user_id,))


# ── Items CRUD ────────────────────────────────────────────────────────────────
def add_item(data: Dict[str, Any], performed_by: str = 'System'):
    name      = data['name']
    unit_size = data.get('unit_size')
    qty       = float(data.get('quantity', 0))

    with get_connection() as conn:
        if unit_size is not None:
            dup = _row(_run(conn, _q(
                "SELECT id,quantity,unit FROM items WHERE LOWER(name)=LOWER(?) AND unit_size=?"
            ), (name, unit_size)).fetchone())
        else:
            dup = _row(_run(conn, _q(
                "SELECT id,quantity,unit FROM items WHERE LOWER(name)=LOWER(?) AND unit_size IS NULL"
            ), (name,)).fetchone())

        if dup:
            item_id = dup['id']
            prev    = float(dup['quantity'])
            new_qty = prev + qty
            if USE_PG:
                _run(conn, _q("UPDATE items SET quantity=?,updated_at=NOW() WHERE id=?"), (new_qty, item_id))
            else:
                _run(conn, "UPDATE items SET quantity=?,updated_at=datetime('now','localtime') WHERE id=?",
                     (new_qty, item_id))
            _ins(conn, _q(
                "INSERT INTO batches (item_id,quantity,expiry_date,batch_ref) VALUES (?,?,?,?)"
            ), (item_id, qty, data.get('expiry_date'), data.get('batch_ref') or 'Merged batch'))
            _ins(conn, _q(
                "INSERT INTO transactions (item_id,transaction_type,quantity,previous_qty,new_qty,notes,performed_by) "
                "VALUES (?,?,?,?,?,?,?)"
            ), (item_id, 'IN', qty, prev, new_qty, 'Smart merge — batch added', performed_by))
            return item_id, True

        item_id = _ins(conn, _q(
            "INSERT INTO items "
            "(name,category,quantity,unit,unit_size,brand,location,min_stock,notes,"
            "glass_volume,glass_dimensions,custom_spec,expiry_date,image_path) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        ), (
            name, data['category'], qty, data['unit'],
            unit_size,
            data.get('brand') or None, data.get('location') or None,
            float(data.get('min_stock', 0)), data.get('notes') or None,
            data.get('glass_volume') or None, data.get('glass_dimensions') or None,
            data.get('custom_spec') or None, data.get('expiry_date') or None,
            data.get('image_path') or None,
        ))
        _ins(conn, _q(
            "INSERT INTO batches (item_id,quantity,expiry_date,batch_ref) VALUES (?,?,?,?)"
        ), (item_id, qty, data.get('expiry_date'), data.get('batch_ref') or 'Batch 1'))
        _ins(conn, _q(
            "INSERT INTO transactions (item_id,transaction_type,quantity,previous_qty,new_qty,notes,performed_by) "
            "VALUES (?,?,?,?,?,?,?)"
        ), (item_id, 'IN', qty, 0, qty, 'Initial stock registration', performed_by))
        return item_id, False


def get_items(category: str = None, search: str = None) -> List[Dict]:
    query  = "SELECT * FROM items WHERE 1=1"
    params: list = []
    if category and category != 'All':
        query += _q(" AND category=?")
        params.append(category)
    if search:
        s = f'%{search}%'
        query += _q(
            " AND (LOWER(name) LIKE LOWER(?)"
            " OR LOWER(COALESCE(brand,'')) LIKE LOWER(?)"
            " OR LOWER(COALESCE(location,'')) LIKE LOWER(?))"
        )
        params += [s, s, s]
    query += " ORDER BY updated_at DESC"
    with get_connection() as conn:
        return _rows(_run(conn, query, params).fetchall())


def get_item(item_id: int) -> Optional[Dict]:
    with get_connection() as conn:
        return _row(_run(conn, _q("SELECT * FROM items WHERE id=?"), (item_id,)).fetchone())


def update_item(item_id: int, data: Dict[str, Any]) -> None:
    if USE_PG:
        ts_sql = _q("UPDATE items SET name=?,category=?,unit=?,unit_size=?,brand=?,location=?,min_stock=?,notes=?,glass_volume=?,glass_dimensions=?,custom_spec=?,expiry_date=?,image_path=?,updated_at=NOW() WHERE id=?")
    else:
        ts_sql = "UPDATE items SET name=?,category=?,unit=?,unit_size=?,brand=?,location=?,min_stock=?,notes=?,glass_volume=?,glass_dimensions=?,custom_spec=?,expiry_date=?,image_path=?,updated_at=datetime('now','localtime') WHERE id=?"
    with get_connection() as conn:
        _run(conn, ts_sql, (
            data['name'], data['category'], data['unit'],
            data.get('unit_size') or None,
            data.get('brand') or None, data.get('location') or None,
            float(data.get('min_stock', 0)), data.get('notes') or None,
            data.get('glass_volume') or None, data.get('glass_dimensions') or None,
            data.get('custom_spec') or None, data.get('expiry_date') or None,
            data.get('image_path') or None, item_id,
        ))


def delete_item(item_id: int) -> None:
    with get_connection() as conn:
        _run(conn, _q("DELETE FROM batches WHERE item_id=?"),     (item_id,))
        _run(conn, _q("DELETE FROM transactions WHERE item_id=?"), (item_id,))
        _run(conn, _q("DELETE FROM items WHERE id=?"),             (item_id,))


# ── Batch operations ──────────────────────────────────────────────────────────
def get_item_batches(item_id: int) -> List[Dict]:
    sql = _q(f"SELECT * FROM batches WHERE item_id=? AND quantity>0 {_fefo()}")
    with get_connection() as conn:
        return _rows(_run(conn, sql, (item_id,)).fetchall())


def add_batch(item_id: int, qty: float, expiry_date: str = None,
              batch_ref: str = '', notes: str = '', performed_by: str = 'Warehouse Staff') -> None:
    if qty <= 0: raise ValueError('Quantity must be greater than zero.')
    with get_connection() as conn:
        row = _row(_run(conn, _q("SELECT quantity FROM items WHERE id=?"), (item_id,)).fetchone())
        if not row: raise ValueError('Item not found.')
        prev    = float(row['quantity'])
        new_qty = prev + qty
        if USE_PG:
            _run(conn, _q("UPDATE items SET quantity=?,updated_at=NOW() WHERE id=?"), (new_qty, item_id))
        else:
            _run(conn, "UPDATE items SET quantity=?,updated_at=datetime('now','localtime') WHERE id=?",
                 (new_qty, item_id))
        _ins(conn, _q(
            "INSERT INTO batches (item_id,quantity,expiry_date,batch_ref) VALUES (?,?,?,?)"
        ), (item_id, qty, expiry_date or None, batch_ref or f'Batch {_today()}'))
        _ins(conn, _q(
            "INSERT INTO transactions (item_id,transaction_type,quantity,previous_qty,new_qty,notes,performed_by) "
            "VALUES (?,?,?,?,?,?,?)"
        ), (item_id, 'IN', qty, prev, new_qty, notes, performed_by))


def checkout_item(item_id: int, qty: float, notes: str = '',
                  performed_by: str = 'Warehouse Staff') -> Dict:
    with get_connection() as conn:
        row = _row(_run(conn, _q("SELECT name,quantity,unit FROM items WHERE id=?"), (item_id,)).fetchone())
        if not row: raise ValueError('Item not found.')
        prev = float(row['quantity'])
        if qty <= 0:   raise ValueError('Quantity must be greater than zero.')
        if qty > prev: raise ValueError(f"Insufficient stock. Available: {_fmt(prev)} {row['unit']}")

        batches = _rows(_run(conn, _q(
            f"SELECT id,quantity,expiry_date FROM batches WHERE item_id=? AND quantity>0 {_fefo()}"
        ), (item_id,)).fetchall())

        remaining = qty
        for batch in batches:
            if remaining <= 0: break
            deduct    = min(float(batch['quantity']), remaining)
            new_b_qty = float(batch['quantity']) - deduct
            remaining -= deduct
            if new_b_qty <= 0:
                _run(conn, _q("DELETE FROM batches WHERE id=?"), (batch['id'],))
            else:
                _run(conn, _q("UPDATE batches SET quantity=? WHERE id=?"), (new_b_qty, batch['id']))

        new_qty = prev - qty
        if USE_PG:
            _run(conn, _q("UPDATE items SET quantity=?,updated_at=NOW() WHERE id=?"), (new_qty, item_id))
        else:
            _run(conn, "UPDATE items SET quantity=?,updated_at=datetime('now','localtime') WHERE id=?",
                 (new_qty, item_id))
        _ins(conn, _q(
            "INSERT INTO transactions (item_id,transaction_type,quantity,previous_qty,new_qty,notes,performed_by) "
            "VALUES (?,?,?,?,?,?,?)"
        ), (item_id, 'OUT', qty, prev, new_qty, notes, performed_by))
        return {'item': row['name'], 'unit': row['unit'],
                'previous': prev, 'withdrawn': qty, 'remaining': new_qty}


# ── Expiry queries ─────────────────────────────────────────────────────────────
def get_expired_batches() -> List[Dict]:
    if USE_PG:
        sql = (
            "SELECT b.id,b.item_id,b.quantity,b.expiry_date,b.batch_ref,"
            "i.name AS item_name,i.unit,i.category,i.location,"
            "EXTRACT(DAY FROM CURRENT_DATE - b.expiry_date)::INTEGER AS days_overdue "
            "FROM batches b JOIN items i ON b.item_id=i.id "
            f"WHERE {_expired_where()} AND b.quantity>0 {_fefo()}"
        )
        with get_connection() as conn:
            return _rows(_run(conn, sql).fetchall())
    else:
        today = _today()
        with get_connection() as conn:
            return _rows(_run(conn, """
                SELECT b.id,b.item_id,b.quantity,b.expiry_date,b.batch_ref,
                       i.name AS item_name,i.unit,i.category,i.location,
                       CAST(julianday('now','localtime')-julianday(b.expiry_date) AS INTEGER) AS days_overdue
                FROM batches b JOIN items i ON b.item_id=i.id
                WHERE b.expiry_date IS NOT NULL AND b.expiry_date < ? AND b.quantity > 0
                ORDER BY b.expiry_date ASC
            """, (today,)).fetchall())


def get_near_expiry_batches(days: int = 30) -> List[Dict]:
    if USE_PG:
        sql = (
            "SELECT b.id,b.item_id,b.quantity,b.expiry_date,b.batch_ref,"
            "i.name AS item_name,i.unit,i.category,i.location,"
            f"EXTRACT(DAY FROM b.expiry_date - CURRENT_DATE)::INTEGER AS days_left "
            "FROM batches b JOIN items i ON b.item_id=i.id "
            f"WHERE {_near_expiry_where(days)} AND b.quantity>0 {_fefo()}"
        )
        with get_connection() as conn:
            return _rows(_run(conn, sql).fetchall())
    else:
        today  = _today()
        cutoff = (_date.fromisoformat(today) + timedelta(days=days)).isoformat()
        with get_connection() as conn:
            return _rows(_run(conn, """
                SELECT b.id,b.item_id,b.quantity,b.expiry_date,b.batch_ref,
                       i.name AS item_name,i.unit,i.category,i.location,
                       CAST(julianday(b.expiry_date)-julianday('now','localtime') AS INTEGER) AS days_left
                FROM batches b JOIN items i ON b.item_id=i.id
                WHERE b.expiry_date IS NOT NULL AND b.expiry_date >= ? AND b.expiry_date <= ? AND b.quantity > 0
                ORDER BY b.expiry_date ASC
            """, (today, cutoff)).fetchall())


# ── Transactions / Activity ───────────────────────────────────────────────────
def get_activity_log(performed_by: str = None, date_from: str = None,
                     date_to: str = None, transaction_type: str = None,
                     search: str = None, limit: int = 500) -> List[Dict]:
    query  = (
        "SELECT t.id,t.transaction_type,t.quantity,t.previous_qty,t.new_qty,"
        "t.notes,t.performed_by,t.created_at,"
        "i.name AS item_name,i.unit,i.category "
        "FROM transactions t JOIN items i ON t.item_id=i.id WHERE 1=1"
    )
    params: list = []
    if performed_by:
        query += _q(" AND LOWER(t.performed_by)=LOWER(?)")
        params.append(performed_by)
    if date_from:
        if USE_PG:
            query += " AND t.created_at::DATE >= %s"
        else:
            query += " AND t.created_at >= ?"
        params.append(date_from)
    if date_to:
        if USE_PG:
            query += " AND t.created_at::DATE <= %s"
        else:
            query += " AND t.created_at < ?"
        params.append(date_to)
    if transaction_type:
        query += _q(" AND t.transaction_type=?")
        params.append(transaction_type)
    if search:
        query += _q(" AND LOWER(i.name) LIKE LOWER(?)")
        params.append(f'%{search}%')
    query += _q(" ORDER BY t.created_at DESC LIMIT ?")
    params.append(limit)
    with get_connection() as conn:
        return _rows(_run(conn, query, params).fetchall())


# ── Dashboard stats ───────────────────────────────────────────────────────────
def get_dashboard_stats() -> Dict:
    with get_connection() as conn:
        total = _run(conn, "SELECT COUNT(*) AS count FROM items").fetchone()
        total_count = total['count'] if USE_PG else total[0]

        cats = _rows(_run(conn,
            "SELECT category,COUNT(*) AS cnt,COALESCE(SUM(quantity),0) AS total_qty "
            "FROM items GROUP BY category ORDER BY cnt DESC"
        ).fetchall())

        ls = _run(conn,
            "SELECT COUNT(*) AS count FROM items WHERE min_stock>0 AND quantity<=min_stock AND quantity>0"
        ).fetchone()
        low_stock = ls['count'] if USE_PG else ls[0]

        oos = _run(conn, "SELECT COUNT(*) AS count FROM items WHERE quantity=0").fetchone()
        out_of_stock = oos['count'] if USE_PG else oos[0]

        today_str = _today()
        if USE_PG:
            today_txns = _run(conn,
                "SELECT COUNT(*) AS count FROM transactions WHERE created_at::DATE = CURRENT_DATE"
            ).fetchone()['count']
            expired_count = _run(conn,
                f"SELECT COUNT(*) AS count FROM batches WHERE {_expired_where()} AND quantity > 0"
            ).fetchone()['count']
            cutoff_q = f"CURRENT_DATE + INTERVAL '30 days'"
            near_expiry_count = _run(conn,
                f"SELECT COUNT(*) AS count FROM batches WHERE {_near_expiry_where(30)} AND quantity > 0"
            ).fetchone()['count']
        else:
            today_txns = _run(conn,
                "SELECT COUNT(*) FROM transactions WHERE created_at LIKE ?",
                (f'{today_str}%',)
            ).fetchone()[0]
            cutoff = (_date.fromisoformat(today_str) + timedelta(days=30)).isoformat()
            expired_count = _run(conn,
                "SELECT COUNT(*) FROM batches WHERE expiry_date IS NOT NULL AND expiry_date < ? AND quantity > 0",
                (today_str,)
            ).fetchone()[0]
            near_expiry_count = _run(conn,
                "SELECT COUNT(*) FROM batches WHERE expiry_date IS NOT NULL "
                "AND expiry_date >= ? AND expiry_date <= ? AND quantity > 0",
                (today_str, cutoff)
            ).fetchone()[0]

        recent = _rows(_run(conn, _q("""
            SELECT t.transaction_type,t.quantity,t.new_qty,t.created_at,t.performed_by,t.notes,
                   i.name AS item_name,i.unit,i.category
            FROM transactions t JOIN items i ON t.item_id=i.id
            ORDER BY t.created_at DESC LIMIT ?
        """), (10,)).fetchall())

        low_items = _rows(_run(conn,
            "SELECT id,name,category,unit,quantity,min_stock,location FROM items "
            "WHERE min_stock>0 AND quantity<=min_stock AND quantity>0 "
            "ORDER BY (CAST(quantity AS FLOAT)/CAST(min_stock AS FLOAT)) ASC "
            + _q("LIMIT ?"), (10,)
        ).fetchall())

        return {
            "total_items":         total_count,
            "categories":          cats,
            "low_stock":           low_stock,
            "out_of_stock":        out_of_stock,
            "today_transactions":  today_txns,
            "expired_batches":     expired_count,
            "near_expiry_batches": near_expiry_count,
            "recent_transactions": recent,
            "low_stock_items":     low_items,
        }


# ── Reports ───────────────────────────────────────────────────────────────────
def get_report_data(date_from: str, date_to: str) -> Dict:
    with get_connection() as conn:
        if USE_PG:
            dr_where = "t.created_at::DATE >= %s AND t.created_at::DATE <= %s"
            day_expr = "t.created_at::DATE AS day"
            day_grp  = "t.created_at::DATE"
        else:
            dr_where = "t.created_at >= ? AND t.created_at < ?"
            day_expr = "date(t.created_at) AS day"
            day_grp  = "date(t.created_at)"

        p2 = [date_from, date_to]

        summary = _row(_run(conn,
            f"SELECT COALESCE(SUM(CASE WHEN transaction_type='IN'  THEN quantity ELSE 0 END),0) AS total_in,"
            f"COALESCE(SUM(CASE WHEN transaction_type='OUT' THEN quantity ELSE 0 END),0) AS total_out,"
            f"COUNT(CASE WHEN transaction_type='IN'  THEN 1 END) AS in_count,"
            f"COUNT(CASE WHEN transaction_type='OUT' THEN 1 END) AS out_count,"
            f"COUNT(DISTINCT item_id) AS items_touched "
            f"FROM transactions t WHERE {dr_where}", p2
        ).fetchone())

        top_items = _rows(_run(conn,
            f"SELECT i.name,i.unit,i.category,"
            f"COALESCE(SUM(CASE WHEN t.transaction_type='IN'  THEN t.quantity ELSE 0 END),0) AS qty_in,"
            f"COALESCE(SUM(CASE WHEN t.transaction_type='OUT' THEN t.quantity ELSE 0 END),0) AS qty_out,"
            f"COUNT(*) AS txn_count "
            f"FROM transactions t JOIN items i ON t.item_id=i.id "
            f"WHERE {dr_where} GROUP BY i.id,i.name,i.unit,i.category "
            f"ORDER BY txn_count DESC,qty_out DESC "
            + _q("LIMIT ?"), p2 + [15]
        ).fetchall())

        cats = _rows(_run(conn,
            f"SELECT i.category,"
            f"COALESCE(SUM(CASE WHEN t.transaction_type='IN'  THEN t.quantity ELSE 0 END),0) AS qty_in,"
            f"COALESCE(SUM(CASE WHEN t.transaction_type='OUT' THEN t.quantity ELSE 0 END),0) AS qty_out,"
            f"COUNT(*) AS txn_count "
            f"FROM transactions t JOIN items i ON t.item_id=i.id "
            f"WHERE {dr_where} GROUP BY i.category ORDER BY txn_count DESC", p2
        ).fetchall())

        daily = _rows(_run(conn,
            f"SELECT {day_expr},"
            f"COALESCE(SUM(CASE WHEN transaction_type='IN'  THEN quantity ELSE 0 END),0) AS qty_in,"
            f"COALESCE(SUM(CASE WHEN transaction_type='OUT' THEN quantity ELSE 0 END),0) AS qty_out,"
            f"COUNT(*) AS txn_count "
            f"FROM transactions t WHERE {dr_where} "
            f"GROUP BY {day_grp} ORDER BY {day_grp} ASC", p2
        ).fetchall())

        txns = _rows(_run(conn,
            f"SELECT t.transaction_type,t.quantity,t.previous_qty,t.new_qty,"
            f"t.created_at,t.performed_by,t.notes,"
            f"i.name AS item_name,i.unit,i.category "
            f"FROM transactions t JOIN items i ON t.item_id=i.id "
            f"WHERE {dr_where} ORDER BY t.created_at DESC "
            + _q("LIMIT ?"), p2 + [500]
        ).fetchall())

        return {
            "total_in":      float(summary['total_in']  if summary else 0),
            "total_out":     float(summary['total_out'] if summary else 0),
            "in_count":      summary['in_count']        if summary else 0,
            "out_count":     summary['out_count']       if summary else 0,
            "items_touched": summary['items_touched']   if summary else 0,
            "top_items":     top_items,
            "categories":    cats,
            "daily":         daily,
            "transactions":  txns,
        }
