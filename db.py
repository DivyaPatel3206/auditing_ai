import sqlite3
from contextlib import contextmanager
from typing import Optional

DB_NAME = "tally.db"


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                mailing_name TEXT,
                address TEXT,
                state TEXT,
                country TEXT DEFAULT 'India',
                phone TEXT,
                email TEXT,
                financial_year_start TEXT NOT NULL,
                books_from TEXT NOT NULL,
                currency TEXT DEFAULT '₹',
                maintain_inventory TEXT DEFAULT 'Yes',
                enable_gst TEXT DEFAULT 'Yes'
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS ledgers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL,
                ledger_name TEXT NOT NULL,
                group_name TEXT NOT NULL,
                opening_balance REAL DEFAULT 0,
                balance_type TEXT DEFAULT 'Debit',
                gst_applicable TEXT DEFAULT 'No',
                gst_number TEXT,
                address TEXT,
                phone TEXT,
                email TEXT,
                UNIQUE(company_id, ledger_name),
                FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS vouchers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL,
                voucher_number TEXT NOT NULL,
                date TEXT NOT NULL,
                type TEXT NOT NULL,
                narration TEXT,
                ai_risk_level TEXT DEFAULT 'Low',
                ai_risk_score INTEGER DEFAULT 0,
                ai_flags TEXT DEFAULT '',
                FOREIGN KEY(company_id) REFERENCES companies(id) ON DELETE CASCADE
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS voucher_entries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                voucher_id INTEGER NOT NULL,
                ledger_id INTEGER NOT NULL,
                debit REAL DEFAULT 0,
                credit REAL DEFAULT 0,
                FOREIGN KEY(voucher_id) REFERENCES vouchers(id) ON DELETE CASCADE,
                FOREIGN KEY(ledger_id) REFERENCES ledgers(id) ON DELETE CASCADE
            )
        """)

        conn.execute("CREATE INDEX IF NOT EXISTS idx_ledgers_company ON ledgers(company_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_vouchers_company ON vouchers(company_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_vouchers_company_type ON vouchers(company_id, type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_entries_voucher ON voucher_entries(voucher_id)")


def list_companies():
    with get_conn() as conn:
        return conn.execute("SELECT * FROM companies ORDER BY id DESC").fetchall()


def get_company(company_id: Optional[int]):
    if not company_id:
        return None
    with get_conn() as conn:
        return conn.execute("SELECT * FROM companies WHERE id = ?", (company_id,)).fetchone()


def create_company(data: dict):
    with get_conn() as conn:
        cur = conn.execute("""
            INSERT INTO companies (
                name, mailing_name, address, state, country, phone, email,
                financial_year_start, books_from, currency, maintain_inventory, enable_gst
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data["name"],
            data.get("mailing_name"),
            data.get("address"),
            data.get("state"),
            data.get("country", "India"),
            data.get("phone"),
            data.get("email"),
            data["financial_year_start"],
            data["books_from"],
            data.get("currency", "₹"),
            data.get("maintain_inventory", "Yes"),
            data.get("enable_gst", "Yes"),
        ))
        return cur.lastrowid


def delete_company(company_id: int):
    with get_conn() as conn:
        conn.execute("DELETE FROM companies WHERE id = ?", (company_id,))


def list_ledgers(company_id: int):
    with get_conn() as conn:
        return conn.execute("""
            SELECT * FROM ledgers
            WHERE company_id = ?
            ORDER BY ledger_name ASC
        """, (company_id,)).fetchall()


def create_ledger(data: dict):
    with get_conn() as conn:
        conn.execute("""
            INSERT INTO ledgers (
                company_id, ledger_name, group_name, opening_balance, balance_type,
                gst_applicable, gst_number, address, phone, email
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data["company_id"],
            data["ledger_name"],
            data["group_name"],
            data.get("opening_balance", 0),
            data.get("balance_type", "Debit"),
            data.get("gst_applicable", "No"),
            data.get("gst_number"),
            data.get("address"),
            data.get("phone"),
            data.get("email"),
        ))


def delete_ledger(ledger_id: int):
    with get_conn() as conn:
        conn.execute("DELETE FROM ledgers WHERE id = ?", (ledger_id,))


def get_ledger_map(company_id: int):
    rows = list_ledgers(company_id)
    return {row["id"]: row for row in rows}


def list_vouchers(company_id: int):
    with get_conn() as conn:
        vouchers = conn.execute("""
            SELECT * FROM vouchers
            WHERE company_id = ?
            ORDER BY id DESC
        """, (company_id,)).fetchall()

        result = []
        for v in vouchers:
            entries = conn.execute("""
                SELECT ve.*, l.ledger_name, l.group_name
                FROM voucher_entries ve
                JOIN ledgers l ON l.id = ve.ledger_id
                WHERE ve.voucher_id = ?
                ORDER BY ve.id ASC
            """, (v["id"],)).fetchall()
            result.append({"voucher": v, "entries": entries})
        return result


def recent_vouchers(company_id: int, limit: int = 10):
    with get_conn() as conn:
        vouchers = conn.execute("""
            SELECT * FROM vouchers
            WHERE company_id = ?
            ORDER BY id DESC
            LIMIT ?
        """, (company_id, limit)).fetchall()

        result = []
        for v in vouchers:
            entries = conn.execute("""
                SELECT ve.*, l.ledger_name
                FROM voucher_entries ve
                JOIN ledgers l ON l.id = ve.ledger_id
                WHERE ve.voucher_id = ?
                ORDER BY ve.id ASC
            """, (v["id"],)).fetchall()
            result.append({"voucher": v, "entries": entries})
        return result


def create_voucher(company_id: int, data: dict, cleaned_entries: list, ai_result: dict):
    with get_conn() as conn:
        cur = conn.execute("""
            INSERT INTO vouchers (
                company_id, voucher_number, date, type, narration,
                ai_risk_level, ai_risk_score, ai_flags
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            company_id,
            data["voucher_number"],
            data["date"],
            data["type"],
            data.get("narration", ""),
            ai_result["risk_level"],
            ai_result["risk_score"],
            " | ".join(ai_result["flags"]),
        ))
        voucher_id = cur.lastrowid

        for e in cleaned_entries:
            conn.execute("""
                INSERT INTO voucher_entries (voucher_id, ledger_id, debit, credit)
                VALUES (?, ?, ?, ?)
            """, (voucher_id, e["ledger_id"], e["debit"], e["credit"]))

        return voucher_id


def delete_voucher(voucher_id: int):
    with get_conn() as conn:
        conn.execute("DELETE FROM vouchers WHERE id = ?", (voucher_id,))


def dashboard_summary(company_id: Optional[int]):
    with get_conn() as conn:
        company_count = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]

        if not company_id:
            return {
                "company_count": company_count,
                "ledger_count": 0,
                "voucher_count": 0,
                "debit_total": 0.0,
                "credit_total": 0.0,
            }

        ledger_count = conn.execute(
            "SELECT COUNT(*) FROM ledgers WHERE company_id = ?",
            (company_id,)
        ).fetchone()[0]

        voucher_count = conn.execute(
            "SELECT COUNT(*) FROM vouchers WHERE company_id = ?",
            (company_id,)
        ).fetchone()[0]

        debit_total = conn.execute("""
            SELECT COALESCE(SUM(ve.debit), 0)
            FROM voucher_entries ve
            JOIN vouchers v ON v.id = ve.voucher_id
            WHERE v.company_id = ?
        """, (company_id,)).fetchone()[0]

        credit_total = conn.execute("""
            SELECT COALESCE(SUM(ve.credit), 0)
            FROM voucher_entries ve
            JOIN vouchers v ON v.id = ve.voucher_id
            WHERE v.company_id = ?
        """, (company_id,)).fetchone()[0]

        return {
            "company_count": company_count,
            "ledger_count": ledger_count,
            "voucher_count": voucher_count,
            "debit_total": float(debit_total or 0),
            "credit_total": float(credit_total or 0),
        }


def existing_voucher_numbers(company_id: int, voucher_number: str):
    with get_conn() as conn:
        return conn.execute("""
            SELECT COUNT(*) FROM vouchers
            WHERE company_id = ? AND voucher_number = ?
        """, (company_id, voucher_number)).fetchone()[0]


def average_voucher_amount_by_type(company_id: int, voucher_type: str):
    with get_conn() as conn:
        row = conn.execute("""
            SELECT AVG(total_amount) AS avg_amount
            FROM (
                SELECT v.id, SUM(ve.debit) AS total_amount
                FROM vouchers v
                JOIN voucher_entries ve ON ve.voucher_id = v.id
                WHERE v.company_id = ? AND v.type = ?
                GROUP BY v.id
                ORDER BY v.id DESC
                LIMIT 20
            )
        """, (company_id, voucher_type)).fetchone()
        return float(row["avg_amount"] or 0.0)


def ai_dashboard_data(company_id: int):
    with get_conn() as conn:
        stats = conn.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN ai_risk_level = 'High' THEN 1 ELSE 0 END) AS high_count,
                SUM(CASE WHEN ai_risk_level = 'Medium' THEN 1 ELSE 0 END) AS medium_count,
                SUM(CASE WHEN ai_risk_level = 'Low' THEN 1 ELSE 0 END) AS low_count,
                COALESCE(AVG(ai_risk_score), 0) AS avg_score
            FROM vouchers
            WHERE company_id = ?
        """, (company_id,)).fetchone()

        risky = conn.execute("""
            SELECT *
            FROM vouchers
            WHERE company_id = ?
            ORDER BY ai_risk_score DESC, id DESC
            LIMIT 15
        """, (company_id,)).fetchall()

        return {
            "stats": stats,
            "risky_vouchers": risky,
        }
