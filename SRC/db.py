import os
import json
import psycopg2
from psycopg2.extras import Json, register_default_jsonb

register_default_jsonb()

DATABASE_URL = os.environ.get("DATABASE_URL")


def _get_conn():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not configured")
    # Ensure SSL for services like Railway when not specified in the URL
    if "sslmode=" in (DATABASE_URL or ""):
        return psycopg2.connect(DATABASE_URL)
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def ensure_table():
    if not DATABASE_URL:
        return
    create_sql = (
        "CREATE TABLE IF NOT EXISTS guild_configs ("
        "guild_id TEXT PRIMARY KEY,"
        "name TEXT,"
        "data JSONB NOT NULL,"
        "updated_at TIMESTAMPTZ"
        ")"
    )
    alter_name = "ALTER TABLE guild_configs ADD COLUMN IF NOT EXISTS name TEXT"
    alter_updated = "ALTER TABLE guild_configs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ"

    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(create_sql)
                except Exception:
                    # Best-effort: try to add missing columns if table already exists
                    pass
                try:
                    cur.execute(alter_name)
                    cur.execute(alter_updated)
                except Exception:
                    # Ignore - we'll still attempt other operations and let callers handle errors
                    pass
    finally:
        conn.close()


def load_all_configs() -> dict:
    """Return mapping guild_id -> config (dict) from DB."""
    if not DATABASE_URL:
        return {}
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT guild_id, data FROM guild_configs")
            rows = cur.fetchall()
            result = {}
            for gid, data in rows:
                # data may be returned as a dict (JSONB) or as a string (text column)
                if isinstance(data, str):
                    try:
                        data = json.loads(data)
                    except Exception:
                        # leave as string if it isn't valid JSON
                        pass
                result[gid] = data
            return result
    finally:
        conn.close()


def load_guild_config(guild_id: str):
    if not DATABASE_URL:
        return None
    conn = _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT data FROM guild_configs WHERE guild_id = %s", (str(guild_id),))
            row = cur.fetchone()
            if not row:
                return None
            data = row[0]
            if isinstance(data, str):
                try:
                    return json.loads(data)
                except Exception:
                    return data
            return data
    finally:
        conn.close()


def save_guild_config(guild_id: str, cfg: dict):
    if not DATABASE_URL:
        return
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                # Try upsert including name and updated_at (works if those columns exist)
                try:
                    cur.execute(
                        "INSERT INTO guild_configs (guild_id, name, data, updated_at) VALUES (%s, %s, %s, now()) "
                        "ON CONFLICT (guild_id) DO UPDATE SET data = EXCLUDED.data, name = EXCLUDED.name, updated_at = now()",
                        (str(guild_id), cfg.get("name"), Json(cfg)),
                    )
                    return
                except Exception:
                    pass

                try:
                    cur.execute(
                        "INSERT INTO guild_configs (guild_id, data) VALUES (%s, %s) "
                        "ON CONFLICT (guild_id) DO UPDATE SET data = EXCLUDED.data",
                        (str(guild_id), Json(cfg)),
                    )
                except Exception:
                    # Fallback: store as JSON string if column is plain text
                    cur.execute(
                        "INSERT INTO guild_configs (guild_id, data) VALUES (%s, %s) "
                        "ON CONFLICT (guild_id) DO UPDATE SET data = EXCLUDED.data",
                        (str(guild_id), json.dumps(cfg)),
                    )
    finally:
        conn.close()


def save_config(config: dict):
    if not DATABASE_URL:
        return
    conn = _get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                for guild_id, cfg in config.items():
                    try:
                        cur.execute(
                            "INSERT INTO guild_configs (guild_id, name, data, updated_at) VALUES (%s, %s, %s, now()) "
                            "ON CONFLICT (guild_id) DO UPDATE SET data = EXCLUDED.data, name = EXCLUDED.name, updated_at = now()",
                            (str(guild_id), cfg.get("name"), Json(cfg)),
                        )
                        continue
                    except Exception:
                        pass

                    try:
                        cur.execute(
                            "INSERT INTO guild_configs (guild_id, data) VALUES (%s, %s) "
                            "ON CONFLICT (guild_id) DO UPDATE SET data = EXCLUDED.data",
                            (str(guild_id), Json(cfg)),
                        )
                    except Exception:
                        cur.execute(
                            "INSERT INTO guild_configs (guild_id, data) VALUES (%s, %s) "
                            "ON CONFLICT (guild_id) DO UPDATE SET data = EXCLUDED.data",
                            (str(guild_id), json.dumps(cfg)),
                        )
    finally:
        conn.close()


# Ensure the table exists when module is imported (if DATABASE_URL present)
ensure_table()
