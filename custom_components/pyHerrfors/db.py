import logging
import os

import duckdb as dd

from .const import DEFAULT_DB_FILE

logger = logging.getLogger(__name__)

DB_FILE = os.getenv("DB_FILE", DEFAULT_DB_FILE)


def db_empty():
    if os.path.exists(DB_FILE):
        con = dd.connect(DB_FILE)
        db_tables = con.sql("SELECT * FROM duckdb_tables()")
        if len(db_tables["table_name"].fetchall()) == 0:
            con.close()
            return True
        con.close()
        return False

    if not os.path.exists(DB_FILE):
        return True

    return False


def table_not_exists(table_name):
    if os.path.exists(DB_FILE):
        con = dd.connect(DB_FILE)
        db_tables = con.sql(
            f"SELECT * FROM information_schema.tables where table_name='{table_name}'"
        )

        if len(db_tables) == 0:
            con.close()
            return True
        con.close()
        return False

    return True


def get_table_columns(con, table_name):
    rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = ?
        ORDER BY ordinal_position
    """,
        [table_name],
    ).fetchall()
    return [r[0] for r in rows]


def insert_to_db(df_to_insert, table_name, del_key_column=None):
    con = dd.connect(DB_FILE)

    if table_not_exists(table_name):
        con.sql(
            f"""
                CREATE TABLE {table_name} as
                SELECT * FROM df_to_insert
                """
        )

    if del_key_column is not None:
        con.sql(
            f"""
        DELETE FROM {table_name}
        where {del_key_column} in ( select {del_key_column} from df_to_insert group by {del_key_column})
        """
        )
    logger.info("insert data to table  %s", table_name)

    cols = get_table_columns(con, table_name)
    common = [c for c in cols if c in df_to_insert.columns]
    col_list = ", ".join(common)

    con.sql(
        f"""
            INSERT INTO {table_name}
            SELECT
            {col_list}
            FROM df_to_insert
    """
    )

    con.execute("CHECKPOINT")
    con.close()


def get_all_from_db_as_df(table_name=None):
    con = dd.connect(DB_FILE)
    try:
        if table_name is None:
            table_name = "price_calculations_all"
        return con.sql(f"SELECT * FROM {table_name}").to_df()
    finally:
        con.close()
