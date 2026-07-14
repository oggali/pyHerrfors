"""DuckDB persistence helpers for pyHerrfors."""
import logging
import os
from contextlib import contextmanager

import duckdb as dd

from .const import DEFAULT_DB_FILE

logger = logging.getLogger(__name__)

DB_FILE = os.getenv("DB_FILE", DEFAULT_DB_FILE)

ALLOWED_TABLES = {
    "price_calculations_all",
    "day_group_calculations",
    "month_group_calculations",
    "year_group_calculations",
}


@contextmanager
def connect_db():
    con = dd.connect(DB_FILE)
    try:
        yield con
    finally:
        con.close()


def db_empty():
    if not os.path.exists(DB_FILE):
        return True
    with connect_db() as con:
        db_tables = con.sql("SELECT * FROM duckdb_tables()")
        return len(db_tables["table_name"].fetchall()) == 0


_db_empty = db_empty


def table_not_exists(table_name, con=None):
    if not os.path.exists(DB_FILE):
        return True

    def _check(connection):
        db_tables = connection.sql(
            f"SELECT * FROM information_schema.tables where table_name='{table_name}'"
        )
        return len(db_tables) == 0

    if con is not None:
        return _check(con)

    with connect_db() as connection:
        return _check(connection)


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
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Unknown table name: {table_name}")

    with connect_db() as con:
        if table_not_exists(table_name, con=con):
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


def get_all_from_db_as_df(table_name=None):
    if table_name is None:
        table_name = "price_calculations_all"
    if table_name not in ALLOWED_TABLES:
        raise ValueError(f"Unknown table name: {table_name}")

    with connect_db() as con:
        return con.sql(f"SELECT * FROM {table_name}").to_df()
