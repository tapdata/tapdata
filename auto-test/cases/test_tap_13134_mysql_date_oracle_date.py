# desc: TAP-13134 MySQL DATE 同步至手动创建的 Oracle DATE 列
from datetime import date, datetime

from tap11941 import (cleanup, configured_name, drop_oracle_table, open_database,
                      pipeline_tables, refresh_datasource_schema, table_name)


_state = None


def test(Pipeline, p):
    global _state
    source_key = configured_name("TAP11941_MYSQL_SOURCE")
    sink_key = configured_name("TAP11941_ORACLE_SINK")
    source_table = table_name("tap11941_mysql_date")
    sink_table = table_name("tap11941_oracle_date")
    source_db = None
    sink_db = None

    try:
        source_db = open_database(source_key, source_table)
        sink_db = open_database(sink_key, sink_table)
        source_db.execute("DROP TABLE IF EXISTS {}".format(source_table))
        source_db.execute(
            "CREATE TABLE {} (ID INT NOT NULL PRIMARY KEY, BIRTH_DATE DATE NOT NULL)".format(source_table)
        )
        source_db.execute(
            "INSERT INTO {} (ID, BIRTH_DATE) VALUES (%s, %s)".format(source_table),
            (1, date(2026, 10, 21)),
        )

        drop_oracle_table(sink_db, sink_table)
        sink_db.execute(
            "CREATE TABLE {} (ID NUMBER(10) NOT NULL PRIMARY KEY, BIRTH_DATE DATE NOT NULL)".format(sink_table)
        )
        refresh_datasource_schema(source_key)
        refresh_datasource_schema(sink_key)
        pipeline_tables(p, source_key, source_table, sink_key, sink_table)
        _state = (source_db, sink_db, source_table, sink_table)
    except Exception:
        cleanup(
            (source_db, "DROP TABLE IF EXISTS {}".format(source_table)),
            (sink_db, "DROP TABLE {} PURGE".format(sink_table)),
        )
        raise


def check(Pipeline, p):
    if _state is None:
        return False
    source_db, sink_db, source_table, sink_table = _state
    try:
        rows = sink_db.query_sql(
            "SELECT ID, BIRTH_DATE FROM {} WHERE ID = 1".format(sink_table)
        )
        if len(rows) != 1:
            return False
        value = rows[0]["BIRTH_DATE"]
        if isinstance(value, datetime):
            value = value.date()
        if value != date(2026, 10, 21):
            return False

        column = sink_db.query_sql(
            "SELECT DATA_TYPE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = :table_name "
            "AND COLUMN_NAME = 'BIRTH_DATE'",
            {"table_name": sink_table.upper()},
        )
        return len(column) == 1 and column[0]["DATA_TYPE"] == "DATE"
    finally:
        cleanup(
            (source_db, "DROP TABLE IF EXISTS {}".format(source_table)),
            (sink_db, "DROP TABLE {} PURGE".format(sink_table)),
        )
