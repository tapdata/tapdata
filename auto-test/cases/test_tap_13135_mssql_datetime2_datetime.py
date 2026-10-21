# desc: TAP-13135 MSSQL datetime2(7) 同步至手动创建的 datetime 列
from datetime import datetime

from tap11941 import (cleanup, configured_name, open_database, pipeline_tables,
                      refresh_datasource_schema, table_name)


_state = None


def test(Pipeline, p):
    global _state
    source_key = configured_name("TAP11941_MSSQL_SOURCE")
    sink_key = configured_name("TAP11941_MSSQL_SINK")
    source_table = table_name("tap11941_mssql_datetime2")
    sink_table = table_name("tap11941_mssql_datetime")
    source_db = None
    sink_db = None

    try:
        source_db = open_database(source_key, source_table)
        sink_db = open_database(sink_key, sink_table)
        source_db.execute(
            "IF OBJECT_ID(N'dbo.{}', N'U') IS NOT NULL DROP TABLE dbo.{}".format(
                source_table, source_table
            )
        )
        source_db.execute(
            "CREATE TABLE dbo.{} (EVENT_ID INT NOT NULL PRIMARY KEY, "
            "EVENT_TIME datetime2(7) NOT NULL)".format(source_table)
        )
        source_db.execute(
            "INSERT INTO dbo.{} (EVENT_ID, EVENT_TIME) VALUES (%s, %s)".format(source_table),
            (1, "2026-10-21T12:13:14.1234567"),
        )

        sink_db.execute(
            "IF OBJECT_ID(N'dbo.{}', N'U') IS NOT NULL DROP TABLE dbo.{}".format(
                sink_table, sink_table
            )
        )
        sink_db.execute(
            "CREATE TABLE dbo.{} (EVENT_ID INT NOT NULL PRIMARY KEY, "
            "EVENT_TIME datetime NOT NULL)".format(sink_table)
        )
        refresh_datasource_schema(source_key)
        refresh_datasource_schema(sink_key)
        pipeline_tables(p, source_key, source_table, sink_key, sink_table)
        _state = (source_db, sink_db, source_table, sink_table)
    except Exception:
        cleanup(
            (source_db, "IF OBJECT_ID(N'dbo.{}', N'U') IS NOT NULL DROP TABLE dbo.{}".format(source_table, source_table)),
            (sink_db, "IF OBJECT_ID(N'dbo.{}', N'U') IS NOT NULL DROP TABLE dbo.{}".format(sink_table, sink_table)),
        )
        raise


def check(Pipeline, p):
    if _state is None:
        return False
    source_db, sink_db, source_table, sink_table = _state
    try:
        rows = sink_db.query_sql(
            "SELECT EVENT_ID, EVENT_TIME FROM dbo.{} WHERE EVENT_ID = 1".format(sink_table)
        )
        if len(rows) != 1:
            return False
        value = rows[0]["EVENT_TIME"]
        if value.date() != datetime(2026, 10, 21).date():
            return False
        if (value.hour, value.minute, value.second) != (12, 13, 14):
            return False
        # SQL Server datetime stores time in 1/300-second increments; .1234567
        # is represented as .1233333 and is returned with 123 milliseconds.
        if value.microsecond // 1000 != 123:
            return False

        column = sink_db.query_sql(
            "SELECT DATA_TYPE, DATETIME_PRECISION FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = %s AND COLUMN_NAME = 'EVENT_TIME'",
            (sink_table,),
        )
        return len(column) == 1 and column[0]["DATA_TYPE"] == "datetime" and column[0]["DATETIME_PRECISION"] == 3
    finally:
        cleanup(
            (source_db, "IF OBJECT_ID(N'dbo.{}', N'U') IS NOT NULL DROP TABLE dbo.{}".format(source_table, source_table)),
            (sink_db, "IF OBJECT_ID(N'dbo.{}', N'U') IS NOT NULL DROP TABLE dbo.{}".format(sink_table, sink_table)),
        )
