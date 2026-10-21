import pytds


class MssqlUtil:
    connector = "mssql"

    def __init__(self, config, table=None):
        self.connection = pytds.connect(
            server=config["host"],
            port=int(config.get("port", 1433)),
            database=config["database"],
            user=config.get("username") or config.get("user"),
            password=config["password"],
            timeout=int(config.get("timeout", 30)),
        )
        self.table = table

    def execute(self, sql, params=None):
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params or ())
            self.connection.commit()
            return cursor.rowcount
        finally:
            cursor.close()

    def query_sql(self, sql, params=None):
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params or ())
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def close(self):
        self.connection.close()


class SqlServerUtil(MssqlUtil):
    # Some Tapdata connector catalogs use `sqlserver` as the connector key.
    connector = "sqlserver"
