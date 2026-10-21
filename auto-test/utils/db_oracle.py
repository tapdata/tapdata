import oracledb


class OracleUtil:
    connector = "oracle"

    def __init__(self, config, table=None):
        dsn = config.get("dsn") or config.get("uri")
        if not dsn:
            host = config["host"]
            port = int(config.get("port", 1521))
            sid = config.get("sid")
            if sid:
                dsn = oracledb.makedsn(host, port, sid=sid)
            else:
                service_name = (config.get("service_name") or config.get("serviceName")
                                or config.get("database"))
                if not service_name:
                    raise ValueError("Oracle test config needs dsn, sid, or service_name/database")
                dsn = oracledb.makedsn(host, port, service_name=service_name)

        self.connection = oracledb.connect(
            user=config.get("username") or config.get("user"),
            password=config["password"],
            dsn=dsn,
        )
        self.table = table

    def execute(self, sql, params=None):
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params or {})
            self.connection.commit()
            return cursor.rowcount
        finally:
            cursor.close()

    def query_sql(self, sql, params=None):
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params or {})
            columns = [column[0] for column in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def close(self):
        self.connection.close()
