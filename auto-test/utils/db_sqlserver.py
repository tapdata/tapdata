import pymssql


# Mssql DB Utils, to make curd to mssql
class MssqlUtil:
    connector = "sql server"

    def __init__(self, config, table=None):
        user = config["username"]
        password = config["password"]
        host = config["host"]
        port = config["port"]
        database = config["database"]
        schema = config["schema"]
        self.mssql_client = pymssql.connect(user=user, password=password, host=host, port=port,
                                            database=database, autocommit=True)
        self.db = database
        self.table = table

    def __destroy__(self):
        try:
            self.mssql_client.close()
        except Exception as e:
            pass

    def cursor(self):
        return self.mssql_client.cursor(as_dict=True)

    def open_cdc(self):
        cdc_sql = f"""
    USE TAPDATA
    GO
    EXEC sys.sp_cdc_enable_table
         @source_schema = N'dbo',
         @source_name = N'{self.table}',
         @role_name = NULL
    GO
        """
        self.cursor().execute(cdc_sql)


if __name__ == '__main__':
    pass
