import pymssql
from log import logger as logger2
logger = logger2


# Mssql DB Utils, to make curd to mssql
class MssqlUtil:
    connector = "sql server"

    def __init__(self, config, table=None):
        user = config["user"]
        password = config["password"]
        host = config["host"]
        port = config["port"]
        database = config["database"]
        schema = config["schema"]
        self.mssql_client = pymssql.connect(user=user, password=password, host=host, port=port,
                                            database=database, autocommit=True)
        self.db = database
        self.schema = schema
        self.table = table

    def __destroy__(self):
        try:
            self.mssql_client.close()
        except Exception as e:
            pass

    @property
    def _cursor(self):
        return self.mssql_client.cursor(as_dict=True)

    def open_cdc(self):
        cdc_sql = f"""
    EXEC sys.sp_cdc_enable_table
         @source_schema = N'{self.schema}',
         @source_name = N'{self.table}',
         @role_name = NULL
    
        """
        cur = self._cursor
        try:
            cur.execute(cdc_sql)
        except pymssql.OperationalError as e:
            logger.warn("【{}.{}】: {}", self.db, self.table, e)
        except Exception:
            logger.error("【{}.{}】: cdc Startup failure", self.db, self.table)
        else:
            logger.info("【{}.{}】: cdc Successful startup", self.db, self.table)

    def close_cdc(self):
        cdc_sql = f"""
        EXEC sys.sp_cdc_disable_table
             @source_schema = N'{self.schema}',
             @source_name = N'{self.table}',
             @capture_instance = N'{self.schema}_{self.table}'
        """
        cur = self._cursor
        try:
            cur.execute(cdc_sql)
        except pymssql.OperationalError as e:
            logger.warn("【{}.{}】: {}", self.db, self.table, e)
        except Exception:
            logger.error("【{}.{}】: cdc Shutdown failure", self.db, self.table)
        else:
            logger.info("【{}.{}】: cdc Closed successfully", self.db, self.table)


if __name__ == '__main__':
    config1 = {
        "connector": "sql server",
        "host": "139.198.127.226",
        "port": 31930,
        "schema": "dbo",
        "user": "sa",
        "password": "Gotapd8#",
        "database": "TAPDATA",
        "__core": True,
        "__type": ["source", "sink", "cdc"]
    }
    m = MssqlUtil(table='car_claim_1679285465349_5157', config=config1)
    m.close_cdc()
