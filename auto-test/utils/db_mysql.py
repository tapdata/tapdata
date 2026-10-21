import mysql.connector


# Mysql DB Utils, to make curd to mysql
class MysqlUtil:
    connector = "mysql"

    def __init__(self, config, table=None):
        user = config["username"]
        password = config["password"]
        host = config["host"]
        port = config["port"]
        database = config["database"]
        self.mysql_client = mysql.connector.connect(user=user, password=password, host=host, port=port,
                                                    database=database)
        self.db = database
        self.table = table

    def __destroy__(self):
        try:
            self.mysql_client.close()
        except Exception as e:
            pass

    def query(self, condition={}, limit=1, table=None):
        if table is None:
            table = self.table
        cursor = self.mysql_client.cursor()
        sql = "DESCRIBE %s.%s" % (self.db, table)
        cursor.execute(sql)
        schema = cursor.fetchall()

        sql = "SELECT * FROM `%s`" % (table)
        if len(condition) > 0:
            sql += " where "
            for k, v in condition.items():
                sql += "%s = %s AND" % (k, v)
            sql = sql[0:-4]
        sql = sql + " LIMIT %d" % limit
        cursor = self.mysql_client.cursor()
        cursor.execute(sql)
        datas = list(cursor.fetchall())
        cursor.close()
        results = []
        for i in datas:
            result = {}
            for j in range(len(schema)):
                result[schema[j][0]] = i[j]
            results.append(result)

        return results
