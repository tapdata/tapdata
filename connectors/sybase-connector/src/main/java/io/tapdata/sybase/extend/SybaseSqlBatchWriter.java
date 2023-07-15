package io.tapdata.sybase.extend;

import io.tapdata.connector.mysql.MysqlJdbcContextV2;
import io.tapdata.connector.mysql.writer.MysqlSqlBatchWriter;

/**
 * @author GavinXiao
 * @description SybaseSqlBatchWriter create by Gavin
 * @create 2023/7/11 19:07
 **/
public class SybaseSqlBatchWriter extends MysqlSqlBatchWriter {
    public SybaseSqlBatchWriter(MysqlJdbcContextV2 mysqlJdbcContext) throws Throwable {
        super(mysqlJdbcContext);
    }
}
