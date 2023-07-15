package io.tapdata.sybase.extend;

import io.tapdata.connector.mysql.MysqlJdbcContextV2;
import io.tapdata.connector.mysql.MysqlReader;

/**
 * @author GavinXiao
 * @description SybaseReader create by Gavin
 * @create 2023/7/11 19:08
 **/
public class SybaseReader extends MysqlReader {
    public SybaseReader(MysqlJdbcContextV2 mysqlJdbcContext) {
        super(mysqlJdbcContext);
    }
}
