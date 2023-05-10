package io.tapdata.common.ddl.type;

import io.tapdata.common.ddl.DDLFilter;
import io.tapdata.common.ddl.parser.CCJSqlParser;
import io.tapdata.common.ddl.parser.DDLParser;
import io.tapdata.kit.EmptyKit;

/**
 * @author samuel
 * @Description
 * @create 2022-07-01 14:34
 **/
public enum DDLParserType {
    MYSQL_CCJ_SQL_PARSER("com.github.jsqlparser", CCJSqlParser.class, "io.tapdata.connector.mysql.ddl.ccj.MysqlWrapperType", "io.tapdata.connector.mysql.ddl.ccj.MysqlDDLFilter"),
    DB2_CCJ_SQL_PARSER("com.github.jsqlparser", CCJSqlParser.class, "io.tapdata.connector.db2.ddl.ccj.Db2WrapperType", ""),
    ORACLE_CCJ_SQL_PARSER("com.github.jsqlparser", CCJSqlParser.class, "io.tapdata.connector.oracle.ddl.ccj.OracleWrapperType", ""),
    MSSQL_CCJ_SQL_PARSER("com.github.jsqlparser", CCJSqlParser.class, "io.tapdata.connector.mssql.ddl.ccj.MssqlWrapperType", ""),
    DAMENG_CCJ_SQL_PARSER("com.github.jsqlparser", CCJSqlParser.class, "io.tapdata.connector.dameng.ddl.ccj.DamengWrapperType", ""),
    TIDB_CCJ_SQL_PARSER("com.github.jsqlparser", CCJSqlParser.class, "io.tapdata.connector.tidb.ddl.ccj.TidblWrapperType", "");


    private final String desc;
    private final Class<? extends DDLParser<?>> parserClz;
    private final String wrapperTypeClassName;
    private final String filterClassName;

    DDLParserType(String desc, Class<? extends DDLParser<?>> parserClz, String wrapperTypeClassName, String filterClassName) {
        this.desc = desc;
        this.parserClz = parserClz;
        this.wrapperTypeClassName = wrapperTypeClassName;
        this.filterClassName = filterClassName;
    }

    public String getDesc() {
        return desc;
    }

    public Class<? extends DDLParser<?>> getParserClz() {
        return parserClz;
    }

    public String getWrapperTypeClassName() {
        return wrapperTypeClassName;
    }

    public Class<? extends WrapperType> getWrapperTypeClass() {
        try {
            return (Class<? extends WrapperType>) Class.forName(wrapperTypeClassName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public String getFilterClassName() {
        return filterClassName;
    }

    public Class<? extends DDLFilter> getFilterClass() {
        try {
            if (EmptyKit.isBlank(filterClassName)) {
                return DDLFilter.class;
            }
            return (Class<? extends DDLFilter>) Class.forName(filterClassName);
        } catch (ClassNotFoundException e) {
            return DDLFilter.class;
        }
    }
}
