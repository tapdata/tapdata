package io.tapdata.connector.gbase8s;

import io.tapdata.common.JdbcContext;
import io.tapdata.connector.gbase8s.config.Gbase8sConfig;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.util.List;

public class Gbase8sJdbcContext extends JdbcContext {

    private final static String TAG = Gbase8sJdbcContext.class.getSimpleName();

    public Gbase8sJdbcContext(Gbase8sConfig config) {
        super(config);
    }

    @Override
    public List<DataMap> queryAllTables(List<String> tableNames) {
        TapLogger.debug(TAG, "Query some tables, schema: " + getConfig().getSchema());
        List<DataMap> tableList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND T.TABNAME IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(GBASE_8S_ALL_TABLE, getConfig().getSchema(), tableSql),
                    resultSet -> tableList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllTables failed, error: " + e.getMessage(), e);
        }
        return tableList;
    }

    @Override
    public List<DataMap> queryAllColumns(List<String> tableNames) {
        TapLogger.debug(TAG, "Query columns of some tables, schema: " + getConfig().getSchema());
        List<DataMap> columnList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND T.TABNAME IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(GBASE_8S_ALL_COLUMN, getConfig().getSchema(), tableSql),
                    resultSet -> columnList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllColumns failed, error: " + e.getMessage(), e);
        }
        return columnList;
    }

    @Override
    public List<DataMap> queryAllIndexes(List<String> tableNames) {
        TapLogger.debug(TAG, "Query indexes of some tables, schema: " + getConfig().getSchema());
        List<DataMap> indexList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND T.TABNAME IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(GBASE_8S_ALL_INDEX, getConfig().getSchema(), tableSql),
                    resultSet -> indexList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllIndexes failed, error: " + e.getMessage(), e);
        }
        return indexList;
    }

    public boolean isTableNameCaseSensitive() {
        String extParams = getConfig().getExtParams();
        if (EmptyKit.isBlank(extParams)) {
            return false;
        }
        return extParams.toLowerCase().contains("delimident=y");
    }

    private static final String GBASE_8S_ALL_TABLE = "SELECT T.TABNAME,\n" +
            "(SELECT COMMENTS FROM SYSCOMMS WHERE TABID=T.TABID) COMMENTS\n" +
            "FROM SYSTABLES T WHERE T.OWNER='%s' %s ORDER BY TABNAME";
    private static final String GBASE_8S_ALL_COLUMN = "SELECT T.TABNAME,C.COLNAME,C.COLTYPENAME,C.COLTYPENAME2,C.COLLENGTH,\n" +
            "(CASE WHEN C.COLTYPE>256 THEN 'NO' ELSE 'YES' END) IS_NULLABLE,\n" +
            "(SELECT COMMENTS FROM SYSCOLCOMMS WHERE TABID=C.TABID AND COLNO=C.COLNO) COMMENTS\n" +
            "FROM SYSCOLUMNSEXT C, SYSTABLES T WHERE T.OWNER='%s' %s AND C.TABID=T.TABID\n" +
            "ORDER BY T.TABNAME,C.COLNO";
    private static final String GBASE_8S_ALL_INDEX = "SELECT T.TABNAME,C.COLNAME,C.COLNO,I.IDXNAME,\n" +
            "(CASE WHEN I.IDXTYPE='U' THEN 'YES' ELSE 'NO' END) ISUNIQUE,\n" +
            "(CASE WHEN P.CONSTRTYPE='P' THEN 'YES' ELSE 'NO' END) ISPRIMARY\n" +
            "FROM SYSTABLES T\n" +
            "INNER JOIN SYSCOLUMNS C ON (C.TABID=T.TABID)\n" +
            "INNER JOIN SYSINDEXES I ON (I.TABID=T.TABID)\n" +
            "LEFT JOIN SYSCONSTRAINTS P ON (P.TABID=T.TABID AND P.IDXNAME=I.IDXNAME)\n" +
            "WHERE T.OWNER='%s' %s\n" +
            "AND C.COLNO IN (I.PART1,I.PART2,I.PART3,I.PART4,I.PART5,I.PART6,I.PART7,I.PART8,\n" +
            "I.PART9,I.PART10,I.PART11,I.PART12,I.PART13,I.PART14,I.PART15,I.PART16)\n" +
            "ORDER BY T.TABNAME,C.COLNO";
}
