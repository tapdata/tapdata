package io.tapdata.connector.dameng;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.common.JdbcContext;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * @author lemon
 */
public class DamengContext extends JdbcContext {
    private final static String TAG = DamengContext.class.getSimpleName();


    private static final String VERSION_V8 = "V8";

    private static final String VERSION_V7 = "V7";


    private final static String DAMENG_ALL_TABLE = "SELECT T.TABLE_NAME, C.COMMENTS\n" +
            "FROM ALL_TABLES T\n" +
            "         INNER JOIN\n" +
            "     ALL_TAB_COMMENTS C ON C.TABLE_NAME = T.TABLE_NAME AND C.OWNER='%s'\n" +
            "WHERE T.OWNER='%s' %s ORDER BY T.TABLE_NAME";

    private final static String DAMENG_ALL_COLUMN = "SELECT COL.*, COM.COMMENTS\n" +
            "FROM ALL_TAB_COLUMNS COL\n" +
            "INNER JOIN ALL_COL_COMMENTS COM\n" +
            "    ON COL.TABLE_NAME=COM.TABLE_NAME\n" +
            "    AND COL.COLUMN_NAME=COM.COLUMN_NAME\n" +
            "    AND COM.OWNER='%s'\n" +
            "WHERE COL.OWNER='%s'\n" +
            "%s ORDER BY COL.TABLE_NAME, COL.COLUMN_ID";

    private final static String DAMENG_ALL_INDEX = "SELECT I.TABLE_NAME, I.INDEX_NAME, C.COLUMN_NAME, I.UNIQUENESS, C.DESCEND,\n" +
            "       (CASE WHEN EXISTS (SELECT * FROM ALL_CONSTRAINTS WHERE OWNER='%s' AND INDEX_NAME=I.INDEX_NAME AND CONSTRAINT_TYPE='P') THEN 1 ELSE 0 END) IS_PK\n" +
            "FROM ALL_INDEXES I\n" +
            "         INNER JOIN ALL_IND_COLUMNS C ON I.INDEX_NAME = C.INDEX_NAME AND C.TABLE_OWNER='%s'\n" +
            "WHERE I.TABLE_OWNER='%s' %s\n" +
            "ORDER BY I.TABLE_NAME, I.INDEX_NAME, C.COLUMN_POSITION";

    private final static String DAMENG_SYS_TIMEZONE = "SELECT TO_CHAR(SYSTIMESTAMP ,'TZH:TZM') FROM DUAL";


    public DamengContext(DamengConfig config, HikariDataSource hikariDataSource) {

        super(config, hikariDataSource);
    }

    @Override
    public List<DataMap> queryAllTables(List<String> tableNames) {
        String schema = getConfig().getSchema().toUpperCase();
        TapLogger.debug(TAG, "Query some tables, schema: " + schema);
        List<DataMap> tableList = list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND T.TABLE_NAME IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(DAMENG_ALL_TABLE, schema, schema, tableSql),
                    resultSet -> tableList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllTables failed, error: " + e.getMessage(), e);
        }
        return tableList;
    }

    @Override
    public void queryAllTables(List<String> tableNames, int batchSize, Consumer<List<String>> consumer) {
        String schema = getConfig().getSchema().toUpperCase();
        TapLogger.debug(TAG, "Query some tables, schema: " + schema);
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND T.TABLE_NAME IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        List<String> temp = list();
        try {
            query(String.format(DAMENG_ALL_TABLE, schema, schema, tableSql),
                    resultSet -> {
                        while (resultSet.next()) {
                            String tableName = resultSet.getString("TABLE_NAME");
                            if (null != tableName && !"".equals(tableName.trim())) {
                                temp.add(tableName);
                            }
                            if (temp.size() >= batchSize) {
                                consumer.accept(temp);
                                temp.clear();
                            }
                        }
                    });
            if (!temp.isEmpty()) {
                consumer.accept(temp);
                temp.clear();
            }
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllTables failed, error: " + e.getMessage(), e);
        }
    }

    @Override
    public List<DataMap> queryAllColumns(List<String> tableNames) {
        String schema = getConfig().getSchema().toUpperCase();
        TapLogger.debug(TAG, "Query columns of some tables, schema: " + schema);
        List<DataMap> columnList = list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND COL.TABLE_NAME IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(DAMENG_ALL_COLUMN, schema, schema, tableSql),
                    resultSet -> columnList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllColumns failed, error: " + e.getMessage(), e);
        }
        return columnList;
    }

    @Override
    public List<DataMap> queryAllIndexes(List<String> tableNames) {
        String schema = getConfig().getSchema().toUpperCase();
        TapLogger.debug(TAG, "Query indexes of some tables, schema: " + schema);
        List<DataMap> indexList = list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND I.TABLE_NAME IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(DAMENG_ALL_INDEX, schema, schema, schema, tableSql),
                    resultSet -> indexList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllIndexes failed, error: " + e.getMessage(), e);
        }
        return indexList;
    }


    @Override
    public String queryVersion() {
        AtomicReference<String> version = new AtomicReference<>("");
        try {
            queryWithNext("SELECT * FROM V$VERSION", resultSet -> version.set(resultSet.getString(1)));
        } catch (Throwable e) {
            e.printStackTrace();
        }
        if (version.get().contains(VERSION_V7)) {
            return "V7";
        } else if (version.get().contains(VERSION_V8)) {
            return "V8";
        }
        return "V6";
    }

    /**
     * 查询数据库时区
     */
    public String querySysTimezone() {
        AtomicReference<String> sysTimezone = new AtomicReference<>("");
        try {
            queryWithNext(DAMENG_SYS_TIMEZONE, rs -> sysTimezone.set(rs.getString(1)));
        } catch (Throwable e) {
           throw new RuntimeException("query sysTimezone fail",e);
        }
        if (StringUtils.isNotBlank(sysTimezone.get())) {
            return sysTimezone.get();
        }

        return null;
    }

}