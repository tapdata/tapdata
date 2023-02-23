package io.tapdata.connector.gauss;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.common.JdbcContext;
import io.tapdata.connector.gauss.config.GaussConfig;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class GaussJdbcContext extends JdbcContext {

    private final static String TAG = GaussJdbcContext.class.getSimpleName();

    public GaussJdbcContext(GaussConfig config, HikariDataSource hikariDataSource) {
        super(config, hikariDataSource);
    }

    /**
     * query version of database
     *
     * @return version description
     */
    @Override
    public String queryVersion() {
        AtomicReference<String> version = new AtomicReference<>("");
        try {
            queryWithNext("SHOW server_version_num", resultSet -> version.set(resultSet.getString(1)));
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
        return version.get();
    }

    @Override
    public List<DataMap> queryAllTables(List<String> tableNames) {
        TapLogger.debug(TAG, "Query some tables, schema: " + getConfig().getSchema());
        List<DataMap> tableList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(PG_ALL_TABLE, getConfig().getDatabase(), getConfig().getSchema(), tableSql),
                    resultSet -> tableList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllTables failed, error: " + e.getMessage(), e);
        }
        return tableList;
    }

    @Override
    public void queryAllTables(List<String> tableNames, int batchSize, Consumer<List<String>> consumer) {
        TapLogger.debug(TAG, "Query some tables, schema: " + getConfig().getSchema());
        List<String> tableList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(PG_ALL_TABLE, getConfig().getDatabase(), getConfig().getSchema(), tableSql),
                    resultSet -> {
                        while (resultSet.next()) {
                            String tableName = resultSet.getString("table_name");
                            if (StringUtils.isNotBlank(tableName)) {
                                tableList.add(tableName);
                            }
                            if (tableList.size() >= batchSize) {
                                consumer.accept(tableList);
                                tableList.clear();
                            }
                        }
                    });
            if (!tableList.isEmpty()) {
                consumer.accept(tableList);
                tableList.clear();
            }
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllTables failed, error: " + e.getMessage(), e);
        }
    }

    @Override
    public List<DataMap> queryAllColumns(List<String> tableNames) {
        TapLogger.debug(TAG, "Query columns of some tables, schema: " + getConfig().getSchema());
        List<DataMap> columnList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(PG_ALL_COLUMN, getConfig().getDatabase(), getConfig().getSchema(), tableSql),
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
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(PG_ALL_INDEX, getConfig().getDatabase(), getConfig().getSchema(), tableSql),
                    resultSet -> indexList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllIndexes failed, error: " + e.getMessage(), e);
        }
        return indexList;
    }

    private final static String PG_ALL_TABLE =
            "SELECT t.table_name,\n" +
                    "       (select max(cast(obj_description(relfilenode, 'pg_class') as varchar)) as comment\n" +
                    "        from pg_class c\n" +
                    "        where relname = t.table_name)\n" +
                    "FROM information_schema.tables t WHERE t.table_type='BASE TABLE' and t.table_catalog='%s' AND t.table_schema='%s' %s ORDER BY t.table_name";
    private final static String PG_ALL_COLUMN =
            "SELECT col.*, d.description,\n" +
                    "       (SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) AS \"dataType\"\n" +
                    "        FROM pg_catalog.pg_attribute a\n" +
                    "        WHERE a.attnum > 0\n" +
                    "          AND a.attname = col.column_name\n" +
                    "          AND NOT a.attisdropped\n" +
                    "          AND a.attrelid =\n" +
                    "              (SELECT max(cl.oid)\n" +
                    "               FROM pg_catalog.pg_class cl\n" +
                    "                        LEFT JOIN pg_catalog.pg_namespace n ON n.oid = cl.relnamespace\n" +
                    "               WHERE cl.relname = col.table_name))\n" +
                    "FROM information_schema.columns col\n" +
                    "         JOIN pg_class c ON c.relname = col.table_name\n" +
                    "         LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = col.ordinal_position\n" +
                    "WHERE col.table_catalog='%s' AND col.table_schema='%s' %s\n" +
                    "ORDER BY col.table_name,col.ordinal_position";
    private final static String PG_ALL_INDEX =
            "SELECT\n" +
                    "    t.relname AS table_name,\n" +
                    "    i.relname AS index_name,\n" +
                    "    a.attname AS column_name,\n" +
                    "    ix.indisunique AS is_unique,\n" +
                    "    ix.indisprimary AS is_primary,\n" +
                    "    (CASE WHEN ix.indoption[a.attnum-1]&1=0 THEN 'A' ELSE 'D' END) AS asc_or_desc\n" +
                    "FROM\n" +
                    "    pg_class t,\n" +
                    "    pg_class i,\n" +
                    "    pg_index ix,\n" +
                    "    pg_attribute a,\n" +
                    "    information_schema.tables tt\n" +
                    "WHERE\n" +
                    "        t.oid = ix.indrelid\n" +
                    "  AND i.oid = ix.indexrelid\n" +
                    "  AND a.attrelid = t.oid\n" +
                    "  AND a.attnum = ANY(ix.indkey)\n" +
                    "  AND t.relkind = 'r'\n" +
                    "  AND tt.table_name=t.relname\n" +
                    "  AND tt.table_catalog='%s'\n" +
                    "  AND tt.table_schema='%s'\n" +
                    "    %s\n" +
                    "ORDER BY t.relname, i.relname, a.attnum";
}
