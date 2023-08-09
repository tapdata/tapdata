package io.tapdata.connector.dws;

import io.tapdata.common.JdbcContext;
import io.tapdata.connector.dws.config.DwsConfig;
import io.tapdata.connector.dws.exception.DwsExceptionCollector;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.connector.postgres.exception.PostgresExceptionCollector;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class DwsJdbcContext extends JdbcContext {

    private final static String TAG = DwsJdbcContext.class.getSimpleName();

    public DwsJdbcContext(DwsConfig config) {
        super(config);
        exceptionCollector = new DwsExceptionCollector();
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
    protected String queryAllTablesSql(String schema, List<String> tableNames) {
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        return String.format(PG_ALL_TABLE, getConfig().getDatabase(), schema, tableSql);
    }

    @Override
    protected String queryAllColumnsSql(String schema, List<String> tableNames) {
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        return String.format(PG_ALL_COLUMN, schema, getConfig().getDatabase(), schema, tableSql);
    }

    @Override
    protected String queryAllIndexesSql(String schema, List<String> tableNames) {
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        return String.format(PG_ALL_INDEX, getConfig().getDatabase(), schema, tableSql);
    }

    //查分区
    public Integer queryFromPGPARTITION(List<String> tableNames) {
        AtomicInteger count = new AtomicInteger();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND relname IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format("SELECT count(*) FROM \"pg_catalog\".\"pg_partition\" where 1=1 %s", tableSql),
                    resultSet -> {
                        if (null != resultSet && resultSet.next()) {
                            count.set(resultSet.getInt(1));
                        }
                    });
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllIndexes failed, error: " + e.getMessage(), e);
        }
        return count.get();
    }



    public List<String> queryDistributedKeys(String schema,String tableName) {
        List<String> distributedKeys = new ArrayList();
        try {
            query(String.format("SELECT getdistributekey('\""+schema+"\".\""+tableName+"\"')"),
                    resultSet -> {
                            if (null != resultSet && resultSet.next()) {
                                String distributeStr = resultSet.getString(1);
                                if (null != distributeStr){
                                    String[] split = distributeStr.split(", ");
                                    for (String key : split) {
                                        distributedKeys.add(key);
                                    }
                                }
                            }
                    });
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllIndexes failed, error: " + e.getMessage(), e);
        }
        return distributedKeys;
    }

    public DataMap getTableInfo(String tableName) {
        DataMap dataMap = DataMap.create();
        List<String> list = new ArrayList<>();
        list.add("size");
        list.add("rowcount");
        try {
            query(String.format(TABLE_INFO_SQL, tableName, getConfig().getDatabase(), getConfig().getSchema()), resultSet -> {
                while (resultSet.next()) {
                    dataMap.putAll(DbKit.getRowFromResultSet(resultSet, list));
                }
            });
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute getTableInfo failed, error: " + e.getMessage(), e);
        }
        return dataMap;
    }

    protected final static String PG_ALL_TABLE =
            "SELECT t.table_name \"tableName\",\n" +
                    "       (select max(cast(obj_description(relfilenode, 'pg_class') as varchar)) as \"tableComment\"\n" +
                    "        from pg_class c\n" +
                    "        where relname = t.table_name)\n" +
                    "FROM information_schema.tables t WHERE t.table_type='BASE TABLE' and t.table_catalog='%s' AND t.table_schema='%s' %s ORDER BY t.table_name";
    protected final static String PG_ALL_COLUMN =
            "SELECT\n" +
                    "    col.table_name \"tableName\",\n" +
                    "    col.column_name \"columnName\",\n" +
                    "    col.column_default \"columnDefault\",\n" +
                    "    col.is_nullable \"nullable\",\n" +
                    "       (SELECT max(d.description)\n" +
                    "        FROM pg_catalog.pg_class c,\n" +
                    "             pg_description d\n" +
                    "        WHERE c.relname = col.table_name\n" +
                    "          AND d.objoid = c.oid\n" +
                    "          AND d.objsubid = col.ordinal_position) AS \"columnComment\",\n" +
                    "       (SELECT pg_catalog.format_type(a.atttypid, a.atttypmod)\n" +
                    "        FROM pg_catalog.pg_attribute a\n" +
                    "        WHERE a.attnum > 0\n" +
                    "          AND a.attname = col.column_name\n" +
                    "          AND NOT a.attisdropped\n" +
                    "          AND a.attrelid =\n" +
                    "              (SELECT max(cl.oid)\n" +
                    "               FROM pg_catalog.pg_class cl\n" +
                    "               WHERE cl.relname = col.table_name and cl.relnamespace=(select oid from pg_namespace where nspname='%s'))) AS \"dataType\"\n" +
                    "FROM information_schema.columns col\n" +
                    "WHERE col.table_catalog = '%s'\n" +
                    "  AND col.table_schema = '%s' %s\n" +
                    "ORDER BY col.table_name, col.ordinal_position";

    protected final static String PG_ALL_INDEX =
            "SELECT\n" +
                    "    t.relname AS \"tableName\",\n" +
                    "    i.relname AS \"indexName\",\n" +
                    "    a.attname AS \"columnName\",\n" +
                    "    (CASE WHEN ix.indisunique THEN '1' ELSE '0' END) AS \"isUnique\",\n" +
                    "    (CASE WHEN ix.indisprimary THEN '1' ELSE '0' END) AS \"isPk\",\n" +
                    "    (CASE WHEN ix.indoption[row_number() over (partition by t.relname,i.relname order by a.attnum) - 1] & 1 = 0 THEN '1' ELSE '0' END) AS \"isAsc\"\n" +
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
                    "  AND tt.table_schema='%s' %s\n" +
                    "ORDER BY t.relname, i.relname, a.attnum";


    protected final static String TABLE_INFO_SQL = "SELECT\n" +
            " pg_total_relation_size('\"' || table_schema || '\".\"' || table_name || '\"') AS size,\n" +
            " (select reltuples from pg_class  pc where pc.relname = t1.table_name ) as rowcount \n" +
            " FROM information_schema.tables t1 where t1.table_name ='%s' and t1.table_catalog='%s' and t1.table_schema='%s' ";

}
