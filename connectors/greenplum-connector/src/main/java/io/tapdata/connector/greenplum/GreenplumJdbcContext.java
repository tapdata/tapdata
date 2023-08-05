package io.tapdata.connector.greenplum;

import io.tapdata.connector.postgres.PostgresJdbcContext;
import io.tapdata.connector.postgres.config.PostgresConfig;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.util.List;

public class GreenplumJdbcContext extends PostgresJdbcContext {

    public GreenplumJdbcContext(PostgresConfig config) {
        super(config);
    }

    @Override
    protected String queryAllIndexesSql(String schema, List<String> tableNames) {
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND table_name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        return String.format(GP_ALL_INDEX, getConfig().getDatabase(), schema, tableSql);
    }

    private final static String GP_ALL_INDEX =
            "SELECT\n" +
                    "    t.relname AS \"tableName\",\n" +
                    "    i.relname AS \"indexName\",\n" +
                    "    a.attname AS \"columnName\",\n" +
                    "    (CASE WHEN ix.indisunique THEN '1' ELSE '0' END) AS \"isUnique\",\n" +
                    "    (CASE WHEN ix.indisprimary THEN '1' ELSE '0' END) AS \"isPk\",\n" +
                    "    '1' AS \"isAsc\"\n" +
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
}
