package io.tapdata.connector.tidb;

import io.tapdata.common.JdbcContext;
import io.tapdata.connector.tidb.config.TidbConfig;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * @author Dexter
 */
public class TidbJdbcRunner extends JdbcContext {
    private static final String TAG = TidbJdbcRunner.class.getSimpleName();

    public TidbJdbcRunner(TidbConfig config) {
        super(config);
    }

    @Override
    public Connection getConnection() throws SQLException {
        Connection connection = super.getConnection();
        // set autoCommit=true since tidb will throw deadlock error when processing
        // non-primary-key data; more detail
        connection.setAutoCommit(true);

        return connection;
    }

    @Override
    public String queryVersion() {
        AtomicReference<String> version = new AtomicReference<>("");
        try {
            queryWithNext(TIDB_VERSION, resultSet -> version.set(resultSet.getString(1)));
        } catch (Throwable e) {
            e.printStackTrace();
        }
        return version.get();
    }

    @Override
    public void queryAllTables(List<String> tableNames, int batchSize, Consumer<List<String>> consumer) {
        TapLogger.debug(TAG, "Query some tables, schema: " + getConfig().getSchema());
        List<String> tableList = TapSimplify.list();
        String tableFilter = EmptyKit.isNotEmpty(tableNames) ? "AND tbl.name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(TIDB_ALL_TABLE, getConfig().getSchema(), tableFilter),
                    rs -> {
                        while (rs.next()) {
                            String tableName = rs.getString("name");
                            if (null != tableName && !"".equals(tableName.trim())) {
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
    public List<DataMap> queryAllTables(List<String> tableNames) {
        TapLogger.debug(TAG, "Query some tables, schema: " + getConfig().getSchema());
        List<DataMap> tableList = TapSimplify.list();
        String tableFilter = EmptyKit.isNotEmpty(tableNames) ? "AND tbl.name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(TIDB_ALL_TABLE, getConfig().getSchema(), tableFilter),
                    rs -> tableList.addAll(DbKit.getDataFromResultSet(rs)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllTables failed, error: " + e.getMessage(), e);
        }

        return tableList;
    }

    @Override
    public List<DataMap> queryAllColumns(List<String> tableNames) {
        TapLogger.debug(TAG, "Query columns of some tables, schema: " + getConfig().getSchema());
        List<DataMap> columnList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND tbl.name IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(TIDB_ALL_COLUMN, getConfig().getSchema(), tableSql),
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
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND object_name(t_i.object_id) IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(TIDB_ALL_INDEX, getConfig().getSchema(), tableSql),
                    resultSet -> indexList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllIndexes failed, error: " + e.getMessage(), e);
        }
        return indexList;
    }

    private final static String TIDB_VERSION = "SELECT @@VERSION";
    private final static String TIDB_ALL_TABLE = "" +
            "   SELECT tbl.name, \n" +
            "          ext.value \n" +
            "     FROM sys.tables tbl \n" +
            "LEFT JOIN sys.extended_properties ext \n" +
            "       ON ext.major_id = tbl.object_id \n" +
            "      AND ext.minor_id = 0 \n" +
            "      AND ext.name = 'TIDB_Description' \n" +
            "    WHERE schema_name(tbl.schema_id) = '%s' \n" +
            "      %s \n" +
            "ORDER BY name";

    private final static String TIDB_ALL_COLUMN = "" +
            "    SELECT tbl.name AS table_name, \n" +
            "           col.name AS column_name, \n" +
            "           ext.value AS comment, \n" +
            "           col.column_id AS column_id, \n" +
            "           types.name AS type, \n" +
            "           col.max_length AS max_length, \n" +
            "           col.precision AS precision, \n" +
            "           col.scale AS scale, \n" +
            "           col.is_nullable AS is_nullable \n" +
            "      FROM sys.tables tbl \n" +
            "INNER JOIN sys.columns col \n" +
            "        ON tbl.object_id = col.object_id \n" +
            " LEFT JOIN sys.types types \n" +
            "        ON col.user_type_id = types.user_type_id \n" +
            " LEFT JOIN sys.extended_properties ext \n" +
            "        ON ext.major_id = col.object_id \n" +
            "       AND ext.minor_id = col.column_id \n" +
            "       AND ext.name = 'TIDB_Description' \n" +
            "     WHERE schema_name(tbl.schema_id) = '%s' \n" +
            "       %s \n" +
            "  ORDER BY tbl.name, col.column_id";

    private final static String TIDB_ALL_INDEX = "" +
            "    SELECT t_i.Name index_name, \n" +
            "           t_i.type_desc IndexType, \n" +
            "           t_i.is_primary_key is_primary, \n" +
            "           t_i.is_unique is_unique, \n" +
            "           object_name(t_i.object_id) table_name, \n" +
            "           t_c.Name column_name, \n" +
            "           t_ic.index_column_id index_column_id, \n" +
            "           CASE INDEXKEY_PROPERTY(t_ic.object_id, t_ic.index_id, t_ic.index_column_id, 'IsDescending') WHEN 1 THEN 'DESC' WHEN 0 THEN 'ASC' ELSE null END column_collation \n" +
            "      FROM sys.indexes t_i \n" +
            "INNER JOIN sys.index_columns t_ic \n" +
            "        ON t_i.object_id = t_ic.object_id \n" +
            "       AND t_i.index_id= t_ic.index_id \n" +
            "INNER JOIN sys.columns t_c \n" +
            "        ON t_c.object_id = t_ic.object_id \n" +
            "       AND t_c.column_id= t_ic.Column_id \n" +
            "     WHERE t_i.is_disabled = 0 \n" +
            "       AND object_schema_name(t_i.object_id) = '%s' \n" +
            "       %s \n" +
            "     ORDER BY t_i.index_id, t_ic.index_column_id";
}
