package io.tapdata.connector.tidb;

import com.zaxxer.hikari.HikariDataSource;
import io.tapdata.common.JdbcContext;
import io.tapdata.common.ResultSetConsumer;
import io.tapdata.entity.conversion.TableFieldTypesGenerator;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.mapping.DefaultExpressionMatchingMap;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapIndex;
import io.tapdata.entity.schema.TapIndexField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.simplify.TapSimplify;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.kit.DbKit;
import io.tapdata.kit.EmptyKit;
import io.tapdata.kit.StringKit;
import org.apache.commons.collections4.CollectionUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static io.tapdata.entity.simplify.TapSimplify.list;

/**
 * @author lemon
 */
public class TidbContext extends JdbcContext{
    private static final String TAG = TidbContext.class.getSimpleName();

    private static final String SELECT_TIDB_VERSION = "select version() as version";

    private static final String DATABASE_TIMEZON_SQL = "SELECT TIMEDIFF(NOW(), UTC_TIMESTAMP()) as timezone";

    private final static String TIDB_ALL_TABLE = "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_TYPE='BASE TABLE'";

    private static final String TIDB_ALL_COLUMNS = "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s'";


    private static final String DROP_TABLE_IF_EXISTS_SQL = "DROP TABLE IF EXISTS `%s`.`%s`";

    private static final String TRUNCATE_TABLE_SQL = "TRUNCATE TABLE `%s`.`%s`";


    private final static String TIDB_ALL_INDEX = "select *\n" +
            "from (select i.TABLE_NAME,\n" +
            "             i.INDEX_NAME,\n" +
            "             i.INDEX_TYPE,\n" +
            "             i.COLLATION,\n" +
            "             i.NON_UNIQUE,\n" +
            "             i.COLUMN_NAME,\n" +
            "             i.SEQ_IN_INDEX,\n" +
            "             (select k.CONSTRAINT_NAME\n" +
            "              from INFORMATION_SCHEMA.KEY_COLUMN_USAGE k\n" +
            "              where k.TABLE_SCHEMA = '%s'\n" +
            "                and k.TABLE_NAME = i.TABLE_NAME\n" +
            "                and i.INDEX_NAME = CONCAT(k.CONSTRAINT_NAME, '_idx')\n" +
            "                and i.COLUMN_NAME = k.COLUMN_NAME) CONSTRAINT_NAME\n" +
            "      from INFORMATION_SCHEMA.STATISTICS i\n" +
            "\n" +
            "      where i.TABLE_SCHEMA = '%s'\n" +
            "        and i.TABLE_NAME %s\n" +
            "        and i.INDEX_NAME <> 'PRIMARY') t\n" +
            "where t.CONSTRAINT_NAME is null";



    public TidbContext(TidbConfig config, HikariDataSource hikariDataSource) {
        super(config, hikariDataSource);
    }

    @Override
    public List<DataMap> queryAllTables(List<String> tableNames) {
        List<DataMap> tableList = TapSimplify.list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND TABLE_NAME IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(TIDB_ALL_TABLE, getConfig().getDatabase())+ tableSql,
                    resultSet -> tableList.addAll(DbKit.getDataFromResultSet(resultSet)));
          } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllTables failed, error: " + e.getMessage(), e);
        }
        return tableList;
    }

    @Override
    public void queryAllTables(List<String> tableNames, int batchSize, Consumer<List<String>> consumer) {

    }

    @Override
    public List<DataMap> queryAllColumns(List<String> tableNames) {
        String schema = getConfig().getDatabase();
        TapLogger.debug(TAG, "Query columns of some tables, schema: " + schema);
        List<DataMap> columnList = list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? "AND COL.TABLE_NAME IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(TIDB_ALL_COLUMNS, schema, tableSql),
                    resultSet -> columnList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllColumns failed, error: " + e.getMessage(), e);
        }
        return columnList;
    }

    @Override
    public List<DataMap> queryAllIndexes(List<String> tableNames) {
        String schema = getConfig().getDatabase();
        TapLogger.debug(TAG, "Query indexes of some tables, schema: " + schema);
        List<DataMap> indexList = list();
        String tableSql = EmptyKit.isNotEmpty(tableNames) ? " IN (" + StringKit.joinString(tableNames, "'", ",") + ")" : "";
        try {
            query(String.format(TIDB_ALL_INDEX, schema, schema, tableSql),
                    resultSet -> indexList.addAll(DbKit.getDataFromResultSet(resultSet)));
        } catch (Throwable e) {
            TapLogger.error(TAG, "Execute queryAllIndexes failed, error: " + e.getMessage(), e);
        }
        return indexList;
    }

    public String getVersion() throws Throwable {
        AtomicReference<String> version = new AtomicReference<>();
        query(SELECT_TIDB_VERSION, resultSet -> {
            if (resultSet.next()) {
                version.set(resultSet.getString("version"));
            }
        });
        return version.get();
    }

    public void discoverFields(List<DataMap> columnList, TapTable tapTable, TableFieldTypesGenerator tableFieldTypesGenerator,
                                DefaultExpressionMatchingMap dataTypesMap) {
        AtomicInteger primaryPos = new AtomicInteger(1);
        if (CollectionUtils.isEmpty(columnList)) {
            return;
        }
        columnList.forEach(dataMap -> {
            String columnName = dataMap.getString("COLUMN_NAME");
            String columnType = dataMap.getString("COLUMN_TYPE");
            TapField field = TapSimplify.field(columnName, columnType);
            tableFieldTypesGenerator.autoFill(field, dataTypesMap);

            int ordinalPosition = Integer.parseInt(dataMap.getString("ORDINAL_POSITION"));
            field.pos(ordinalPosition);

            String isNullable = dataMap.getString("IS_NULLABLE");
            field.nullable(isNullable.equals("YES"));

            Object columnKey = dataMap.getObject("COLUMN_KEY");
            if (columnKey instanceof String && columnKey.equals("PRI")) {
                field.primaryKeyPos(primaryPos.getAndIncrement());
            }
            tapTable.add(field);
        });
    }

    public void discoverIndexes(List<DataMap> indexList, TapTable tapTable) {
        List<TapIndex> indexes = new ArrayList<>();

        if (CollectionUtils.isEmpty(indexList)) {
            return;
        }
        indexList.forEach(dataMap -> {
            String indexName = dataMap.getString("INDEX_NAME");
            TapIndex tapIndex = indexes.stream().filter(i -> i.getName().equals(indexName)).findFirst().orElse(null);
            if (null == tapIndex) {
                tapIndex = new TapIndex();
                tapIndex.setName(indexName);
                int nonUnique = Integer.parseInt(dataMap.getString("NON_UNIQUE"));
                tapIndex.setUnique(nonUnique == 1);
                tapIndex.setPrimary(false);
                indexes.add(tapIndex);
            }
            List<TapIndexField> indexFields = tapIndex.getIndexFields();
            if (null == indexFields) {
                indexFields = new ArrayList<>();
                tapIndex.setIndexFields(indexFields);
            }
            TapIndexField tapIndexField = new TapIndexField();
            tapIndexField.setName(dataMap.getString("COLUMN_NAME"));
            String collation = dataMap.getString("COLLATION");
            tapIndexField.setFieldAsc("A".equals(collation));
            indexFields.add(tapIndexField);
        });
        tapTable.setIndexList(indexes);

    }

    public void dropTable(String tableName) throws Throwable {
        String database = getConfig().getDatabase();
        String sql = String.format(DROP_TABLE_IF_EXISTS_SQL, database, tableName);
        execute(sql);
    }

    public void clearTable(String tableName) throws Throwable {
        String database = getConfig().getDatabase();
        String sql = String.format(TRUNCATE_TABLE_SQL, database, tableName);
        execute(sql);
    }


    /**
     * 查询
     * @param resultSetConsumer
     */
    @Override
    public void query(String sql, ResultSetConsumer resultSetConsumer) throws Throwable {
        TapLogger.debug(TAG, "Execute query, sql: " + sql);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)
        ) {
            statement.setFetchSize(1000);
            if (null != resultSet) {
                resultSetConsumer.accept(resultSet);
            }
        } catch (SQLException e) {
            throw new Exception("Execute query failed, sql: " + sql + ", code: " + e.getSQLState() + "(" + e.getErrorCode() + "), error: " + e.getMessage(), e);
        }
    }


    public String timezone() throws SQLException {

        String timeZone = null;
        TapLogger.debug(TAG, "Get timezone sql: " + DATABASE_TIMEZON_SQL);
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(DATABASE_TIMEZON_SQL)
        ) {
            while (resultSet.next()) {
                timeZone = resultSet.getString(1);
                return timeZone;
            }
        }
        return null;
    }
}
