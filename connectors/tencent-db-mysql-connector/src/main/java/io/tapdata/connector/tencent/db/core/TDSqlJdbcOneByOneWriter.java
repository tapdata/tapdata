package io.tapdata.connector.tencent.db.core;

import io.tapdata.connector.mysql.writer.MysqlJdbcOneByOneWriter;
import io.tapdata.connector.tencent.db.mysql.MysqlJdbcContext;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author GavinXiao
 * @description TDSqlJdbcOneByOneWriter create by Gavin
 * @create 2023/4/13 17:19
 **/
public class TDSqlJdbcOneByOneWriter extends MysqlJdbcOneByOneWriter {
    public static final String TAG = TDSqlJdbcOneByOneWriter.class.getSimpleName();
    public static final String NORMAL_TABLE = "NORMAL";
    public static final String PARTITION_TABLE = "PARTITION";
    private AtomicReference<String> tableType = new AtomicReference<>("NORMAL");
    public TDSqlJdbcOneByOneWriter type(AtomicReference<String> tableType){
        this.tableType = tableType;
        return this;
    }

    protected synchronized String tableType(){
        return this.tableType.get();
    }

    public TDSqlJdbcOneByOneWriter(MysqlJdbcContext mysqlJdbcContext, Object jdbcCacheMap) throws Throwable {
        super(mysqlJdbcContext, jdbcCacheMap);
    }

    @Override
    protected List<String> updateKeyValues(LinkedHashMap<String, TapField> nameFieldMap, TapRecordEvent tapRecordEvent) {
        if (!PARTITION_TABLE.equals(tableType())) return super.updateKeyValues(nameFieldMap, tapRecordEvent);
        List<String> setList = new ArrayList<>();
        nameFieldMap.forEach((fieldName, field) -> {
            if (!needAddIntoPreparedStatementValues(field, tapRecordEvent)) {
                return;
            }
            //if (null == field.getPartitionKey() || !field.getPartitionKey()) {
            //    setList.add("`" + fieldName + "`=?");
            //}
            if (null == field.getComment() || !TDSqlDiscoverSchema.PARTITION_KEY_SINGLE.equals(field.getComment())) {
                setList.add("`" + fieldName + "`=?");
            }
        });
        return setList;
    }

    @Override
    protected int setPreparedStatementValues(TapTable tapTable, TapRecordEvent tapRecordEvent, PreparedStatement preparedStatement) throws Throwable {
        if (!PARTITION_TABLE.equals(tableType())) return super.setPreparedStatementValues(tapTable, tapRecordEvent, preparedStatement);
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        int parameterIndex = 1;
        Map<String, Object> after = getAfter(tapRecordEvent);
        if (MapUtils.isEmpty(after)) {
            throw new Exception("Set prepared statement values failed, after is empty: " + tapRecordEvent);
        }
        List<String> afterKeys = new ArrayList<>(after.keySet());
        for (String fieldName : nameFieldMap.keySet()) {
            TapField tapField = nameFieldMap.get(fieldName);
            if (null == tapField || (null != tapField.getComment() && TDSqlDiscoverSchema.PARTITION_KEY_SINGLE.equals(tapField.getComment()))) {
                afterKeys.remove(fieldName);
                continue;
            }
            if (!needAddIntoPreparedStatementValues(tapField, tapRecordEvent)) {
                continue;
            }
            preparedStatement.setObject(parameterIndex++, after.get(fieldName));
            afterKeys.remove(fieldName);
        }
        if (CollectionUtils.isNotEmpty(afterKeys)) {
            Map<String, Object> missingAfter = new HashMap<>();
            afterKeys.forEach(k -> missingAfter.put(k, after.get(k)));
            TapLogger.warn(TAG, "Found fields in after data not exists in schema fields, will skip it: " + missingAfter);
        }
        return parameterIndex;
    }


    protected PreparedStatement getUpdatePreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        JdbcCache jdbcCache = getJdbcCache();
        Map<String, PreparedStatement> updateMap = jdbcCache.getUpdateMap();
        String key = getKey(tapTable, tapRecordEvent);
        PreparedStatement preparedStatement = updateMap.get(key);
        if (null == preparedStatement) {
            DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
            String database = connectionConfig.getString("database");
            String name = connectionConfig.getString("name");
            String tableId = tapTable.getId();
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            if (MapUtils.isEmpty(nameFieldMap)) {
                throw new Exception("Create update prepared statement error, table \"" + tableId + "\"'s fields is empty, retry after reload connection \"" + name + "\"'s schema");
            }
            List<String> setList = updateKeyValues(nameFieldMap, tapRecordEvent);
            List<String> whereList = new ArrayList<>();
            Collection<String> uniqueKeys = getUniqueKeys(tapTable);
            for (String uniqueKey : uniqueKeys) {
                whereList.add("`" + uniqueKey + "` is NULL or `" + uniqueKey + "` = ?");
            }
            String sql = String.format(UPDATE_SQL_TEMPLATE, database, tableId, String.join(",", setList), String.join(" AND ", whereList));
            try {
                preparedStatement = jdbcCache.getConnection().prepareStatement(sql);
            } catch (SQLException e) {
                throw new Exception("Create update prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
            } catch (Exception e) {
                throw new Exception("Create update prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
            }
            updateMap.put(key, preparedStatement);
        }
        return preparedStatement;
    }

    protected PreparedStatement getDeletePreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        JdbcCache jdbcCache = getJdbcCache();
        Map<String, PreparedStatement> deleteMap = jdbcCache.getDeleteMap();
        String key = getKey(tapTable, tapRecordEvent);
        PreparedStatement preparedStatement = deleteMap.get(key);
        if (null == preparedStatement) {
            DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
            String database = connectionConfig.getString("database");
            String name = connectionConfig.getString("name");
            String tableId = tapTable.getId();
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            if (MapUtils.isEmpty(nameFieldMap)) {
                throw new Exception("Create delete prepared statement error, table \"" + tableId + "\"'s fields is empty, retry after reload connection \"" + name + "\"'s schema");
            }
            List<String> whereList = new ArrayList<>();
            Collection<String> uniqueKeys = getUniqueKeys(tapTable);
            for (String uniqueKey : uniqueKeys) {
                whereList.add("`" + uniqueKey + "` is NULL or `" + uniqueKey + "` = ?");
            }
            String sql = String.format(DELETE_SQL_TEMPLATE, database, tableId, String.join(" AND ", whereList));
            try {
                preparedStatement = jdbcCache.getConnection().prepareStatement(sql);
            } catch (SQLException e) {
                throw new Exception("Create delete prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
            } catch (Exception e) {
                throw new Exception("Create delete prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
            }
            deleteMap.put(key, preparedStatement);
        }
        return preparedStatement;
    }

    protected PreparedStatement getCheckRowExistsPreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        JdbcCache jdbcCache = getJdbcCache();
        Map<String, PreparedStatement> checkExistsMap = jdbcCache.getCheckExistsMap();
        String key = getKey(tapTable, tapRecordEvent);
        PreparedStatement preparedStatement = checkExistsMap.get(key);
        if (null == preparedStatement) {
            DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
            String database = connectionConfig.getString("database");
            String name = connectionConfig.getString("name");
            String tableId = tapTable.getId();
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            if (MapUtils.isEmpty(nameFieldMap)) {
                throw new Exception("Create check row exists prepared statement error, table \"" + tableId + "\"'s fields is empty, retry after reload connection \"" + name + "\"'s schema");
            }
            List<String> whereList = new ArrayList<>();
            Collection<String> uniqueKeys = getUniqueKeys(tapTable);
            for (String uniqueKey : uniqueKeys) {
                whereList.add("`" + uniqueKey + "` is NULL or `" + uniqueKey + "` = ?");
            }
            String sql = String.format(CHECK_ROW_EXISTS_TEMPLATE, database, tableId, String.join(" AND ", whereList));
            try {
                preparedStatement = jdbcCache.getConnection().prepareStatement(sql);
            } catch (SQLException e) {
                throw new Exception("Create check row exists prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
            } catch (Exception e) {
                throw new Exception("Create check row exists prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
            }
            checkExistsMap.put(key, preparedStatement);
        }
        return preparedStatement;
    }
}
