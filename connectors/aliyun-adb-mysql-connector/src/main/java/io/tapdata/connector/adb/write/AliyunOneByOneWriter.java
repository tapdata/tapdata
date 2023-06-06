package io.tapdata.connector.adb.write;

import io.tapdata.connector.mysql.MysqlJdbcContextV2;
import io.tapdata.connector.mysql.writer.MysqlJdbcOneByOneWriter;
import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.entity.utils.DataMap;
import io.tapdata.exception.runtime.TapPdkSkippableDataEx;
import io.tapdata.pdk.apis.context.TapConnectorContext;
import io.tapdata.pdk.apis.entity.WriteListResult;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.tapdata.base.ConnectorBase.deleteDMLEvent;
import static io.tapdata.base.ConnectorBase.insertRecordEvent;

/**
 * @author GavinXiao
 * @description AliyunOneByOneWriter create by Gavin
 * @create 2023/6/5 14:11
 **/
public class AliyunOneByOneWriter extends MysqlJdbcOneByOneWriter implements WriteStage{

    private static final String TAG = AliyunOneByOneWriter.class.getSimpleName();

    public AliyunOneByOneWriter(MysqlJdbcContextV2 mysqlJdbcContext, Map<String, JdbcCache> jdbcCacheMap) throws Throwable {
        super(mysqlJdbcContext, jdbcCacheMap);
    }

    public void splitToInsertAndDeleteFromUpdate(TapConnectorContext context, TapUpdateRecordEvent event, TapTable table, WriteListResult<TapRecordEvent> result) throws Throwable {
        if (null == event){
            return;
        }
        Map<String, Object> before = event.getBefore();
        Map<String, Object> after = event.getAfter();
        if (null == before || before.isEmpty() || null == after || after.isEmpty()){
            int updateRow = super.doUpdateOne(context, table, event);
            result.incrementModified(updateRow);
            return;
        }
        if (Objects.isNull(table)){
            throw new CoreException("TapTable can not be empty, update event will be cancel");
        }
        String tableId = table.getId();
        Long referenceTime = event.getReferenceTime();

        int deleteCount = doDeleteOne(context, table, deleteDMLEvent(before, tableId).referenceTime(referenceTime));
        result.incrementRemove(deleteCount);

        int addCount = doInsert(context, table, insertRecordEvent(after, tableId).referenceTime(referenceTime));
        result.incrementInserted(addCount);
    }

    @Override
    public int splitToInsertAndDeleteFromUpdate(TapConnectorContext context, TapUpdateRecordEvent event, TapTable table) throws Throwable {
        if (null == event){
            return 0;
        }
        Map<String, Object> before = event.getBefore();
        Map<String, Object> after = event.getAfter();
        if (null == before || before.isEmpty() || null == after || after.isEmpty()){
            super.doUpdateOne(context, table, event);
            //result.incrementModified(updateRow);
            return 1;
        }
        if (Objects.isNull(table)){
            throw new CoreException("TapTable can not be empty, update event will be cancel");
        }
        String tableId = table.getId();
        Long referenceTime = event.getReferenceTime();

        doDeleteOne(context, table, deleteDMLEvent(before, tableId).referenceTime(referenceTime));
        //result.incrementRemove(deleteCount);
        doInsert(context, table, insertRecordEvent(after, tableId).referenceTime(referenceTime));
        //result.incrementInserted(addCount);
        return 1;
    }

    @Override
    protected int doUpdate(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        if (hasEqualsValueOfPrimaryKey(tapConnectorContext, (TapUpdateRecordEvent) tapRecordEvent, tapTable)) {
            return super.doUpdate(tapConnectorContext, tapTable, tapRecordEvent);
        } else {
            return splitToInsertAndDeleteFromUpdate(tapConnectorContext, (TapUpdateRecordEvent) tapRecordEvent, tapTable);
        }
    }

    @Override
    protected PreparedStatement getInsertPreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        JdbcCache jdbcCache = getJdbcCache();
        Map<String, PreparedStatement> insertMap = jdbcCache.getInsertMap();
        String key = getKey(tapTable, tapRecordEvent);
        PreparedStatement preparedStatement = insertMap.get(key);
        DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        String name = connectionConfig.getString("name");
        String tableId = tapTable.getId();
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (MapUtils.isEmpty(nameFieldMap)) {
            throw new Exception("Create insert prepared statement error, table \"" + tableId + "\"'s fields is empty, retry after reload connection \"" + name + "\"'s schema");
        }
        List<String> fields = new ArrayList<>();
        nameFieldMap.forEach((fieldName, field) -> {
            if (!needAddIntoPreparedStatementValues(field, tapRecordEvent)) {
                return;
            }
            fields.add("`" + fieldName + "`");
        });
        List<String> questionMarks = fields.stream().map(f -> "?").collect(Collectors.toList());
        String sql = String.format(INSERT_SQL_TEMPLATE, database, tableId, String.join(",", fields), String.join(",", questionMarks));
        try {
            preparedStatement = jdbcCache.getConnection().prepareStatement(sql);
        } catch (SQLException e) {
            throw new Exception("Create insert prepared statement error, sql: " + sql + ", message: " + e.getSQLState() + " " + e.getErrorCode() + " " + e.getMessage(), e);
        } catch (Exception e) {
            throw new Exception("Create insert prepared statement error, sql: " + sql + ", message: " + e.getMessage(), e);
        }
        insertMap.put(key, preparedStatement);
        return preparedStatement;
    }

    @Override
    protected PreparedStatement getDeletePreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        JdbcCache jdbcCache = getJdbcCache();
        Map<String, PreparedStatement> deleteMap = jdbcCache.getDeleteMap();
        String key = getKey(tapTable, tapRecordEvent);
        PreparedStatement preparedStatement = deleteMap.get(key);
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
            whereList.add("`" + uniqueKey + "`<=>?");
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
        return preparedStatement;
    }

    @Override
    protected PreparedStatement getUpdatePreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
        JdbcCache jdbcCache = getJdbcCache();
        Map<String, PreparedStatement> updateMap = jdbcCache.getUpdateMap();
        String key = getKey(tapTable, tapRecordEvent);
        PreparedStatement preparedStatement = null;//updateMap.get(key);
        DataMap connectionConfig = tapConnectorContext.getConnectionConfig();
        String database = connectionConfig.getString("database");
        String name = connectionConfig.getString("name");
        String tableId = tapTable.getId();
        LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
        if (MapUtils.isEmpty(nameFieldMap)) {
            throw new Exception("Create update prepared statement error, table \"" + tableId + "\"'s fields is empty, retry after reload connection \"" + name + "\"'s schema");
        }
        List<String> setList = new ArrayList<>();
        Collection<String> keys = tapTable.primaryKeys(false);
        nameFieldMap.forEach((fieldName, field) -> {
            if (!needAddIntoPreparedStatementValues(field, tapRecordEvent)) {
                return;
            }
            if (null != keys && !keys.isEmpty() && !keys.contains(fieldName)) {
                setList.add("`" + fieldName + "`=?");
            }
        });
        List<String> whereList = new ArrayList<>();
        Collection<String> uniqueKeys = getUniqueKeys(tapTable);
        for (String uniqueKey : uniqueKeys) {
            whereList.add("`" + uniqueKey + "`<=>?");
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
        return preparedStatement;
    }

    @Override
    protected int setPreparedStatementValues(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent, PreparedStatement preparedStatement) throws Throwable {
        if (tapRecordEvent instanceof TapUpdateRecordEvent) {
            LinkedHashMap<String, TapField> nameFieldMap = tapTable.getNameFieldMap();
            int parameterIndex = 1;
            Map<String, Object> after = getAfter(tapRecordEvent);
            if (MapUtils.isEmpty(after)) {
                throw new Exception("Set prepared statement values failed, after is empty: " + tapRecordEvent);
            }
            List<String> afterKeys = new ArrayList<>(after.keySet());
            Collection<String> keys = tapTable.primaryKeys(false);
            for (String fieldName : nameFieldMap.keySet()) {
                try {
                    TapField tapField = nameFieldMap.get(fieldName);
                    if (!needAddIntoPreparedStatementValues(tapField, tapRecordEvent) || (null != keys && !keys.isEmpty() && keys.contains(fieldName))) {
                        continue;
                    }
                    preparedStatement.setObject(parameterIndex++, after.get(fieldName));
                    afterKeys.remove(fieldName);
                } catch (SQLException e) {
                    throw new TapPdkSkippableDataEx(String.format("Set prepared statement values failed: %s, field: '%s', value '%s', record: %s"
                            , e.getMessage()
                            , fieldName
                            , after.get(fieldName)
                            , tapRecordEvent
                    ), tapConnectorContext.getSpecification().getId(), e);
                }
            }
            if (CollectionUtils.isNotEmpty(afterKeys)) {
                Map<String, Object> missingAfter = new HashMap<>();
                afterKeys.forEach(k -> missingAfter.put(k, after.get(k)));
                TapLogger.warn(TAG, "Found fields in after data not exists in schema fields, will skip it: " + missingAfter);
            }
            return parameterIndex;
        } else{
            return super.setPreparedStatementValues(tapConnectorContext, tapTable, tapRecordEvent, preparedStatement);
        }
    }

    @Override
    protected PreparedStatement getCheckRowExistsPreparedStatement(TapConnectorContext tapConnectorContext, TapTable tapTable, TapRecordEvent tapRecordEvent) throws Throwable {
//        if (tapRecordEvent instanceof TapUpdateRecordEvent) {
            JdbcCache jdbcCache = getJdbcCache();
            Map<String, PreparedStatement> checkExistsMap = jdbcCache.getCheckExistsMap();
            String key = getKey(tapTable, tapRecordEvent);
            PreparedStatement preparedStatement = checkExistsMap.get(key);
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
                whereList.add("`" + uniqueKey + "`<=>?");
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
            return preparedStatement;
//        } else {
//            return super.getCheckRowExistsPreparedStatement(tapConnectorContext, tapTable, tapRecordEvent);
//        }
    }
}
