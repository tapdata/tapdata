package io.tapdata.connector.tencent.db.core;

import io.tapdata.connector.mysql.writer.MysqlJdbcOneByOneWriter;
import io.tapdata.connector.tencent.db.mysql.MysqlJdbcContext;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.logger.TapLogger;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.sql.PreparedStatement;
import java.util.*;

/**
 * @author GavinXiao
 * @description TDSqlJdbcOneByOneWriter create by Gavin
 * @create 2023/4/13 17:19
 **/
public class TDSqlJdbcOneByOneWriter extends MysqlJdbcOneByOneWriter {
    public static final String TAG = TDSqlJdbcOneByOneWriter.class.getSimpleName();
    public TDSqlJdbcOneByOneWriter(MysqlJdbcContext mysqlJdbcContext, Object jdbcCacheMap) throws Throwable {
        super(mysqlJdbcContext, jdbcCacheMap);
    }

    @Override
    protected List<String> updateKeyValues(LinkedHashMap<String, TapField> nameFieldMap, TapRecordEvent tapRecordEvent){
        List<String> setList = new ArrayList<>();
        nameFieldMap.forEach((fieldName, field) -> {
            if (!needAddIntoPreparedStatementValues(field, tapRecordEvent)) {
                return;
            }
//            if (null == field.getPartitionKey() || !field.getPartitionKey()) {
//                setList.add("`" + fieldName + "`=?");
//            }
            if (null == field.getComment() || !TDSqlDiscoverSchema.PARTITION_KEY_SINGLE.equals(field.getComment())) {
                setList.add("`" + fieldName + "`=?");
            }
        });
        return setList;
    }

    @Override
    protected int setPreparedStatementValues(TapTable tapTable, TapRecordEvent tapRecordEvent, PreparedStatement preparedStatement) throws Throwable {
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
}
