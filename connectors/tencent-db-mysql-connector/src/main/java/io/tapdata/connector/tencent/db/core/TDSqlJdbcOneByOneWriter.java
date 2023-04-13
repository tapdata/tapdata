package io.tapdata.connector.tencent.db.core;

import io.tapdata.connector.mysql.writer.MysqlJdbcOneByOneWriter;
import io.tapdata.connector.tencent.db.mysql.MysqlJdbcContext;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author GavinXiao
 * @description TDSqlJdbcOneByOneWriter create by Gavin
 * @create 2023/4/13 17:19
 **/
public class TDSqlJdbcOneByOneWriter extends MysqlJdbcOneByOneWriter {
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
            if (null == field.getPartitionKey() || !field.getPartitionKey()) {
                setList.add("`" + fieldName + "`=?");
            }
        });
        return setList;
    }
}
