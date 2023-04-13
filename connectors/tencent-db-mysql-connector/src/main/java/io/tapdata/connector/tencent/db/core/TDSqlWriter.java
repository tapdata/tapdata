package io.tapdata.connector.tencent.db.core;

import io.tapdata.connector.mysql.writer.MysqlSqlBatchWriter;
import io.tapdata.connector.tencent.db.mysql.MysqlJdbcContext;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.pdk.apis.context.TapConnectorContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class TDSqlWriter extends MysqlSqlBatchWriter {
    public TDSqlWriter(MysqlJdbcContext mysqlJdbcContext) throws Throwable {
        super(mysqlJdbcContext);
    }

    @Override
    protected String appendLargeInsertOnDuplicateUpdateSql(TapConnectorContext tapConnectorContext, TapTable tapTable, List<TapRecordEvent> tapRecordEvents) {
        return appendReplaceIntoSql(tapConnectorContext, tapTable, tapRecordEvents);
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
