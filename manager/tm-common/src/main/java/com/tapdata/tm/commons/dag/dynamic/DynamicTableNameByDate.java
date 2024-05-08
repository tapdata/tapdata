package com.tapdata.tm.commons.dag.dynamic;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * dynamic table name:
 *  Directly add the suffix based on the current time: ${oldTableName}_yyyy_mm_dd
 * */
class DynamicTableNameByDate extends DynamicTableStage {
    public DynamicTableNameByDate(String tableName, String dynamicRule) {
        super(tableName, dynamicRule);
    }

    @Override
    public DynamicTableResult genericTableName() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd");
        String dateFormatStr = sdf.format(new Date());
        return DynamicTableResult.of()
                .withDynamicName(String.format("%s_%s", tableName, dateFormatStr))
                .withOldTableName(tableName);
    }
}
