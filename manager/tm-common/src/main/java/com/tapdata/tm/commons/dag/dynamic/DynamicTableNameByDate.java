package com.tapdata.tm.commons.dag.dynamic;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

/**
 * dynamic table name:
 *  Directly add the suffix based on the current time: ${oldTableName}_yyyy_mm_dd
 * @author Gavin'Xiao
 * https://github.com/11000100111010101100111
 * */
class DynamicTableNameByDate extends DynamicTableStage {
    public static final String FORMAT = "%s%s%s";
    public DynamicTableNameByDate(String tableName, DynamicTableConfig dynamicRule) {
        super(tableName, dynamicRule);
    }

    @Override
    public DynamicTableResult genericTableName() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd");
        String dateFormatStr = sdf.format(new Date());
        DynamicTableResult result = DynamicTableResult.of().withOldTableName(tableName);
        String couplingSymbols = Optional.ofNullable(dynamicRule.getCouplingSymbols()).orElse(DEFAULT_COUPLING_SYMBOLS);
        if (CouplingLocation.PREFIX.equals(dynamicRule.getCouplingLocation())) {
            result.withDynamicName(String.format(FORMAT, dateFormatStr, couplingSymbols, tableName));
        } else {
            result.withDynamicName(String.format(FORMAT, tableName, couplingSymbols, dateFormatStr));
        }
        return result;
    }
}
