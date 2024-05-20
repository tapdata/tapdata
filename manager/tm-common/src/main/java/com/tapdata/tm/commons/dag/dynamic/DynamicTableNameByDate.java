package com.tapdata.tm.commons.dag.dynamic;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * dynamic table name:
 *  Directly add the suffix based on the current time:
 *      [${PREFIX}${couplingSymbols}]${oldTableName}[${couplingSymbols}${SUFFIX}]
 *      default couplingSymbols: _
 *      default couplingLocation: SUFFIX
 *      default dynamic char: yyyy_MM_dd
 * @author Gavin'Xiao
 * https://github.com/11000100111010101100111
 * */
class DynamicTableNameByDate extends DynamicTableStage {
    public static final String DATE_FORMAT = "yyyy_MM_dd";
    public DynamicTableNameByDate(String tableName, DynamicTableConfig dynamicRule) {
        super(tableName, dynamicRule);
    }

    @Override
    public DynamicTableResult genericTableName() {
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
        String dateFormatStr = sdf.format(new Date());
        return DynamicTableResult.of()
                .withOldTableName(tableName)
                .withDynamicName(genericWithCouplingLocationAndCouplingSymbols(dateFormatStr));
    }
}
