package com.tapdata.tm.commons.dag.dynamic;

import java.util.Optional;

abstract class DynamicTableStage {
    public static final String FORMAT = "%s%s%s";
    public static final String DEFAULT_COUPLING_SYMBOLS = "_";
    protected String tableName;
    protected DynamicTableConfig dynamicRule;

    protected DynamicTableStage(String tableName, DynamicTableConfig dynamicRule) {
        this.tableName = tableName;
        if (null == dynamicRule) {
            dynamicRule = DynamicTableConfig.of()
                    .withRuleType(DynamicTableRule.DEFAULT.name)
                    .withCouplingLocation(CouplingLocation.SUFFIX)
                    .withCouplingSymbols(DEFAULT_COUPLING_SYMBOLS);
        }
        this.dynamicRule = dynamicRule;
    }

    abstract DynamicTableResult genericTableName();

    protected String genericWithCouplingLocationAndCouplingSymbols(String dynamicChar) {
        String couplingSymbols = Optional.ofNullable(dynamicRule.getCouplingSymbols()).orElse(DEFAULT_COUPLING_SYMBOLS);
        if (CouplingLocation.PREFIX.equals(dynamicRule.getCouplingLocation())) {
            return String.format(FORMAT, dynamicChar, couplingSymbols, tableName);
        } else {
            return String.format(FORMAT, tableName, couplingSymbols, dynamicChar);
        }
    }
}
