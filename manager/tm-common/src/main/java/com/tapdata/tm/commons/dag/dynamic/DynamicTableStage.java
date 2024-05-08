package com.tapdata.tm.commons.dag.dynamic;

abstract class DynamicTableStage {
    public static final String DEFAULT_COUPLING_SYMBOLS = "_";
    protected String tableName;
    protected DynamicTableConfig dynamicRule;
    public DynamicTableStage(String tableName, DynamicTableConfig dynamicRule) {
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
}
