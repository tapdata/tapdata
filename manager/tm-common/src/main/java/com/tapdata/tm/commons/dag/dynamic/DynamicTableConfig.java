package com.tapdata.tm.commons.dag.dynamic;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;

@EqualsAndHashCode
@ToString
public class DynamicTableConfig implements Serializable {
    /**
     * generate dynamic table name rule type
     */
    private String ruleType;

    private String couplingSymbols;

    private CouplingLocation couplingLocation;

    private Map<String, Object> params;

    public static DynamicTableConfig of() {
        return new DynamicTableConfig();
    }

    public DynamicTableConfig withRuleType(String ruleType) {
        this.ruleType = ruleType;
        return this;
    }

    public DynamicTableConfig withCouplingSymbols(String couplingSymbols) {
        this.couplingSymbols = couplingSymbols;
        return this;
    }

    public DynamicTableConfig withCouplingLocation(CouplingLocation couplingLocation) {
        this.couplingLocation = couplingLocation;
        return this;
    }

    public DynamicTableConfig withParams(Map<String, Object> params) {
        this.params = params;
        return this;
    }

    public void setRuleType(String ruleType) {
        this.ruleType = ruleType;
    }

    public void setCouplingSymbols(String couplingSymbols) {
        this.couplingSymbols = couplingSymbols;
    }

    public void setCouplingLocation(CouplingLocation couplingLocation) {
        this.couplingLocation = couplingLocation;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }

    public String getRuleType() {
        return ruleType;
    }

    public String getCouplingSymbols() {
        return couplingSymbols;
    }

    public CouplingLocation getCouplingLocation() {
        return couplingLocation;
    }

    public Map<String, Object> getParams() {
        return params;
    }
}
