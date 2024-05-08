package com.tapdata.tm.commons.dag.dynamic;

import com.tapdata.tm.error.TapDynamicTableNameExCode_1;
import io.tapdata.exception.TapCodeException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class DynamicTableNameUtil {
    private DynamicTableNameUtil(){}

    public static DynamicTableResult getDynamicTable(String tableName, DynamicTableConfig rule) {
        DynamicTableRule dynamicTableRule = null == rule ? DynamicTableRule.DEFAULT : DynamicTableRule.getRule(rule.getRuleType());
        Class<? extends DynamicTableStage> stage = dynamicTableRule.stage;
        try {
            Constructor<? extends DynamicTableStage> constructor = stage.getConstructor(String.class, DynamicTableConfig.class);
            DynamicTableStage dynamicTableStage = constructor.newInstance(tableName, rule);
            return dynamicTableStage.genericTableName();
        } catch (InstantiationException e) {
            throw new TapCodeException(TapDynamicTableNameExCode_1.UN_SUPPORT_DYNAMIC_RULE, "Fail to get instantiation for " + dynamicTableRule.name + ", message: " + e.getMessage());
        } catch (InvocationTargetException e) {
            throw new TapCodeException(TapDynamicTableNameExCode_1.UN_SUPPORT_DYNAMIC_RULE, "Fail to get invocation for "+ dynamicTableRule.name + ", message: " + e.getMessage());
        } catch (NoSuchMethodException e) {
            throw new TapCodeException(TapDynamicTableNameExCode_1.UN_SUPPORT_DYNAMIC_RULE, "No such method: " + stage.getName() + ".genericTableName()");
        } catch (IllegalAccessException e) {
            throw new TapCodeException(TapDynamicTableNameExCode_1.UN_SUPPORT_DYNAMIC_RULE, "Can not access method: " + stage.getName() + ".genericTableName()");
        }
    }
}
