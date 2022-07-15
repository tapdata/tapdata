package io.tapdata.oceanbase.util;

import io.tapdata.pdk.apis.entity.QueryOperator;

import java.util.HashMap;
import java.util.Map;

/**
 * @author dayun
 * @date 2022/6/28 19:59
 */
public enum QueryOperatorEnum {
    GT(QueryOperator.GT, ">"), GTE(QueryOperator.GTE, ">="), LE(QueryOperator.LT, "<"), LTE(QueryOperator.LTE, "<="),
    ;

    private int op;
    private String opStr;

    QueryOperatorEnum(int op, String opStr) {
        this.op = op;
        this.opStr = opStr;
    }

    public String getOpStr() {
        return opStr;
    }

    private static Map<String, QueryOperatorEnum> opMap;

    static {
        opMap = new HashMap<>();
        for (QueryOperatorEnum queryOperatorEnum : QueryOperatorEnum.values()) {
            opMap.put(String.valueOf(queryOperatorEnum.op), queryOperatorEnum);
        }
    }

    public static QueryOperatorEnum fromOp(int op) {
        return opMap.get(String.valueOf(op));
    }
}
