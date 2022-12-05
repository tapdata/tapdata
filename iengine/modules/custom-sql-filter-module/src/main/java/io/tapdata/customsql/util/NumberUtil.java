package io.tapdata.customsql.util;

import io.tapdata.pdk.apis.entity.QueryOperator;

import java.math.BigDecimal;
import java.util.Objects;

public class NumberUtil {


    public static boolean compare(QueryOperator queryOperator, Object databaseValue) {
        // operator   1:> 2:>= 3:< 4:<= 5:=
        Object filterValue = queryOperator.getValue();
        int operator = queryOperator.getOperator();
        BigDecimal filterBigDecimal = new BigDecimal(filterValue.toString());
        BigDecimal databaseBigDecimal = new BigDecimal(databaseValue.toString());
        if (filterValue == null || databaseValue == null) {
            return Objects.equals(filterBigDecimal, filterBigDecimal) && (
                    operator == QueryOpertorEnum.GTE.getOp()
                            || operator == QueryOpertorEnum.LTE.getOp()
                            || operator == QueryOpertorEnum.EQL.getOp());
        }
        int compareResult = databaseBigDecimal.compareTo(filterBigDecimal);
        if (compareResult > 0 && (operator == QueryOpertorEnum.GT.getOp()
                || operator == QueryOpertorEnum.GTE.getOp())) {
            return true;
        }
        if (compareResult == 0 && (operator == QueryOpertorEnum.GTE.getOp() ||
                operator == QueryOpertorEnum.LTE.getOp()
                || operator == QueryOpertorEnum.EQL.getOp())) {
            return true;

        }
        if (compareResult < 1 && (operator == QueryOpertorEnum.LE.getOp() ||
                operator == QueryOpertorEnum.LTE.getOp())) {
            return true;

        }
        return false;
    }


}
