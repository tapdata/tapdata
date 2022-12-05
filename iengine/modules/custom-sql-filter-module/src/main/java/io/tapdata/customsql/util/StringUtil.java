package io.tapdata.customsql.util;

import io.tapdata.pdk.apis.entity.QueryOperator;

import java.util.Objects;

public class StringUtil {
    public static boolean compare(QueryOperator queryOperator, Object databaseValue) {
        // queryOperator   1:> 2:>= 3:< 4:<= 5:=
        Object filterValue = queryOperator.getValue();
        int operator = queryOperator.getOperator();
        if(filterValue == null || databaseValue ==null){
            return Objects.equals(filterValue, databaseValue) && (
                    operator == QueryOpertorEnum.GTE.getOp()
                    || operator == QueryOpertorEnum.LTE.getOp()
                    || operator == QueryOpertorEnum.EQL.getOp());

        }
        if (operator == QueryOpertorEnum.GTE.getOp() ||
                operator == QueryOpertorEnum.LTE.getOp()
                || operator == QueryOpertorEnum.EQL.getOp()) {
            if (filterValue.toString().equals(databaseValue.toString())) {
                return true;
            }
        }
        int compareResult = databaseValue.toString().compareTo(filterValue.toString());
        if (compareResult > 0 && (operator == QueryOpertorEnum.GT.getOp()
                || operator == QueryOpertorEnum.GTE.getOp())) {
            return true;
        }
        if (compareResult == 0 && (operator == QueryOpertorEnum.GTE.getOp() ||
                operator == QueryOpertorEnum.LTE.getOp()
                || operator == QueryOpertorEnum.EQL.getOp())) {
            return true;

        }
        return compareResult < 1 && (operator == QueryOpertorEnum.LE.getOp() || operator == QueryOpertorEnum.LTE.getOp());
    }

}
