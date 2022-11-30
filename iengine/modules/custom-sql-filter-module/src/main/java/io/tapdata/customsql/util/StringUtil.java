package io.tapdata.customsql.util;

import java.util.Objects;

public class StringUtil {
    public static boolean compare(Object filterValue, Object databaseValue, int queryOperator) {
        // queryOperator   1:> 2:>= 3:< 4:<= 5:=

        if(filterValue == null || databaseValue ==null){
            return Objects.equals(filterValue, databaseValue) && (
                     queryOperator == QueryOpertorEnum.GTE.getOp()
                    || queryOperator == QueryOpertorEnum.LTE.getOp()
                    || queryOperator == QueryOpertorEnum.EQL.getOp());

        }
        if (queryOperator == QueryOpertorEnum.GTE.getOp() ||
                queryOperator == QueryOpertorEnum.LTE.getOp()
                || queryOperator == QueryOpertorEnum.EQL.getOp()) {
            if (filterValue.toString().equals(databaseValue.toString())) {
                return true;
            }
        }
        int compareResult = databaseValue.toString().compareTo(filterValue.toString());
        if (compareResult > 0 && (queryOperator == QueryOpertorEnum.GT.getOp()
                || queryOperator == QueryOpertorEnum.GTE.getOp())) {
            return true;
        }
        if (compareResult == 0 && (queryOperator == QueryOpertorEnum.GTE.getOp() ||
                queryOperator == QueryOpertorEnum.LTE.getOp()
                || queryOperator == QueryOpertorEnum.EQL.getOp())) {
            return true;

        }
        return compareResult < 1 && (queryOperator == QueryOpertorEnum.LE.getOp() || queryOperator == QueryOpertorEnum.LTE.getOp());
    }

}
