package io.tapdata.customsql.util;

import java.math.BigDecimal;

public class NumberUtil {


    public static boolean compare(Object filterValue, Object databaseValue, int queryOperator) {
        // queryOperator   1:> 2:>= 3:< 4:<= 5:=
        BigDecimal filterBigDecimal = new BigDecimal(filterValue.toString());
        BigDecimal databaseBigDecimal = new BigDecimal(databaseValue.toString());
        int compareResult = filterBigDecimal.compareTo(databaseBigDecimal);
        if (compareResult > 0 && (queryOperator == QueryOpertorEnum.GT.getOp()
                || queryOperator == QueryOpertorEnum.GTE.getOp())) {
            return true;
        }
        if (compareResult == 0 && (queryOperator == QueryOpertorEnum.GTE.getOp() ||
                queryOperator == QueryOpertorEnum.LTE.getOp()
                || queryOperator == QueryOpertorEnum.EQL.getOp())) {
            return true;

        }
        if (compareResult < 1 && (queryOperator == QueryOpertorEnum.LE.getOp() || queryOperator == QueryOpertorEnum.LTE.getOp())) {
            return true;

        }
        return false;
    }


}
