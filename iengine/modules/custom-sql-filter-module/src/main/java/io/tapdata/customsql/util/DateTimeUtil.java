package io.tapdata.customsql.util;

import io.tapdata.entity.schema.value.DateTime;

import java.util.Date;

public class DateTimeUtil {

    public static boolean compare(Object filterValue, Object databaseValue, int queryOperator) {
        // queryOperator   1:> 2:>= 3:< 4:<= 5:=
        DateTime filterDateTime = new DateTime((Date) filterValue);
        DateTime databaseDateTime = new DateTime((Date) databaseValue);
        if (filterDateTime.getSeconds() > databaseDateTime.getSeconds() && (queryOperator == QueryOpertorEnum.GT.getOp()
                || queryOperator == QueryOpertorEnum.GTE.getOp())) {
            return true;

        } else if (filterDateTime.getSeconds() == databaseDateTime.getSeconds()) {
            if (filterDateTime.getNano() > databaseDateTime.getNano() && (queryOperator == QueryOpertorEnum.GT.getOp()
                    || queryOperator == QueryOpertorEnum.GTE.getOp())) {
                return true;
            } else if (filterDateTime.getNano() == databaseDateTime.getNano() &&
                    (queryOperator == QueryOpertorEnum.GTE.getOp() || queryOperator == QueryOpertorEnum.LTE.getOp()
                            || queryOperator == QueryOpertorEnum.EQL.getOp())) {
                return true;
            } else if (filterDateTime.getNano() < databaseDateTime.getNano()
                    && (queryOperator == QueryOpertorEnum.LE.getOp() || queryOperator == QueryOpertorEnum.LTE.getOp())) {
                return true;
            }
        } else if (filterDateTime.getSeconds() < databaseDateTime.getSeconds() &&
                (queryOperator == QueryOpertorEnum.LE.getOp() || queryOperator == QueryOpertorEnum.LTE.getOp())){
            return true;

        }
        return false;

    }

}
