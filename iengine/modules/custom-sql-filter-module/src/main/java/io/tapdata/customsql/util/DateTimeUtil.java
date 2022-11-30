package io.tapdata.customsql.util;

import io.tapdata.entity.codec.impl.utils.AnyTimeToDateTime;
import io.tapdata.entity.schema.value.DateTime;

import java.util.Objects;

public class DateTimeUtil {

    public static boolean compare(Object filterValue, Object databaseValue, int queryOperator) {
        // queryOperator   1:> 2:>= 3:< 4:<= 5:=
        DateTime filterDateTime = AnyTimeToDateTime.toDateTime(filterValue);
        DateTime databaseDateTime = AnyTimeToDateTime.toDateTime(databaseValue);
        if (filterDateTime == null || databaseDateTime == null) {
            return Objects.equals(filterDateTime, databaseDateTime) && (
                    queryOperator == QueryOpertorEnum.GTE.getOp()
                            || queryOperator == QueryOpertorEnum.LTE.getOp()
                            || queryOperator == QueryOpertorEnum.EQL.getOp());
        }
        if (databaseDateTime.getSeconds() >filterDateTime.getSeconds()  && (queryOperator == QueryOpertorEnum.GT.getOp()
                || queryOperator == QueryOpertorEnum.GTE.getOp())) {
            return true;

        } else if (Objects.equals(filterDateTime.getSeconds(), databaseDateTime.getSeconds())) {
            if (databaseDateTime.getNano() > filterDateTime.getNano() && (queryOperator == QueryOpertorEnum.GT.getOp()
                    || queryOperator == QueryOpertorEnum.GTE.getOp())) {
                return true;
            } else if (Objects.equals(filterDateTime.getNano(), databaseDateTime.getNano()) &&
                    (queryOperator == QueryOpertorEnum.GTE.getOp() || queryOperator == QueryOpertorEnum.LTE.getOp()
                            || queryOperator == QueryOpertorEnum.EQL.getOp())) {
                return true;
            } else return databaseDateTime.getNano() < filterDateTime.getNano()
                    && (queryOperator == QueryOpertorEnum.LE.getOp() || queryOperator == QueryOpertorEnum.LTE.getOp());
        } else return databaseDateTime.getSeconds() < filterDateTime.getSeconds() &&
                (queryOperator == QueryOpertorEnum.LE.getOp() || queryOperator == QueryOpertorEnum.LTE.getOp());

    }


}
