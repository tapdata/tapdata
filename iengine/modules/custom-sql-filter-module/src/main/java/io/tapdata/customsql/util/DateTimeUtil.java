package io.tapdata.customsql.util;

import io.tapdata.entity.codec.impl.utils.AnyTimeToDateTime;
import io.tapdata.entity.schema.value.DateTime;
import io.tapdata.pdk.apis.entity.QueryOperator;

import java.util.Objects;

public class DateTimeUtil {

    public static boolean compare(QueryOperator queryOperator, Object databaseValue) {
        // queryOperator   1:> 2:>= 3:< 4:<= 5:=
        Object filterValue = queryOperator.getValue();
        int operator = queryOperator.getOperator();
        DateTime filterDateTime = AnyTimeToDateTime.toDateTime(filterValue);
        DateTime databaseDateTime = AnyTimeToDateTime.toDateTime(databaseValue);
        if (filterDateTime == null || databaseDateTime == null) {
            return Objects.equals(filterDateTime, databaseDateTime) && (
                    operator == QueryOpertorEnum.GTE.getOp()
                            || operator == QueryOpertorEnum.LTE.getOp()
                            || operator == QueryOpertorEnum.EQL.getOp());
        }
        if (databaseDateTime.getSeconds() >filterDateTime.getSeconds()  && (operator == QueryOpertorEnum.GT.getOp()
                || operator == QueryOpertorEnum.GTE.getOp())) {
            return true;

        } else if (Objects.equals(filterDateTime.getSeconds(), databaseDateTime.getSeconds())) {
            if (databaseDateTime.getNano() > filterDateTime.getNano() && (operator == QueryOpertorEnum.GT.getOp()
                    || operator == QueryOpertorEnum.GTE.getOp())) {
                return true;
            } else if (Objects.equals(filterDateTime.getNano(), databaseDateTime.getNano()) &&
                    (operator == QueryOpertorEnum.GTE.getOp() || operator == QueryOpertorEnum.LTE.getOp()
                            || operator == QueryOpertorEnum.EQL.getOp())) {
                return true;
            } else return databaseDateTime.getNano() < filterDateTime.getNano()
                    && (operator == QueryOpertorEnum.LE.getOp() || operator == QueryOpertorEnum.LTE.getOp());
        } else return databaseDateTime.getSeconds() < filterDateTime.getSeconds() &&
                (operator == QueryOpertorEnum.LE.getOp() || operator == QueryOpertorEnum.LTE.getOp());

    }


}
