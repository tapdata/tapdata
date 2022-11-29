package io.tapdata.customsql.util;

public class StringUtil {
    public static boolean compare(Object filterValue, Object databaseValue, int queryOperator) {
        // queryOperator   1:> 2:>= 3:< 4:<= 5:=
        if (queryOperator == QueryOpertorEnum.GTE.getOp() ||
                queryOperator == QueryOpertorEnum.LTE.getOp()
                || queryOperator == QueryOpertorEnum.EQL.getOp()) {
            if (filterValue.toString().equals(databaseValue.toString())) {
                return true;
            }
        }
        int compareResult = filterValue.toString().compareTo(databaseValue.toString());
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

    public static void main(String[] args) {
        System.out.println(StringUtil.compare("222","222",5));  ;
    }

}
