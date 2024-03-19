package io.tapdata.inspect.sql;

public interface CustomSQLObject<V, C> {
    V execute(Object functionObj, C curMap);

    String getFunctionName();
}
