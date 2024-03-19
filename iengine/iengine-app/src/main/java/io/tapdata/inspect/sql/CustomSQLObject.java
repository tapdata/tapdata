package io.tapdata.inspect.sql;

public interface CustomSQLObject<T, V> {
    V execute(T entity, Object functionObj);

    String getFunctionName();
}
