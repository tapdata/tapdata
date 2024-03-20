package io.tapdata.mongodb.decoder.sql;

public interface CustomSQLObject<V, C> {
    V execute(Object functionObj, C curMap);

    String getFunctionName();
}
