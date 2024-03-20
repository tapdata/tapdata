package io.tapdata.mongodb.decoder;

public interface CustomSQLObject<V, C> {
    V execute(Object functionObj, C curMap);

    String getFunctionName();
}
