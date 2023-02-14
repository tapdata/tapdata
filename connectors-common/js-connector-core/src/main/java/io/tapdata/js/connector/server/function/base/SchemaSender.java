package io.tapdata.js.connector.server.function.base;

import io.tapdata.entity.schema.TapTable;

import java.util.List;
import java.util.function.Consumer;

public interface SchemaSender {
    public void send(Object schemaObj);

    public void setConsumer(Consumer<List<TapTable>> consumer);
}
