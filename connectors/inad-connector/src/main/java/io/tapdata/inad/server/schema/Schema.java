package io.tapdata.inad.server.schema;

import io.tapdata.entity.schema.TapTable;

public interface Schema {
    public TapTable schema();
    public String tableName();
}
