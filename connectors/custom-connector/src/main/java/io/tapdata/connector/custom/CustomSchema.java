package io.tapdata.connector.custom;

import io.tapdata.connector.custom.config.CustomConfig;
import io.tapdata.entity.schema.TapTable;

public class CustomSchema {

    private CustomConfig customConfig;

    public CustomSchema(CustomConfig customConfig) {
        this.customConfig = customConfig;
    }

    public TapTable loadSchema() {
        return null;
    }
}
