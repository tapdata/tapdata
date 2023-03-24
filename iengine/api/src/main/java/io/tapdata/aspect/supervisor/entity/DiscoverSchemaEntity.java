package io.tapdata.aspect.supervisor.entity;

import io.tapdata.entity.utils.DataMap;

public class DiscoverSchemaEntity extends ConnectionTestEntity{
    @Override
    public DataMap summary() {
        return super.summary()
                .kv(MODE_KEY,"discover-schema");
    }
}
