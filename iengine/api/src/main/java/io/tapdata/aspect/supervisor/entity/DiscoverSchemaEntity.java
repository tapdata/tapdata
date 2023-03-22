package io.tapdata.aspect.supervisor.entity;

import io.tapdata.entity.utils.DataMap;

public class DiscoverSchemaEntity extends ConnectionTestEntity{
    @Override
    public DataMap summery() {
        return super.summery()
                .kv(MODE_KEY,"discover-schema");
    }
}
