package io.tapdata.aspect.supervisor.entity;

import io.tapdata.entity.utils.DataMap;

public class CommandEntity extends ConnectionTestEntity{
    String command;
    @Override
    public DataMap summary() {
        return super.summary()
                .kv(MODE_KEY,"command")
                .kv("connectorType",databaseType)
                .kv("name",connectionName)
                .kv("type",type)
                .kv("time",time)
                .kv("pdkHash",pdkHash)
                .kv("command",command)
                .kv("associateId",associateId);
    }

    public CommandEntity command(String command){
        this.command = command;
        return this;
    }
}
