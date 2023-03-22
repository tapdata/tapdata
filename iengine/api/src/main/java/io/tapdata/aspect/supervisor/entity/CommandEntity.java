package io.tapdata.aspect.supervisor.entity;

import io.tapdata.entity.utils.DataMap;

public class CommandEntity extends ConnectionTestEntity{
    String command;
    @Override
    public DataMap summery() {
        return super.summery()
                .kv(MODE_KEY,"command")
                .kv("databaseType",databaseType)
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
