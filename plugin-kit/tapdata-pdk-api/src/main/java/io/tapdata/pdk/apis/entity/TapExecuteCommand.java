package io.tapdata.pdk.apis.entity;

import java.util.Map;

public class TapExecuteCommand {
    private String command;

    private Map<String, Object> params;

    public TapExecuteCommand command(String command) {
        this.command = command;
        return this;
    }

    public TapExecuteCommand params(Map<String, Object> params) {
        this.params = params;
        return this;
    }
    private Integer batchSize;
    public TapExecuteCommand batchSize(Integer batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public static TapExecuteCommand create() {
        return new TapExecuteCommand();
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }
}