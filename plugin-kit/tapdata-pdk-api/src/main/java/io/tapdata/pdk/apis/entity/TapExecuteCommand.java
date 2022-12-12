package io.tapdata.pdk.apis.entity;

public class TapExecuteCommand {
    private String command;
    public TapExecuteCommand command(String command) {
        this.command = command;
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
}