package io.tapdata.common.message;

import io.tapdata.common.constant.MqOp;

public abstract class AbstractMqMessage implements MqMessage {

    private MqOp mqOp;

    private String tableType;

    @Override
    public MqOp getMqOp() {
        return mqOp;
    }

    public void setMqOp(MqOp mqOp) {
        this.mqOp = mqOp;
    }

    @Override
    public void setTableType(String tableType) {
        this.tableType = tableType;
    }

    @Override
    public String getTableType() {
        return this.tableType;
    }
}
