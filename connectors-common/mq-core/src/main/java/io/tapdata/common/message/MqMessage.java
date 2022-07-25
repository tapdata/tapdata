package io.tapdata.common.message;

import io.tapdata.common.constant.MqOp;

public interface MqMessage {

    MqOp getMqOp();

    void setTableType(String tableType);

    String getTableType();
}
