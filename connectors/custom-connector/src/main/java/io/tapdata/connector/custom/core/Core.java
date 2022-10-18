package io.tapdata.connector.custom.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class Core {

    protected final static int DEFAULT_READ_BATCH_SIZE = 25000;
    public static final String MESSAGE_OPERATION_INSERT = "i";
    public static final String MESSAGE_OPERATION_DELETE = "d";
    public static final String MESSAGE_OPERATION_UPDATE = "u";

    public Core() {

    }

    public void push(List<Object> data) {
        push(data, MESSAGE_OPERATION_INSERT, null);
    }

    public void push(Map<String, Object> data) {
        push(data, MESSAGE_OPERATION_INSERT, null);
    }

    public void push(Map<String, Object> data, String op, Object contextMap) {
        List<Object> dataList = new ArrayList<>();
        dataList.add(data);
        push(dataList, op, contextMap);
    }

    public abstract void push(List<Object> data, String op, Object contextMap);
}
