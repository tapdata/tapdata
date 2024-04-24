package com.tapdata.entity.dataflow;

public enum TableBatchReadStatus {
    /**
     * 表示当前表全量已结束
     * */
    OVER,
    /**
     * 表示当前表全量进行中
     * */
    RUNNING;
}
