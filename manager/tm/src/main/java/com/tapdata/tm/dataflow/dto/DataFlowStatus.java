package com.tapdata.tm.dataflow.dto;

/**
 * @Author: Zed
 * @Date: 2021/10/22
 * @Description: dataflow 的状态常量
 */
public enum DataFlowStatus {
    error("error"),
    scheduled("scheduled"),
    paused("paused"),
    force_stopping("force stopping"),
    stopping("stopping"),
    draft("draft"),
    running("running"),
    ;


    DataFlowStatus(String v) {
        this.v = v;
    }

    public String v;



}
