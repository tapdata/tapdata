package com.tapdata.tm.lock.constant;

/**
 * @Author: Zed
 * @Date: 2021/12/18
 * @Description:
 */
public enum LockType {
    /** 默认*/
    DEFAULT_LOCK,
    /** 模型推演*/
    TRANSFORM_SCHEMA,

    /** 启动挖掘任务 */
    START_LOG_COLLECTOR,

    /** 启动LDP-FDM任务 */
    START_LDP_FDM,
    ;
}
