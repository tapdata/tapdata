package com.tapdata.tm.commons.task.dto;

import lombok.Data;

import java.io.Serializable;

/**
 * @Author: Zed
 * @Date: 2021/11/11
 * @Description:
 */
@Data
public class DataSyncMq implements Serializable {
    /** 启动 */
    public static final String OP_TYPE_START = "start";
    /** 停止 */
    public static final String OP_TYPE_STOP = "stop";
    /** 重启 */
    public static final String OP_TYPE_RESTART = "restart";

    /** 已启动 */
    public static final String OP_TYPE_STARTED = "started";
    /** 已重启 */
    public static final String OP_TYPE_RESTARTED = "restarted";
    /** 已停止 */
    public static final String OP_TYPE_STOPPED = "stopped";
    /** 错误 */
    public static final String OP_TYPE_ERROR = "error";
    /** 完成 */
    public static final String OP_TYPE_COMPLETE = "complete";

    /** 删除 */
    public static final String OP_TYPE_DELETED = "deleted";
    /** 已删除 */
    public static final String OP_TYPE_DELETE = "delete";
    /** 重置 */
    public static final String OP_TYPE_RESETED = "reseted";
    /** 已经重置 */
    public static final String OP_TYPE_RESET = "reset";

    /**
     * 重置删除状态上报
     */
    public static final String OP_TYPE_RESET_DELETE_REPORT = "reset_delete_report";

    /** 类型 */
    private String type;
    /** 子任务id  */
    private String taskId;
    /** tm下发操作类型   启动 停止 重启   flowengin上发操作类型  已运行，已停止，已重启 运行错误， 运行完成 */
    private String opType;
    /** 是否强制暂停  */
    private boolean force;

    /** 错误信息  */
    private String errMsg;
    /** 错误堆栈 */
    private String errStack;

    private TaskResetEventDto resetEventDto;
}
