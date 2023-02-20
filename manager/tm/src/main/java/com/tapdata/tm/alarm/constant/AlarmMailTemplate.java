package com.tapdata.tm.alarm.constant;

/**
 * @author jiuyetx
 * @date 2022/9/13
 */
public class AlarmMailTemplate {
    public static final String TASK_STATUS_STOP_ERROR_TITLE = "【Tapdata Notification:Task error】{0}";
    public static final String TASK_STATUS_STOP_ERROR = "An error occurred in your data verification task. \n" +
            "    Task name: {0}\n" +
            "    Error time：{1}\n";

    public static final String TASK_STATUS_STOP_MANUAL_TITLE = "【Tapdata Notification:Task stopped】{0}";
    public static final String TASK_STATUS_STOP_MANUAL = "Your task has been stopped. \n" +
            "    Task name: {0}\n" +
            "    Stop time: {1}\n" +
            "    Stop reason: 被{2}手动停止";

    public static final String TASK_FULL_COMPLETE_TITLE = "【Tapdata Notification:Initial sync complete】{0}";
    public static final String TASK_FULL_COMPLETE = "Your task initial sync has been completed. \n" +
            "    Task name: {0}\n" +
            "    Completion time:{1}";

    public static final String TASK_INCREMENT_START_TITLE = "【Tapdata Notification:Start CDC】{0}";
    public static final String TASK_INCREMENT_START = "Your task Start CDC. \n" +
            "    Task name: {0}\n" +
            "    CDC time: {1}";

    public static final String TASK_INCREMENT_DELAY_START_TITLE = "【Tapdata Notification: CDC delay】{0}";
    public static final String TASK_INCREMENT_DELAY_START = "Your task CDC delay. \n" +
            "    Task name: {0}\n" +
            "    Delay times: {1}ms";

    public static final String DATANODE_CANNOT_CONNECT_TITLE = "【Tapdata Notification: Connection cannot be connected】{0}";
    public static final String DATANODE_CANNOT_CONNECT = "The connection used by your task cannot be connected.\n" +
            "    Task name: {0}\n" +
            "    Connection name: {1}\n" +
            "    Occurred Time: {2}";

    public static final String AVERAGE_HANDLE_CONSUME_TITLE = "【Tapdata Notification: Average processing time exceeds threshold】{0}";
    public static final String AVERAGE_HANDLE_CONSUME = "The node current average processing time exceeds threshold.\n" +
            "    Task name: {0}\n" +
            "    Node name: {1}\n" +
            "    Current: {2}ms\n" +
            "    Threshold: {3}ms\n" +
            "    Occurred Time: {4}";
}
