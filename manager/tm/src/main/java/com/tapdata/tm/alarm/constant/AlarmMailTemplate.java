package com.tapdata.tm.alarm.constant;

/**
 * @author jiuyetx
 * @date 2022/9/13
 */
public class AlarmMailTemplate {
    public static final String TASK_STATUS_STOP_ERROR_TITLE = "【Tapdata Notification:Task error】{0}";
    public static final String TASK_STATUS_STOP_ERROR = "An error occurred in your data verification task. \n" +
            "    Task name: {0}\n" +
            "    Error time：{1}\n" +
            "    Error message: {2}";

    public static final String TASK_STATUS_STOP_MANUAL_TITLE = "【Tapdata Notification:Task stopped】{0}";
    public static final String TASK_STATUS_STOP_MANUAL = "Your task has been stopped. \n" +
            "    Task name: {1}\n" +
            "    Stop time: {2}\n" +
            "    Stop reason: 被{3}手动停止";
}
