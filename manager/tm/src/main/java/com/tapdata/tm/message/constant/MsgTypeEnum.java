package com.tapdata.tm.message.constant;


public enum MsgTypeEnum {
    CONNECTED("connected"),
    CONNECTION_INTERRUPTED("connectionInterrupted"),
    WILL_RELEASE_AGENT("willReleaseAgent"),
    RELEASE_AGENT("releaseAgent"),

    STARTED("started"),
    DELETED("deleted"),
    COMPLETED("complete"),
    PAUSED("paused"),
    ENCOUNT_ERERROR_SKIPPED("encounterERRORSkipped"),

    ERROR("error"),

    //校验 任务运行error
    INSPECT_ERROR("inspectError"),
    //校验 任务运行error
    INSPECT_COUNT("inspectCount"),

    //校验内容有差异
    INSPECT_VALUE("inspectValue"),

    //校验内容有差异
    CDCLag("CDCLag"),

    STOPPED_BY_ERROR("stoppedByError"),

    ALARM("alarm");


    private final String value;

    // 构造方法
    private MsgTypeEnum(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static MsgTypeEnum getEnumByValue(String value) {
        MsgTypeEnum[] msgTypeEnum = values();
        for (MsgTypeEnum businessModeEnum : msgTypeEnum) {
            if (businessModeEnum.getValue().equals(value)) {
                return businessModeEnum;
            }
        }
        return null;
    }


}
