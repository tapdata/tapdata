package com.tapdata.tm.apiCalls.enums;

public enum TimeGranularityType {
    MINUTE(1, "minute"),
    HOUR(2, "hour"),
    DAY(3, "day")
    ;
    final int code;
    final String msg;

    TimeGranularityType(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public int getCode() {
        return code;
    }

    public String getMsg() {
        return msg;
    }
}
