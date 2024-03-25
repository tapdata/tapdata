package com.tapdata.tm.Settings.constant;

public enum CategoryEnum {
    NOTIFICATION("notification"),
    SYSTEM("System"),
    SMTP("SMTP"),
    FRONTEND("Frontend"),
    JOB("Job"),
    WORKER("Worker")
    ;

    private String value;

    CategoryEnum(String value) {
        this.value = value;
    }


    public String getValue() {
        return value;
    }
}
