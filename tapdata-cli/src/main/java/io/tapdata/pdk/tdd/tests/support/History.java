package io.tapdata.pdk.tdd.tests.support;

public class History {
    public static final String ERROR = "ERROR";
    public static final String WARN = "WARN";
    public static final String SUCCEED = "SUCCEED";
    String tag;
    String message;

    public History(String tag, String msg) {
        this.message = msg;
        this.tag = tag;
    }

    public static History error(String msg) {
        return new History(ERROR, msg);
    }

    public static History warn(String msg) {
        return new History(WARN, msg);
    }

    public static History succeed(String msg) {
        return new History(SUCCEED, msg);
    }

    public String tag() {
        return this.tag;
    }

    public String message() {
        return this.message;
    }
}