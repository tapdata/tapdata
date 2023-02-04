package io.tapdata.coding.entity.param;

public class Param {
    //公共参数
    String action;
    //	偏移量，默认 0
    int offset;
    //每页数量，默认 20
    int limit;

    public int offset() {
        return offset;
    }

    public String action() {
        return action;
    }

    public int limit() {
        return limit;
    }

    public Param offset(int offset) {
        this.offset = offset;
        return this;
    }

    public Param action(String action) {
        this.action = action;
        return this;
    }

    public Param limit(int limit) {
        this.limit = limit;
        return this;
    }
}
