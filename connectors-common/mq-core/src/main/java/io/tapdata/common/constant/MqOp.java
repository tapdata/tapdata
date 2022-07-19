package io.tapdata.common.constant;

import java.util.Optional;

public enum MqOp {

    INSERT("insert"),
    UPDATE("update"),
    DELETE("delete"),
    DDL("ddl");

    private final String op;

    MqOp(String op) {
        this.op = op;
    }

    public String getOp() {
        return op;
    }

    public static MqOp fromValue(String value) {
        return Optional.ofNullable(value).map(MqOp::valueOf).orElse(INSERT);
    }
}
