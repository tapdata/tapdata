package io.tapdata.common.constant;

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
        for (MqOp mqOp : MqOp.values()) {
            if (mqOp.getOp().equals(value)) {
                return mqOp;
            }
        }
        return INSERT;
    }
}
