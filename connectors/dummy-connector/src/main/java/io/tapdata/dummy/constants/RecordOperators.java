package io.tapdata.dummy.constants;

public enum RecordOperators {
    Insert("i"), Update("u"), Delete("d"),
    ;

    private String op;

    RecordOperators(String op) {
        this.op = op;
    }

    public String getOp() {
        return op;
    }
}
