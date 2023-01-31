package io.tapdata.coding.enums;

public enum IterationStatus {
    WAIT_PROCESS("WAIT_PROCESS"),
    PROCESSING("PROCESSING"),
    COMPLETED("COMPLETED");

    private String status;

    IterationStatus(String status) {
        this.status = status;
    }

    public String status() {
        return this.status;
    }
}
