package com.tapdata.tm.Settings.dto;

import java.io.Serializable;

public class TestResponseDto implements Serializable {
    private boolean result;
    private String stack;

    public boolean isResult() {
        return result;
    }

    public String getStack() {
        return stack;
    }

    public TestResponseDto(boolean result, String stack) {
        this.result = result;
        this.stack = stack;
    }
}
