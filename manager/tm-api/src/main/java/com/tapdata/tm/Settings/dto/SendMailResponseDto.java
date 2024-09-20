package com.tapdata.tm.Settings.dto;

import java.io.Serializable;

public class SendMailResponseDto implements Serializable {
    private boolean result;
    private String stack;

    public boolean isResult() {
        return result;
    }

    public String getStack() {
        return stack;
    }

    public SendMailResponseDto(boolean result, String stack) {
        this.result = result;
        this.stack = stack;
    }
}
