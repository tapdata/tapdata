package com.tapdata.tm.base.dto;

public class SensitiveResponseMessage<T> extends ResponseMessage<T> {
    @Override
    public String toString() {
        return "SensitiveResponseMessage(data=[REDACTED])";
    }
}
