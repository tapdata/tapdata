package com.tapdata.tm.user.entity;

import lombok.Data;

@Data
public class ConnectionInterrupted {
    private Boolean email=false;
    private Boolean sms=false;

    @Override
    public String toString() {
        return "ConnectionInterrupted";
    }
}
