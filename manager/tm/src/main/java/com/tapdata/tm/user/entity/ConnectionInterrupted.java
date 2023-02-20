package com.tapdata.tm.user.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ConnectionInterrupted {
    private Boolean email=false;
    private Boolean sms=false;

    @Override
    public String toString() {
        return "ConnectionInterrupted";
    }
}
