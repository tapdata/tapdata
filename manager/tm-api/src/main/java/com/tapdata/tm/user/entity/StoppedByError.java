package com.tapdata.tm.user.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class StoppedByError {
    private Boolean email = false;
    private Boolean sms = false;
    private Boolean weChat = false;
}
