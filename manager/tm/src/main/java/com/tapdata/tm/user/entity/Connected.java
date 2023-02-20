package com.tapdata.tm.user.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Connected {
    private Boolean email=false;
    private Boolean sms=false;
}
