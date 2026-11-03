package com.tapdata.tm.alarm.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Collections;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SystemNotificationStatusDto {
    private boolean supported;
    private List<SystemNotificationStatusItemDto> items;

    public static SystemNotificationStatusDto unsupported() {
        return new SystemNotificationStatusDto(false, Collections.emptyList());
    }
}
