package com.tapdata.tm.alarm.dto;

import com.tapdata.tm.commons.task.constant.NotifyEnum;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SystemNotificationStatusItemDto {
    private String key;
    private boolean open;
    private List<NotifyEnum> channels;
}
