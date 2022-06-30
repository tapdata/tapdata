package com.tapdata.tm.Settings.dto;

import lombok.*;

@AllArgsConstructor
@Getter
@Setter
public class AgentNotificationDto extends  NotificationSettingDto{
    private String label;
    private Boolean notice;
    private Boolean email;
}
