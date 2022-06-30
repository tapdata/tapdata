package com.tapdata.tm.Settings.dto;


import lombok.Data;

import java.util.List;

@Data
public class NotificationSettingDto {
    NotificationSettingEnum notificationSettingEnum;

    List<RunNotificationDto> runNotification;
    List<SystemNotificationDto> systemNotification;
    List<SystemNotificationDto> agentNotification;


    private String value;


    public enum NotificationSettingEnum {
        RUN_NOTIFICATION("runNotification"),
        SYSTEM_NOTIFICATION("systemNotification"),
        AGENT_NOTIFICATION("agentNotification");

        private String value;

        NotificationSettingEnum(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }



}
