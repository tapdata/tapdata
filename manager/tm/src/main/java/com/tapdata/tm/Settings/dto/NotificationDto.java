package com.tapdata.tm.Settings.dto;

import lombok.Data;

@Data
public class NotificationDto {
    private String label;
    private Boolean notice;
    private Boolean email;
    private Boolean sms;
}
