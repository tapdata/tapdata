package com.tapdata.tm.Settings.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;


@Data
public class    TestMailDto {
    //private List<SettingsDto> settingsDto;
    @JsonProperty("SMTP_Server_Host")
    private String SMTP_Server_Host;
    @JsonProperty("Email_Communication_Protocol")
    private String Email_Communication_Protocol;
    @JsonProperty("SMTP_Server_Port")
    private String SMTP_Server_Port;
    @JsonProperty("Email_Send_Address")
    private String Email_Send_Address;
    @JsonProperty("SMTP_Server_User")
    private String SMTP_Server_User;
    @JsonProperty("SMTP_Server_password")
    private String SMTP_Server_password;
    @JsonProperty("Email_Receivers")
    private String Email_Receivers;
    private String title;
    private String text;
}
