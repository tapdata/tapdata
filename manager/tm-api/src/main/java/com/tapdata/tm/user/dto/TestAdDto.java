package com.tapdata.tm.user.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class TestAdDto {
    @JsonProperty("AD_Server_Host")
    private String AD_Server_Host;
    @JsonProperty("AD_Server_Port")
    private String AD_Server_Port;
    @JsonProperty("AD_Base_DN")
    private String AD_Base_DN;
    @JsonProperty("AD_Bind_DN")
    private String AD_Bind_DN;
    @JsonProperty("AD_Bind_Password")
    private String AD_Bind_Password;
    @JsonProperty("AD_SSL_Enable")
    private String AD_SSL_Enable;
}
