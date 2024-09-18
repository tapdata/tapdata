package com.tapdata.tm.user.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class TestLdapDto {
    @JsonProperty("Ldap_Server_Host")
    private String Ldap_Server_Host;
    @JsonProperty("Ldap_Server_Port")
    private String Ldap_Server_Port;
    @JsonProperty("Ldap_Base_DN")
    private String Ldap_Base_DN;
    @JsonProperty("Ldap_Bind_DN")
    private String Ldap_Bind_DN;
    @JsonProperty("Ldap_Bind_Password")
    private String Ldap_Bind_Password;
    @JsonProperty("Ldap_SSL_Enable")
    private Boolean Ldap_SSL_Enable;
}
