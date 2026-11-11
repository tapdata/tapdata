package com.tapdata.tm.application.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.commons.base.IDataPermissionDto;
import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.security.oauth2.core.ClientAuthenticationMethod;

import java.util.List;
import java.util.Set;


/**
 * MetadataDefinition
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ApplicationDto extends BaseDto implements IDataPermissionDto {
    private String name;

    private String clientId;
    private String clientKey;
    private String javaScriptKey;
    private String restApiKey;
    private String windowsKey;
    private String masterKey;
    private Boolean authenticationEnabled;
    private Boolean anonymousAllowed;
    private String status;


    private String clientType;
    private String clientName;
    private String clientURI;
    private List grantTypes;
    private String tokenType;

    /**
     * 前端传输组过来，空就传空数组
     */
    private Object responseTypes;
    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    private String clientSecret;

    @JsonProperty(access = JsonProperty.Access.READ_ONLY)
    public boolean isClientSecretConfigured() {
        return clientSecret != null && !clientSecret.isEmpty();
    }
    private List scopes;
    private List scopeNames;
    private List<String> redirectUris;
    private Boolean showMenu;
    private Set<String> clientAuthenticationMethods;
    private String clientSettings;
    private String tokenSettings;

}
