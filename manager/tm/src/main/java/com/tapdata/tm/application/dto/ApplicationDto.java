package com.tapdata.tm.application.dto;

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
public class ApplicationDto extends BaseDto {
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
    private String clientSecret;
    private List scopes;
    private List<String> redirectUris;
    private Boolean showMenu;
    private Set<String> clientAuthenticationMethods;

}
