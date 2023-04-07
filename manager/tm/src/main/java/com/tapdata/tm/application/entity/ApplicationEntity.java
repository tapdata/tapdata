package com.tapdata.tm.application.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.security.oauth2.core.ClientAuthenticationMethod;

import java.util.List;
import java.util.Set;


/**
 * {
 * _id: 9fe81885d337d7c6b225822c04f89a87,
 * name: ldw_client,
 * clientKey: 6f617cce3fa2488b1a7ad1109e91b784e166cfea,
 * javaScriptKey: c29c4baf1fa1c42121961a52fb7b763a9a7dfc86,
 * restApiKey: 1ff9f5f95c296ef6b365d7c473ae308b92702d56,
 * windowsKey: e2b3e22b366a790554feec32e5928182c11b9128,
 * masterKey: 9cb465c9194e1bcdc99600d1ca8be6eecc50ce52,
 * authenticationEnabled: true,
 * anonymousAllowed: true,
 * status: sandbox,
 * created: {
 * $date: 2021-11-30T06:43:44.894Z
 * },
 * modified: {
 * $date: 2021-11-30T06:43:44.894Z
 * },
 * clientType: "",
 * clientName: ldw_client,
 * clientURI: "",
 * grantTypes: [implicit, client_credentials, refresh_token],
 * tokenType: "",
 * responseTypes: "",
 * clientSecret: 11112,
 * scopes: [$everyone, 新用户默认角色, admin],
 * redirectURIs: /ldw_redirect,
 * showMenu: true,
 * user_id: 61306d94725cec27ed3401e3,
 * last_updated: {
 * $date: 2021-11-30T06:51:46.107Z
 * },
 * createTime: {
 * $date: 2021-11-30T06:43:44.895Z
 * }
 * }
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("Application")
public class ApplicationEntity extends BaseEntity {

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
    private List responseTypes;
    private String clientSecret;
    private List scopes;
    private List<String> redirectUris;
    private Boolean showMenu;
    private Set<String> clientAuthenticationMethods;


}