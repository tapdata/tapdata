package com.tapdata.tm.oauth2.entity;

import lombok.Data;

import java.util.List;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/12/18 上午11:09
 */
@Data
public class OAuth2AuthorizationEntity {
    private String id;
    private String state;
    private String registeredClientId;
    private String principalName;
    private String authorizationGrantType;
    //private Map<Class<? extends AbstractOAuth2Token>, OAuth2Authorization.Token<?>> tokens;
    private List<Token> tokens;
    //private Map<String, Object> attributes;
    private String attributes;
}
