package com.tapdata.tm.oauth2.entity;

import lombok.Data;

import java.util.List;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/12/20 下午7:01
 */
@Data
public class OAuth2AuthorizationConsentEntity {
    private String registeredClientId;
    private String principalName;
    private List<String> authorities;
}
