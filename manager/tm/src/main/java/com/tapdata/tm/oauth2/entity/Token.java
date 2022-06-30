package com.tapdata.tm.oauth2.entity;

import lombok.Data;

import java.time.Instant;
import java.util.Set;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/12/18 上午11:49
 */
@Data
public class Token {

    private String tokenType;

    private String token;
    private Instant expiresAt;
    private Instant issuedAt;
    private String tokenMetadata;

    private String accessTokenType;
    private Set<String> scopes;

    private String claims;
}
