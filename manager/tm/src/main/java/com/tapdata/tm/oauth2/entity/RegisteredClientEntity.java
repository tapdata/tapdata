package com.tapdata.tm.oauth2.entity;

import lombok.Data;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.Instant;
import java.util.Set;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/12/17 下午10:44
 */
@Data
public class RegisteredClientEntity {
    private ObjectId id;
    private String clientId;
    private Instant clientIdIssuedAt;
    private String clientSecret;
    private Instant clientSecretExpiresAt;
    private String clientName;
    private Set<String> clientAuthenticationMethods;
    @Field("grantTypes")
    private Set<String> authorizationGrantTypes;
    private Set<String> redirectUris;
    private Set<String> scopes;
    private String clientSettings;
    private String tokenSettings;
}
