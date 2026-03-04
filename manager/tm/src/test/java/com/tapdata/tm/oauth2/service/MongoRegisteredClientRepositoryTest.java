package com.tapdata.tm.oauth2.service;


import com.tapdata.tm.oauth2.entity.RegisteredClientEntity;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClient;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MongoRegisteredClientRepositoryTest {
    MongoRegisteredClientRepository repository;
    MongoOperations mongoOperations;
    @BeforeEach
    void init() {
        mongoOperations = mock(MongoOperations.class);
        repository = new MongoRegisteredClientRepository(mongoOperations);
    }

    @Nested
    class mapperEntityTest {

        @Test
        void testOldVersion() {
            RegisteredClientEntity fromOldVersion = new RegisteredClientEntity();
            fromOldVersion.setAuthorizationGrantTypes(Set.of("refresh_token", "client_credentials", "password", "authorization_code", "urn:ietf:params:oauth:grant-type:jwt-bearer"));
            fromOldVersion.setRedirectUris(Set.of("http://127.0.0.1:3030"));
            fromOldVersion.setScopes(new HashSet<>());
            fromOldVersion.setClientName("name");
            fromOldVersion.setClientSecretExpiresAt(Instant.MAX);
            fromOldVersion.setClientSecret("c");
            fromOldVersion.setId(new ObjectId());
            fromOldVersion.setClientId("id");
            fromOldVersion.setTokenSettings("{\"@class\":\"java.util.HashMap\",\"setting.token.access-token-time-to-live\":[\"java.time.Duration\",1209600.000000000],\"setting.token.refresh-token-time-to-live\":[\"java.time.Duration\",1209600.000000000],\"setting.token.id-token-signature-algorithm\":[\"org.springframework.security.oauth2.jose.jws.SignatureAlgorithm\",\"RS256\"],\"setting.token.reuse-refresh-tokens\":true}");
            when(mongoOperations.findById(anyString(), any(Class.class), anyString())).thenReturn(fromOldVersion);
            RegisteredClient client = repository.findById("id");
            Assertions.assertNotNull(client);
            Assertions.assertNotNull(client.getTokenSettings());
            Assertions.assertNotNull(client.getTokenSettings().getAccessTokenTimeToLive());
            Assertions.assertEquals(1209600L, client.getTokenSettings().getAccessTokenTimeToLive().getSeconds());
        }

        @Test
        void testNewVersion() {
            RegisteredClientEntity fromNewVersion = new RegisteredClientEntity();
            fromNewVersion.setAuthorizationGrantTypes(Set.of("refresh_token", "client_credentials", "password", "authorization_code", "urn:ietf:params:oauth:grant-type:jwt-bearer"));
            fromNewVersion.setScopes(new HashSet<>());
            fromNewVersion.setRedirectUris(Set.of("http://127.0.0.1:3030"));
            fromNewVersion.setClientSecretExpiresAt(Instant.MAX);
            fromNewVersion.setClientSecret("c");
            fromNewVersion.setId(new ObjectId());
            fromNewVersion.setClientId("id");
            fromNewVersion.setTokenSettings("{\"@class\":\"java.util.Collections$UnmodifiableMap\",\"settings.token.reuse-refresh-tokens\":true,\"settings.token.x509-certificate-bound-access-tokens\":false,\"settings.token.id-token-signature-algorithm\":[\"org.springframework.security.oauth2.jose.jws.SignatureAlgorithm\",\"RS256\"],\"settings.token.access-token-time-to-live\":[\"java.time.Duration\",1209600.000000000],\"settings.token.access-token-format\":{\"@class\":\"org.springframework.security.oauth2.server.authorization.settings.OAuth2TokenFormat\",\"value\":\"self-contained\"},\"settings.token.refresh-token-time-to-live\":[\"java.time.Duration\",1209600.000000000],\"settings.token.authorization-code-time-to-live\":[\"java.time.Duration\",300.000000000],\"settings.token.device-code-time-to-live\":[\"java.time.Duration\",300.000000000]}");
            when(mongoOperations.findById(anyString(), any(Class.class), anyString())).thenReturn(fromNewVersion);
            RegisteredClient client = repository.findById("id");
            Assertions.assertNotNull(client);
            Assertions.assertNotNull(client.getTokenSettings());
            Assertions.assertNotNull(client.getTokenSettings().getAccessTokenTimeToLive());
            Assertions.assertEquals(1209600L, client.getTokenSettings().getAccessTokenTimeToLive().getSeconds());
        }

        @Test
        void testAllOfOldAndNewVersion() {
            RegisteredClientEntity fromOldVersion = new RegisteredClientEntity();
            fromOldVersion.setScopes(new HashSet<>());
            fromOldVersion.setAuthorizationGrantTypes(Set.of("refresh_token", "client_credentials", "password", "authorization_code", "urn:ietf:params:oauth:grant-type:jwt-bearer"));
            fromOldVersion.setClientSecretExpiresAt(Instant.MAX);
            fromOldVersion.setRedirectUris(Set.of("http://127.0.0.1:3030"));
            fromOldVersion.setClientName("name");
            fromOldVersion.setClientSecret("c");
            fromOldVersion.setId(new ObjectId());
            fromOldVersion.setClientId("id");
            fromOldVersion.setTokenSettings("{\"@class\":\"java.util.Collections$UnmodifiableMap\",\"setting.token.access-token-time-to-live\":[\"java.time.Duration\",1209600.000000000],\"setting.token.refresh-token-time-to-live\":[\"java.time.Duration\",1209600.000000000],\"setting.token.id-token-signature-algorithm\":[\"org.springframework.security.oauth2.jose.jws.SignatureAlgorithm\",\"RS256\"],\"setting.token.reuse-refresh-tokens\":true,\"settings.token.reuse-refresh-tokens\":true,\"settings.token.x509-certificate-bound-access-tokens\":false,\"settings.token.id-token-signature-algorithm\":[\"org.springframework.security.oauth2.jose.jws.SignatureAlgorithm\",\"RS256\"],\"settings.token.access-token-time-to-live\":[\"java.time.Duration\",1209600.000000000],\"settings.token.access-token-format\":{\"@class\":\"org.springframework.security.oauth2.server.authorization.settings.OAuth2TokenFormat\",\"value\":\"self-contained\"},\"settings.token.refresh-token-time-to-live\":[\"java.time.Duration\",1209600.000000000],\"settings.token.authorization-code-time-to-live\":[\"java.time.Duration\",300.000000000],\"settings.token.device-code-time-to-live\":[\"java.time.Duration\",300.000000000]}");
            when(mongoOperations.findById(anyString(), any(Class.class), anyString())).thenReturn(fromOldVersion);
            RegisteredClient client = repository.findById("id");
            Assertions.assertNotNull(client);
            Assertions.assertNotNull(client.getTokenSettings());
            Assertions.assertNotNull(client.getTokenSettings().getAccessTokenTimeToLive());
            Assertions.assertEquals(1209600L, client.getTokenSettings().getAccessTokenTimeToLive().getSeconds());
        }

        @Test
        void testTokenSettingHasNullVal() {
            RegisteredClientEntity fromOldVersion = new RegisteredClientEntity();
            fromOldVersion.setScopes(new HashSet<>());
            fromOldVersion.setAuthorizationGrantTypes(Set.of("refresh_token", "client_credentials", "password", "authorization_code", "urn:ietf:params:oauth:grant-type:jwt-bearer"));
            fromOldVersion.setClientSecretExpiresAt(Instant.MAX);
            fromOldVersion.setRedirectUris(Set.of("http://127.0.0.1:3030"));
            fromOldVersion.setClientName("name");
            fromOldVersion.setClientSecret("c");
            fromOldVersion.setId(new ObjectId());
            fromOldVersion.setClientId("id");
            fromOldVersion.setTokenSettings("{\"@class\":\"java.util.HashMap\",\"setting.token.access-token-time-to-live\": null,\"settings.token.access-token-time-to-live\": null,\"setting.token.refresh-token-time-to-live\":[\"java.time.Duration\",1209600.000000000],\"setting.token.id-token-signature-algorithm\":[\"org.springframework.security.oauth2.jose.jws.SignatureAlgorithm\",\"RS256\"],\"setting.token.reuse-refresh-tokens\":true}");
            when(mongoOperations.findById(anyString(), any(Class.class), anyString())).thenReturn(fromOldVersion);
            RegisteredClient client = repository.findById("id");
            Assertions.assertNotNull(client);
            Assertions.assertNotNull(client.getTokenSettings());
            Assertions.assertNull(client.getTokenSettings().getAccessTokenTimeToLive());
        }

        @Test
        void testNotHasTokenSetting() {
            RegisteredClientEntity fromOldVersion = new RegisteredClientEntity();
            fromOldVersion.setScopes(new HashSet<>());
            fromOldVersion.setAuthorizationGrantTypes(Set.of("refresh_token", "client_credentials", "password", "authorization_code", "urn:ietf:params:oauth:grant-type:jwt-bearer"));
            fromOldVersion.setClientSecretExpiresAt(Instant.MAX);
            fromOldVersion.setRedirectUris(Set.of("http://127.0.0.1:3030"));
            fromOldVersion.setClientName("name");
            fromOldVersion.setClientSecret("c");
            fromOldVersion.setId(new ObjectId());
            fromOldVersion.setClientId("id");
            when(mongoOperations.findById(anyString(), any(Class.class), anyString())).thenReturn(fromOldVersion);
            RegisteredClient client = repository.findById("id");
            Assertions.assertNotNull(client);
            Assertions.assertNotNull(client.getTokenSettings());
            Assertions.assertNotNull(client.getTokenSettings().getAccessTokenTimeToLive());
            Assertions.assertEquals(1209600L, client.getTokenSettings().getAccessTokenTimeToLive().getSeconds());
        }
    }
}