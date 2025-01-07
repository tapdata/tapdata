package com.tapdata.tm.oauth2.service;

import com.tapdata.tm.oauth2.entity.OAuth2AuthorizationEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.oauth2.server.authorization.OAuth2Authorization;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClient;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClientRepository;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

public class MongoOAuth2AuthorizationServiceTest {
    MongoOAuth2AuthorizationService mongoOAuth2AuthorizationService;
    MongoOperations mongoOperations;
    RegisteredClientRepository registeredClientRepository;
    OAuth2Authorization authorization;

    @BeforeEach
    void beforeEach() {
        mongoOperations = mock(MongoOperations.class);
        registeredClientRepository = mock(RegisteredClientRepository.class);
        mongoOAuth2AuthorizationService = new MongoOAuth2AuthorizationService(mongoOperations, registeredClientRepository);
        authorization = mock(OAuth2Authorization.class);
        when(authorization.getRegisteredClientId()).thenReturn("111");
        AuthorizationGrantType authorizationGrantType = mock(AuthorizationGrantType.class);
        when(authorization.getAuthorizationGrantType()).thenReturn(authorizationGrantType);
        when(authorizationGrantType.getValue()).thenReturn("client_credentials");
    }

    @Nested
    class saveTest {
        @BeforeEach
        void beforeEach() {
            mongoOAuth2AuthorizationService = spy(mongoOAuth2AuthorizationService);
        }

        @Test
        void testSave() {
            when(mongoOAuth2AuthorizationService.findValidToken(authorization)).thenReturn(null);
            mongoOAuth2AuthorizationService.save(authorization);
            verify(mongoOperations, new Times(1)).save(any(OAuth2AuthorizationEntity.class), anyString());
        }

        @Test
        void testSaveWhenExistsValidToken() {
            OAuth2Authorization existingAuthorization = mock(OAuth2Authorization.class);
            OAuth2Authorization.Token<OAuth2AccessToken> accessToken = mock(OAuth2Authorization.Token.class);
            when(existingAuthorization.getAccessToken()).thenReturn(accessToken);
            when(accessToken.isExpired()).thenReturn(false);
            doReturn(existingAuthorization).when(mongoOAuth2AuthorizationService).findValidToken(authorization);
            mongoOAuth2AuthorizationService.save(authorization);
            verify(mongoOperations, new Times(0)).save(any(OAuth2AuthorizationEntity.class), anyString());
        }

        @Test
        void testSaveWhenExistsInvalidToken() {
            OAuth2Authorization existingAuthorization = mock(OAuth2Authorization.class);
            OAuth2Authorization.Token<OAuth2AccessToken> accessToken = mock(OAuth2Authorization.Token.class);
            when(existingAuthorization.getAccessToken()).thenReturn(accessToken);
            when(accessToken.isExpired()).thenReturn(true);
            doReturn(existingAuthorization).when(mongoOAuth2AuthorizationService).findValidToken(authorization);
            mongoOAuth2AuthorizationService.save(authorization);
            verify(mongoOperations, new Times(1)).save(any(OAuth2AuthorizationEntity.class), anyString());
        }
    }

    @Nested
    class findValidTokenTest {
        @Test
        void testFindValidToken() {
            OAuth2AuthorizationEntity entity = mock(OAuth2AuthorizationEntity.class);
            when(mongoOperations.findOne(any(Query.class), any(Class.class), anyString())).thenReturn(entity);
            when(entity.getPrincipalName()).thenReturn("111");
            when(entity.getRegisteredClientId()).thenReturn("111");
            when(entity.getAuthorizationGrantType()).thenReturn("client_credentials");
            String data = "{\"@class\":\"java.util.Collections$UnmodifiableMap\",\"org.springframework.security.oauth2.server.authorization.OAuth2Authorization.AUTHORIZED_SCOPE\":[\"java.util.Collections$UnmodifiableSet\",[\"5b9a0a383fcba02649524bf1\"]]}";
            when(entity.getAttributes()).thenReturn(data);
            when(registeredClientRepository.findById("111")).thenReturn(mock(RegisteredClient.class));
            OAuth2Authorization actual = mongoOAuth2AuthorizationService.findValidToken(authorization);
            assertNotNull(actual);
        }

        @Test
        void testFindValidTokenReturnNull() {
            when(mongoOperations.findOne(any(Query.class), any(Class.class), anyString())).thenReturn(null);
            OAuth2Authorization actual = mongoOAuth2AuthorizationService.findValidToken(authorization);
            assertNull(actual);
        }
    }
}
