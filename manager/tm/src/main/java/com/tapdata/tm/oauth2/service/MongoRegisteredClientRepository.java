package com.tapdata.tm.oauth2.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.oauth2.entity.RegisteredClientEntity;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.security.jackson2.SecurityJackson2Modules;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.ClientAuthenticationMethod;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClient;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClientRepository;
import org.springframework.security.oauth2.server.authorization.jackson2.OAuth2AuthorizationServerJackson2Module;
import org.springframework.security.oauth2.server.authorization.settings.ClientSettings;
import org.springframework.security.oauth2.server.authorization.settings.TokenSettings;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/12/17 下午8:58
 */
@Service
public class MongoRegisteredClientRepository implements RegisteredClientRepository {

    private static ObjectMapper objectMapper = new ObjectMapper();

    private final MongoOperations mongoOperations;
    private final PasswordEncoder passwordEncoder;
    private String collectionName;

    public MongoRegisteredClientRepository(MongoOperations mongoOperations, PasswordEncoder passwordEncoder) {

        Assert.notNull(mongoOperations, "MongoOperations can't be empty.");
        Assert.notNull(passwordEncoder, "PasswordEncoder can't be empty.");
        this.mongoOperations = mongoOperations;
        this.passwordEncoder = passwordEncoder;
        this.collectionName = "Application";

        ClassLoader classLoader = MongoRegisteredClientRepository.class.getClassLoader();
        List<Module> securityModules = SecurityJackson2Modules.getModules(classLoader);
        this.objectMapper.registerModules(securityModules);
        this.objectMapper.registerModule(new OAuth2AuthorizationServerJackson2Module());
    }

    @PostConstruct
    public void init() {
        RegisteredClient registeredClient = RegisteredClient.withId("5c0e750b7a5cd42464a5099d")
                .clientId("5c0e750b7a5cd42464a5099d")
                .clientName("Data Explorer")
                // 使用 {noop} 前缀表示不编码的密码，适用于开发环境
                .clientSecret("{noop}eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9")
                .clientAuthenticationMethod(ClientAuthenticationMethod.CLIENT_SECRET_BASIC)
                .clientAuthenticationMethod(ClientAuthenticationMethod.CLIENT_SECRET_POST)
                .authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE)
                .authorizationGrantType(AuthorizationGrantType.CLIENT_CREDENTIALS)
                .authorizationGrantType(AuthorizationGrantType.JWT_BEARER)
                .authorizationGrantType(AuthorizationGrantType.REFRESH_TOKEN)
                .authorizationGrantType(AuthorizationGrantType.PASSWORD)
                .redirectUri("http://127.0.0.1")
                .scope("admin")
                .clientSettings(ClientSettings.builder().requireAuthorizationConsent(false).build())
                .tokenSettings(TokenSettings.builder().accessTokenTimeToLive(Duration.ofDays(14)).
                        refreshTokenTimeToLive(Duration.ofDays(14)).
                        reuseRefreshTokens(true).
                        build())
                .build();
        if (findByClientId(registeredClient.getClientId()) == null) {
            save(registeredClient);
        }
    }

    public String getCollectionName() {
        if (collectionName == null) {
            collectionName = RegisteredClient.class.getSimpleName();
        }
        return collectionName;
    }

    @Override
    public void save(RegisteredClient registeredClient) {
        RegisteredClientEntity entity = mapperEntity(registeredClient);
        mongoOperations.save(entity, getCollectionName());
    }

    private RegisteredClientEntity mapperEntity(RegisteredClient registeredClient) {
        if (registeredClient == null) {
            return null;
        }
        RegisteredClientEntity registeredClientEntity = new RegisteredClientEntity();
        registeredClientEntity.setId(new ObjectId(registeredClient.getId()));
        registeredClientEntity.setClientId(registeredClient.getClientId());
        registeredClientEntity.setClientIdIssuedAt(registeredClient.getClientIdIssuedAt());
        registeredClientEntity.setClientSecret(registeredClient.getClientSecret());
        registeredClientEntity.setClientSecretExpiresAt(registeredClient.getClientSecretExpiresAt());
        registeredClientEntity.setClientName(registeredClient.getClientName());
        registeredClientEntity.setClientAuthenticationMethods(registeredClient.getClientAuthenticationMethods()
                .stream().map(ClientAuthenticationMethod::getValue).collect(Collectors.toSet()));
        registeredClientEntity.setAuthorizationGrantTypes(registeredClient.getAuthorizationGrantTypes()
                .stream().map(AuthorizationGrantType::getValue).collect(Collectors.toSet()));
        registeredClientEntity.setRedirectUris(registeredClient.getRedirectUris());
        registeredClientEntity.setScopes(registeredClient.getScopes());
        registeredClientEntity.setClientSettings(writeMap(registeredClient.getClientSettings().getSettings()));
        registeredClientEntity.setTokenSettings(writeMap(registeredClient.getTokenSettings().getSettings()));
        return registeredClientEntity;
    }

    private RegisteredClient mapperEntity(RegisteredClientEntity registeredClientEntity) {
        if (registeredClientEntity == null)
            return null;
        return RegisteredClient.withId(registeredClientEntity.getId().toHexString())
                .clientSettings(
                        StringUtils.isNotBlank(registeredClientEntity.getClientSettings()) ?
                                ClientSettings.builder().settings(stringObjectMap -> {stringObjectMap.putAll(parseMap(registeredClientEntity.getClientSettings()));}).build() :
                                ClientSettings.builder().build())
                .tokenSettings(
                        StringUtils.isNotBlank(registeredClientEntity.getTokenSettings()) ?
                                TokenSettings.builder().settings(stringObjectMap -> {stringObjectMap.putAll(parseMap(registeredClientEntity.getTokenSettings()));}).build() :
                                TokenSettings.builder().build()
                )
                .clientId(registeredClientEntity.getClientId())
                .clientIdIssuedAt(registeredClientEntity.getClientIdIssuedAt())
                .clientSecret(registeredClientEntity.getClientSecret())
                .clientSecretExpiresAt(registeredClientEntity.getClientSecretExpiresAt())
                .clientName(registeredClientEntity.getClientName())
                .clientAuthenticationMethods(clientAuthenticationMethods ->
                    clientAuthenticationMethods.addAll(Optional.ofNullable(registeredClientEntity.getClientAuthenticationMethods()).orElse(Collections.emptySet())
                            .stream().map(ClientAuthenticationMethod::new).collect(Collectors.toSet())))
                .authorizationGrantTypes(authorizationGrantTypes ->
                        authorizationGrantTypes.addAll(Optional.ofNullable(registeredClientEntity.getAuthorizationGrantTypes()).orElse(Collections.emptySet())
                        .stream().map(AuthorizationGrantType::new).collect(Collectors.toSet())))
                .redirectUris(re -> re.addAll(registeredClientEntity.getRedirectUris()))
                .scopes(scopes -> scopes.addAll(registeredClientEntity.getScopes()))
                .build();
    }

    @Override
    public RegisteredClient findById(String id) {
        RegisteredClientEntity entity = mongoOperations.findById(id, RegisteredClientEntity.class, getCollectionName());
        return mapperEntity(entity);
    }

    @Override
    public RegisteredClient findByClientId(String clientId) {
        RegisteredClientEntity entity = mongoOperations.findOne(Query.query(Criteria.where("clientId").is(clientId)),
                RegisteredClientEntity.class, getCollectionName());
        return mapperEntity(entity);
    }

    public static Map<String, Object> parseMap(String data) {
        try {
            return objectMapper.readValue(data, new TypeReference<Map<String, Object>>() {});
        } catch (Exception ex) {
            throw new IllegalArgumentException(ex.getMessage(), ex);
        }
    }

    public static String writeMap(Map<String, Object> data) {
        try {
            return objectMapper.writeValueAsString(data);
        } catch (Exception ex) {
            throw new IllegalArgumentException(ex.getMessage(), ex);
        }
    }
}
