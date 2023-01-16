package com.tapdata.tm.oauth2.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.oauth2.entity.RegisteredClientEntity;
import com.tapdata.tm.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.jackson2.SecurityJackson2Modules;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.ClientAuthenticationMethod;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClient;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClientRepository;
import org.springframework.security.oauth2.server.authorization.jackson2.OAuth2AuthorizationServerJackson2Module;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/12/17 下午8:58
 */
@Service
public class MongoRegisteredClientRepository implements RegisteredClientRepository {

    private final PasswordEncoder passwordEncoder;
    private ObjectMapper objectMapper = new ObjectMapper();

    private final MongoOperations mongoOperations;
    private String collectionName;

    public MongoRegisteredClientRepository(MongoOperations mongoOperations, PasswordEncoder passwordEncoder) {

        Assert.notNull(mongoOperations, "MongoOperations can't be empty.");
        this.passwordEncoder = passwordEncoder;
        this.mongoOperations = mongoOperations;
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
                //.clientSecret(passwordEncoder.encode("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"))
                .clientSecret("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9")
                .clientAuthenticationMethod(ClientAuthenticationMethod.CLIENT_SECRET_BASIC)
                .clientAuthenticationMethod(ClientAuthenticationMethod.BASIC)
                .clientAuthenticationMethod(ClientAuthenticationMethod.CLIENT_SECRET_POST)
                .clientAuthenticationMethod(ClientAuthenticationMethod.POST)
                .authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE)
                .authorizationGrantType(AuthorizationGrantType.CLIENT_CREDENTIALS)
                .authorizationGrantType(AuthorizationGrantType.JWT_BEARER)
                .authorizationGrantType(AuthorizationGrantType.IMPLICIT)
                .authorizationGrantType(AuthorizationGrantType.REFRESH_TOKEN)
                .authorizationGrantType(AuthorizationGrantType.PASSWORD)
                .redirectUri("http://127.0.0.1")
                .scope("admin")
                .clientSettings(clientSettings -> {
                    // 是否需要用户确认一下客户端需要获取用户的哪些权限
                    // 比如：客户端需要获取用户的 用户信息、用户照片 但是此处用户可以控制只给客户端授权获取 用户信息。
                    clientSettings.requireUserConsent(true);
                })
                .tokenSettings(tokenSettings -> {
                    tokenSettings.accessTokenTimeToLive(Duration.ofDays(14));
                    tokenSettings.refreshTokenTimeToLive(Duration.ofDays(14));
                    tokenSettings.reuseRefreshTokens(true);
                })
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
        registeredClientEntity.setRedirectUris(StringUtils.join(registeredClient.getRedirectUris(), ","));
        registeredClientEntity.setScopes(registeredClient.getScopes());
        registeredClientEntity.setClientSettings(writeMap(registeredClient.getClientSettings().settings()));
        registeredClientEntity.setTokenSettings(writeMap(registeredClient.getTokenSettings().settings()));
        return registeredClientEntity;
    }

    private RegisteredClient mapperEntity(RegisteredClientEntity registeredClientEntity) {
        if (registeredClientEntity == null)
            return null;
        return RegisteredClient.withId(registeredClientEntity.getId().toHexString())
                .clientSettings(clientSettings -> {
                    clientSettings.settings().putAll(parseMap(registeredClientEntity.getClientSettings()));
                })
                .tokenSettings(tokenSettings -> {
                    tokenSettings.settings().putAll(parseMap(registeredClientEntity.getTokenSettings()));
                })
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
                .redirectUris(re -> re.addAll(Lists.newArrayList(registeredClientEntity.getRedirectUris())))
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

    private Map<String, Object> parseMap(String data) {
        try {
            return this.objectMapper.readValue(data, new TypeReference<Map<String, Object>>() {});
        } catch (Exception ex) {
            throw new IllegalArgumentException(ex.getMessage(), ex);
        }
    }

    private String writeMap(Map<String, Object> data) {
        try {
            return this.objectMapper.writeValueAsString(data);
        } catch (Exception ex) {
            throw new IllegalArgumentException(ex.getMessage(), ex);
        }
    }
}
