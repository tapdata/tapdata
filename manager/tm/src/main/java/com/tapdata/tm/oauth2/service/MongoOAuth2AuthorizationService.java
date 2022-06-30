package com.tapdata.tm.oauth2.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.oauth2.entity.OAuth2AuthorizationEntity;
import com.tapdata.tm.oauth2.entity.Token;
import com.tapdata.tm.oauth2.jackson2.UserDetailJacksonModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.security.jackson2.CoreJackson2Module;
import org.springframework.security.jackson2.SecurityJackson2Modules;
import org.springframework.security.oauth2.core.*;
import org.springframework.security.oauth2.core.endpoint.OAuth2ParameterNames;
import org.springframework.security.oauth2.core.oidc.OidcIdToken;
import org.springframework.security.oauth2.server.authorization.OAuth2Authorization;
import org.springframework.security.oauth2.server.authorization.OAuth2AuthorizationCode;
import org.springframework.security.oauth2.server.authorization.OAuth2AuthorizationService;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClient;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClientRepository;
import org.springframework.security.oauth2.server.authorization.jackson2.OAuth2AuthorizationServerJackson2Module;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/12/17 下午7:36
 */
@Slf4j
@Service
public class MongoOAuth2AuthorizationService implements OAuth2AuthorizationService {

    private static final String TOKEN_TYPE_AUTHORIZATION_CODE = "authorization_code";
    private static final String TOKEN_TYPE_ACCESS_TOKEN = "access_token";
    private static final String TOKEN_TYPE_REFRESH_TOKEN = "refresh_token";
    private static final String TOKEN_TYPE_OIDC_ID_TOKEN = "oidc_id_token";
    private final Map<Class<? extends AbstractOAuth2Token>, String> tokenType =
            new HashMap<Class<? extends AbstractOAuth2Token>, String>(){{
        put(OAuth2AuthorizationCode.class, TOKEN_TYPE_AUTHORIZATION_CODE);
        put(OAuth2AccessToken.class, TOKEN_TYPE_ACCESS_TOKEN);
        put(OAuth2RefreshToken.class, TOKEN_TYPE_REFRESH_TOKEN);
        put(OAuth2RefreshToken2.class, TOKEN_TYPE_REFRESH_TOKEN);
        put(OidcIdToken.class, TOKEN_TYPE_OIDC_ID_TOKEN);
    }};

    private final MongoOperations mongoOperations;
    private final RegisteredClientRepository registeredClientRepository;

    private String collectionName;

    private ObjectMapper objectMapper = new ObjectMapper();

    public MongoOAuth2AuthorizationService(MongoOperations mongoOperations, RegisteredClientRepository registeredClientRepository) {

        Assert.notNull(mongoOperations, "MongoOperations can't be empty.");
        Assert.notNull(registeredClientRepository, "RegisteredClientRepository can't be empty.");

        this.mongoOperations = mongoOperations;
        this.registeredClientRepository = registeredClientRepository;
        this.collectionName = OAuth2Authorization.class.getSimpleName();

        ClassLoader classLoader = MongoOAuth2AuthorizationService.class.getClassLoader();
        List<Module> securityModules = SecurityJackson2Modules.getModules(classLoader);
        this.objectMapper.registerModules(securityModules);
        this.objectMapper.registerModule(new OAuth2AuthorizationServerJackson2Module());
        this.objectMapper.registerModule(new UserDetailJacksonModule());
        this.objectMapper.registerModule(new CoreJackson2Module());
    }

    @Override
    public void save(OAuth2Authorization authorization) {
        mongoOperations.save(mapperEntity(authorization), collectionName);
    }

    @Override
    public void remove(OAuth2Authorization authorization) {
        mongoOperations.remove(mapperEntity(authorization), collectionName);
    }

    @Override
    public OAuth2Authorization findById(String id) {
        return mapperEntity(mongoOperations.findById(id, OAuth2AuthorizationEntity.class, collectionName));
    }

    @Override
    public OAuth2Authorization findByToken(String token, OAuth2TokenType tokenType) {

        Criteria criteria = null;
        if (tokenType == null) {
            criteria = new Criteria().orOperator(
                    Criteria.where("state").is(token),
                    Criteria.where("tokens.token").is(token)
            );

        } else if (OAuth2ParameterNames.STATE.equals(tokenType.getValue())) {
            criteria = Criteria.where("state").is(token);
        } else if (OAuth2ParameterNames.CODE.equals(tokenType.getValue())) {
            criteria = Criteria.where("tokens").elemMatch(Criteria.where("token").is(token)
                    .and("tokenType").is(TOKEN_TYPE_AUTHORIZATION_CODE));
        } else if (OAuth2ParameterNames.ACCESS_TOKEN.equals(tokenType.getValue())) {
            criteria = Criteria.where("tokens").elemMatch(Criteria.where("token").is(token)
                    .and("tokenType").is(TOKEN_TYPE_ACCESS_TOKEN));
        } else if (OAuth2ParameterNames.REFRESH_TOKEN.equals(tokenType.getValue())) {
            criteria = Criteria.where("tokens").elemMatch(Criteria.where("token").is(token)
                    .and("tokenType").is(TOKEN_TYPE_REFRESH_TOKEN));
        }
        if (criteria != null) {
            OAuth2AuthorizationEntity entity = mongoOperations.findOne(Query.query(criteria), OAuth2AuthorizationEntity.class, collectionName);
            if (entity != null)
                return mapperEntity(entity);
        }
        return null;
    }

    private OAuth2AuthorizationEntity mapperEntity(OAuth2Authorization authorization) {
        if (authorization == null) return null;
        OAuth2AuthorizationEntity entity = new OAuth2AuthorizationEntity();
        entity.setId(authorization.getId());
        entity.setAuthorizationGrantType(authorization.getAuthorizationGrantType().getValue());
        entity.setPrincipalName(authorization.getPrincipalName());
        entity.setRegisteredClientId(authorization.getRegisteredClientId());

        String state = null;
        String authorizationState = authorization.getAttribute(OAuth2ParameterNames.STATE);
        if (StringUtils.hasText(authorizationState)) {
            state = authorizationState;
        }
        entity.setState(state);

        List<Token> tokens = new ArrayList<>();

        Token token = mapperToken(authorization.getToken(OAuth2AuthorizationCode.class));
        if (token != null) tokens.add(token);

        OAuth2Authorization.Token<OAuth2AccessToken> accessToken = authorization.getAccessToken();
        token = mapperToken(accessToken);
        if (token != null) {
            if (accessToken != null){
                OAuth2AccessToken.TokenType tokenType = accessToken.getToken().getTokenType();
                token.setAccessTokenType(tokenType.getValue());
                token.setScopes(accessToken.getToken().getScopes());
            }
            tokens.add(token);
        }

        OAuth2Authorization.Token<OidcIdToken> oidcIdToken = authorization.getToken(OidcIdToken.class);
        token = mapperToken(oidcIdToken);
        if (token != null) {
            if (oidcIdToken != null) {
                /*List<KeyValuePair<Object>> claims = new ArrayList<>();
                Optional.ofNullable(oidcIdToken.getClaims()).orElse(Collections.emptyMap())
                        .forEach((key, value) -> {
                            claims.add(new KeyValuePair<>(key, value));
                        });*/
                token.setClaims(writeMap(oidcIdToken.getClaims()));
            }
            tokens.add(token);
        }

        token = mapperToken(authorization.getRefreshToken());
        if (token != null) {
            tokens.add(token);
        }

        /*List<KeyValuePair<Object>> attributes = new ArrayList<>();
        Optional.ofNullable(authorization.getAttributes()).orElse(Collections.emptyMap())
                .forEach((key, value) -> {
                    attributes.add(new KeyValuePair<>(key, value));
                });*/
        entity.setAttributes(writeMap(authorization.getAttributes()));
        entity.setTokens(tokens);
        return entity;
    }

    public Token mapperToken(OAuth2Authorization.Token<? extends AbstractOAuth2Token> authorization) {
        if (authorization != null) {
            String tokenValue = authorization.getToken().getTokenValue();
            Instant expiresAt = authorization.getToken().getExpiresAt();
            Instant issuedAt = authorization.getToken().getIssuedAt();

            Token token = new Token();

            token.setTokenType(tokenType.get(authorization.getToken().getClass()));
            token.setExpiresAt(expiresAt);
            token.setIssuedAt(issuedAt);
            token.setToken(tokenValue);
            token.setTokenMetadata(writeMap(authorization.getMetadata()));
            return token;
        }
        return null;
    }

    private OAuth2Authorization mapperEntity(OAuth2AuthorizationEntity entity) {
        if (entity == null) return null;

        String registeredClientId = entity.getRegisteredClientId();
        RegisteredClient registeredClient = this.registeredClientRepository.findById(registeredClientId);
        if (registeredClient == null) {
            throw new DataRetrievalFailureException(
                    "The RegisteredClient with id '" + registeredClientId + "' was not found in the RegisteredClientRepository.");
        }

        OAuth2Authorization.Builder builder = OAuth2Authorization.withRegisteredClient(registeredClient);

        builder.id(entity.getId())
                .principalName(entity.getPrincipalName())
                .authorizationGrantType(new AuthorizationGrantType(entity.getAuthorizationGrantType()))
                .attributes((attrs) -> {
                    /*Optional.ofNullable(entity.getAttributes()).orElse(Collections.emptyList())
                            .forEach(keyValue -> {
                                attrs.put(keyValue.getKey(), keyValue.getValue());
                            });*/
                    Map<String, Object> map = parseMap(entity.getAttributes());
                    attrs.putAll(map);
                });
        String state = entity.getState();
        if (StringUtils.hasText(state)) {
            builder.attribute(OAuth2ParameterNames.STATE, state);
        }

        entity.getTokens().forEach(token -> {

            AbstractOAuth2Token authorizationToken = null;
            if (TOKEN_TYPE_AUTHORIZATION_CODE.equals(token.getTokenType())){
                authorizationToken = new OAuth2AuthorizationCode(token.getToken(), token.getIssuedAt(), token.getExpiresAt());
            } else if(TOKEN_TYPE_ACCESS_TOKEN.equals(token.getTokenType())) {
                authorizationToken = new OAuth2AccessToken(OAuth2AccessToken.TokenType.BEARER,
                        token.getToken(), token.getIssuedAt(), token.getExpiresAt(), token.getScopes());
            } else if(TOKEN_TYPE_OIDC_ID_TOKEN.equals(token.getTokenType())) {
                /*Map<String, Object> claims = new HashMap<>();
                Optional.ofNullable(token.getClaims()).orElse(Collections.emptyList()).forEach(keyValuePair -> {
                    claims.put(keyValuePair.getKey(), keyValuePair.getValue());
                });*/
                authorizationToken = new OidcIdToken(token.getToken(), token.getIssuedAt(),token.getExpiresAt(), parseMap(token.getClaims()));
            } else if(TOKEN_TYPE_REFRESH_TOKEN.equals(token.getTokenType())) {
                authorizationToken = new OAuth2RefreshToken2(token.getToken(), token.getIssuedAt(), token.getExpiresAt());
            } else {
                log.warn("Not implement by token type {}", token.getTokenType());
            }
            if (authorizationToken != null){
                builder.token(authorizationToken, (metadata) -> {
                    metadata.putAll(parseMap(token.getTokenMetadata()));
                });
            }
        });

        return builder.build();
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
