package com.tapdata.tm.oauth2.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tapdata.tm.oauth2.entity.RegisteredClientEntity;
import jakarta.annotation.PostConstruct;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.security.jackson2.SecurityJackson2Modules;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.ClientAuthenticationMethod;
import org.springframework.security.oauth2.jose.jws.SignatureAlgorithm;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClient;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClientRepository;
import org.springframework.security.oauth2.server.authorization.jackson2.OAuth2AuthorizationServerJackson2Module;
import org.springframework.security.oauth2.server.authorization.settings.ClientSettings;
import org.springframework.security.oauth2.server.authorization.settings.ConfigurationSettingNames;
import org.springframework.security.oauth2.server.authorization.settings.OAuth2TokenFormat;
import org.springframework.security.oauth2.server.authorization.settings.TokenSettings;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/12/17 下午8:58
 */
@Service
public class MongoRegisteredClientRepository implements RegisteredClientRepository {
    private static final Map<String, String> OLD_SETTING_MAP_KEY =
            Map.of(
                    LowerJdkToken.AUTHORIZATION_CODE_TIME_TO_LIVE, ConfigurationSettingNames.Token.AUTHORIZATION_CODE_TIME_TO_LIVE,
                    LowerJdkToken.ACCESS_TOKEN_TIME_TO_LIVE, ConfigurationSettingNames.Token.ACCESS_TOKEN_TIME_TO_LIVE,
                    LowerJdkToken.ACCESS_TOKEN_FORMAT, ConfigurationSettingNames.Token.ACCESS_TOKEN_FORMAT,
                    LowerJdkToken.DEVICE_CODE_TIME_TO_LIVE, ConfigurationSettingNames.Token.DEVICE_CODE_TIME_TO_LIVE,
                    LowerJdkToken.REUSE_REFRESH_TOKENS, ConfigurationSettingNames.Token.REUSE_REFRESH_TOKENS,
                    LowerJdkToken.REFRESH_TOKEN_TIME_TO_LIVE, ConfigurationSettingNames.Token.REFRESH_TOKEN_TIME_TO_LIVE,
                    LowerJdkToken.ID_TOKEN_SIGNATURE_ALGORITHM, ConfigurationSettingNames.Token.ID_TOKEN_SIGNATURE_ALGORITHM,
                    LowerJdkToken.X509_CERTIFICATE_BOUND_ACCESS_TOKENS, ConfigurationSettingNames.Token.X509_CERTIFICATE_BOUND_ACCESS_TOKENS
            );

    public static final class LowerJdkToken {
        private static final String TOKEN_SETTINGS_NAMESPACE = "setting.token.";
        public static final String AUTHORIZATION_CODE_TIME_TO_LIVE;
        public static final String ACCESS_TOKEN_TIME_TO_LIVE;
        public static final String ACCESS_TOKEN_FORMAT;
        public static final String DEVICE_CODE_TIME_TO_LIVE;
        public static final String REUSE_REFRESH_TOKENS;
        public static final String REFRESH_TOKEN_TIME_TO_LIVE;
        public static final String ID_TOKEN_SIGNATURE_ALGORITHM;
        public static final String X509_CERTIFICATE_BOUND_ACCESS_TOKENS;

        private LowerJdkToken() {
        }

        static {
            AUTHORIZATION_CODE_TIME_TO_LIVE = TOKEN_SETTINGS_NAMESPACE.concat("authorization-code-time-to-live");
            ACCESS_TOKEN_TIME_TO_LIVE = TOKEN_SETTINGS_NAMESPACE.concat("access-token-time-to-live");
            ACCESS_TOKEN_FORMAT = TOKEN_SETTINGS_NAMESPACE.concat("access-token-format");
            DEVICE_CODE_TIME_TO_LIVE = TOKEN_SETTINGS_NAMESPACE.concat("device-code-time-to-live");
            REUSE_REFRESH_TOKENS = TOKEN_SETTINGS_NAMESPACE.concat("reuse-refresh-tokens");
            REFRESH_TOKEN_TIME_TO_LIVE = TOKEN_SETTINGS_NAMESPACE.concat("refresh-token-time-to-live");
            ID_TOKEN_SIGNATURE_ALGORITHM = TOKEN_SETTINGS_NAMESPACE.concat("id-token-signature-algorithm");
            X509_CERTIFICATE_BOUND_ACCESS_TOKENS = TOKEN_SETTINGS_NAMESPACE.concat("x509-certificate-bound-access-tokens");
        }

        public static Map<String, Object> transform(Map<String, Object> settings) {
            new ArrayList<>(settings.keySet()).forEach(key -> {
                        String newKey = OLD_SETTING_MAP_KEY.get(key);
                        if (newKey == null) {
                            return;
                        }
                        if (null == settings.get(newKey)) {
                            Object val = settings.get(key);
                            if (null != val) {
                                settings.remove(key);
                                settings.put(newKey, val);
                            }
                        }
                    }
            );
            return settings;
        }
    }

    private static ObjectMapper objectMapper = new ObjectMapper();

    private final MongoOperations mongoOperations;
    private String collectionName;

    public MongoRegisteredClientRepository(MongoOperations mongoOperations) {

        Assert.notNull(mongoOperations, "MongoOperations can't be empty.");
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
        String isDebug = Optional.ofNullable((ServletRequestAttributes) RequestContextHolder.getRequestAttributes())
                .map(ServletRequestAttributes::getRequest)
                .map(e -> e.getHeader("debug"))
                .orElse("false");
        TokenSettings.Builder tokenSettings = TokenSettings.builder()
                .authorizationCodeTimeToLive(Duration.ofDays(14L))
                .accessTokenTimeToLive(Duration.ofDays(14L))
                .accessTokenFormat(OAuth2TokenFormat.SELF_CONTAINED)
                .deviceCodeTimeToLive(Duration.ofDays(14L))
                .reuseRefreshTokens(true)
                .refreshTokenTimeToLive(Duration.ofDays(14L))
                .idTokenSignatureAlgorithm(SignatureAlgorithm.RS256)
                .x509CertificateBoundAccessTokens(false);
        if (StringUtils.isNotBlank(registeredClientEntity.getTokenSettings())) {
            tokenSettings.settings(stringObjectMap -> stringObjectMap.putAll(parseMap(registeredClientEntity.getTokenSettings())));
        }
        if ("true".equalsIgnoreCase(isDebug)) {
            tokenSettings.accessTokenTimeToLive(Duration.ofMinutes(5L));
        }
        return RegisteredClient.withId(registeredClientEntity.getId().toHexString())
                .clientSettings(
                        StringUtils.isNotBlank(registeredClientEntity.getClientSettings()) ?
                                ClientSettings.builder().settings(stringObjectMap -> {stringObjectMap.putAll(parseMap(registeredClientEntity.getClientSettings()));}).build() :
                                ClientSettings.builder().build())
                .tokenSettings(tokenSettings.build())
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
            return LowerJdkToken.transform(objectMapper.readValue(data, new TypeReference<Map<String, Object>>() {}));
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
