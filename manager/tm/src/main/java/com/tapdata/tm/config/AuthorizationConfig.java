package com.tapdata.tm.config;

import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.SecurityContext;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.base.convert.ObjectIdDeserialize;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.role.entity.RoleEntity;
import com.tapdata.tm.utils.SpringContextHelper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.bson.types.ObjectId;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;

import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.annotation.web.configurers.HeadersConfigurer;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.jwt.JwtDecoder;


import org.springframework.security.oauth2.server.authorization.authentication.OAuth2ClientAuthenticationToken;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClient;
import org.springframework.security.oauth2.server.authorization.config.annotation.web.configuration.OAuth2AuthorizationServerConfiguration;
import org.springframework.security.oauth2.server.authorization.config.annotation.web.configurers.OAuth2AuthorizationServerConfigurer;
import org.springframework.security.oauth2.server.authorization.settings.AuthorizationServerSettings;
import org.springframework.security.oauth2.server.authorization.token.JwtEncodingContext;
import org.springframework.security.oauth2.server.authorization.token.OAuth2TokenCustomizer;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.util.matcher.RequestMatcher;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;

import java.io.IOException;
import java.io.InputStream;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/12/15 下午2:37
 */
@Configuration
@Slf4j
public class AuthorizationConfig {
    /**
     * 个性化 JWT token
     */
    @Bean
    public OAuth2TokenCustomizer<JwtEncodingContext> jwtCustomizer(){
        return context -> {
            RegisteredClient registeredClient = context.getRegisteredClient();
            Authentication oAuth2ClientAuthenticationToken = context.getPrincipal();
            MongoOperations mongoOperations = SpringContextHelper.getBean(MongoOperations.class);
            Set<ObjectId> scopes = registeredClient.getScopes().stream().map(ObjectIdDeserialize::toObjectId).collect(Collectors.toSet());
            Criteria criteria = Criteria.where("_id").in(scopes);
            List<RoleEntity> roleEntities = mongoOperations.find(Query.query(criteria), RoleEntity.class);
            List<String> roleNames = roleEntities.stream().map(RoleEntity::getName).toList();
            List<String> roles = new ArrayList<>();
            roles.add("$everyone");
            roles.addAll(roleNames);

            // 优先使用 Registered Client 中配置的 Access Token TTL
            Duration accessTokenTTL = registeredClient.getTokenSettings().getAccessTokenTimeToLive();
            //Duration refreshTokenTTL = registeredClient.getTokenSettings().refreshTokenTimeToLive();

            Instant now = Instant.now();
            long createAt = now.toEpochMilli();
            long expireDate = createAt + 14 * 24 * 60 * 60 * 1000;
            if (accessTokenTTL != null) {
                Instant expireDateInstant = now.plus(accessTokenTTL);
                expireDate = expireDateInstant.toEpochMilli();
            }

            context.getClaims().claim("clientId", registeredClient.getClientId())
                    .claim("createdAt", createAt)
                    .claim("roles", roles)
                    .claim("expiredate", expireDate);

            // authorization code 方式认证时，可以拿到用户信息, client impl
            if (oAuth2ClientAuthenticationToken instanceof UsernamePasswordAuthenticationToken) {
                UserDetail userDetail = (UserDetail) oAuth2ClientAuthenticationToken.getPrincipal();
                context.getClaims().claim("user_id", userDetail.getUserId())
                        .claim("email", userDetail.getEmail());
            }
        };
    }

    /**
     * OAuth2 授权服务器安全过滤器链
     */
    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public SecurityFilterChain authorizationServerSecurityFilterChain(HttpSecurity http) throws Exception {
        OAuth2AuthorizationServerConfigurer authorizationServerConfigurer = OAuth2AuthorizationServerConfigurer.authorizationServer();
        RequestMatcher endpointsMatcher = authorizationServerConfigurer.getEndpointsMatcher();
        http.securityMatcher(endpointsMatcher)
                .authorizeHttpRequests(authorizeRequests -> authorizeRequests
                        .anyRequest().authenticated())
                .with(authorizationServerConfigurer, Customizer.withDefaults())
                .headers(headers -> {
                    headers.frameOptions((HeadersConfigurer.FrameOptionsConfig::sameOrigin));
                })
                .csrf(AbstractHttpConfigurer::disable);
        return http.build();
    }

    /**
     * 默认安全过滤器链
     */
    @Bean
    @Order(2)
    public SecurityFilterChain defaultSecurityFilterChain(HttpSecurity http) throws Exception {
        http
                .authorizeHttpRequests(authorizeRequests -> authorizeRequests
                        .anyRequest().permitAll())
                .formLogin(Customizer.withDefaults())
                .headers(headers -> {
                    headers.frameOptions((HeadersConfigurer.FrameOptionsConfig::sameOrigin));
                })
                .csrf(AbstractHttpConfigurer::disable);
        return http.build();
    }

    /**
     * 对JWT进行签名的 加解密密钥
     */
    @Bean
    public JWKSource<SecurityContext> jwkSource() throws NoSuchAlgorithmException {
        /*KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048);
        KeyPair keyPair = keyPairGenerator.generateKeyPair();
        RSAPublicKey publicKey = (RSAPublicKey) keyPair.getPublic();
        RSAPrivateKey privateKey = (RSAPrivateKey) keyPair.getPrivate();*/

        try {
            // pkcs8
            InputStream input = AuthorizationConfig.class.getClassLoader().getResourceAsStream("oauth/private.pem");
            if (input == null) {
                throw new BizException("OAuth.RSAKey.NotFound");
            }

            String privateKeyData = IOUtils.toString(input, "UTF-8");
            IOUtils.closeQuietly(input);

            input = AuthorizationConfig.class.getClassLoader().getResourceAsStream("oauth/public.pem");
            if (input == null) {
                throw new BizException("OAuth.RSAKey.NotFound");
            }
            String publicKeyData = IOUtils.toString(input, "UTF-8");
            IOUtils.closeQuietly(input);

            privateKeyData = privateKeyData
                    .replace("-----BEGIN PRIVATE KEY-----", "")
                    .replaceAll(System.lineSeparator(), "")
                    .replace("-----END PRIVATE KEY-----", "");
            publicKeyData = publicKeyData
                    .replace("-----BEGIN PUBLIC KEY-----", "")
                    .replaceAll(System.lineSeparator(), "")
                    .replace("-----END PUBLIC KEY-----", "");

            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            PKCS8EncodedKeySpec encodedPrivateKeySpec = new PKCS8EncodedKeySpec(Base64.getMimeDecoder().decode(privateKeyData));
            X509EncodedKeySpec encodedPublicKeySpec = new X509EncodedKeySpec(Base64.getMimeDecoder().decode(publicKeyData));

            RSAPrivateKey privateKey = (RSAPrivateKey) keyFactory.generatePrivate(encodedPrivateKeySpec);
            RSAPublicKey publicKey = (RSAPublicKey) keyFactory.generatePublic(encodedPublicKeySpec) ;

            RSAKey rsaKey = new RSAKey.Builder(publicKey)
                    .privateKey(privateKey)
                    .keyID(UUID.randomUUID().toString())
                    .build();
            JWKSet jwkSet = new JWKSet(rsaKey);
            return (jwkSelector, securityContext) -> jwkSelector.select(jwkSet);

        } catch (IOException e) {
            log.error("Load oauth RSA key failed", e);
            throw new BizException("OAuth.RSAKey.NotFound", e);
        } catch (InvalidKeySpecException e) {
            log.error("Invalid oauth RSA key", e);
            throw new BizException("OAuth.RSAKey.InvalidKey", e);
        }

    }

    /**
     * jwt 解码
     */
    @Bean
    public JwtDecoder jwtDecoder(JWKSource<SecurityContext> jwkSource) {
        return OAuth2AuthorizationServerConfiguration.jwtDecoder(jwkSource);
    }

    /**
     * 配置一些端点的路径，比如：获取token、授权端点 等
     */
    @Bean
    public AuthorizationServerSettings authorizationServerSettings() {
        return AuthorizationServerSettings.builder()
                .tokenEndpoint("/oauth/token")
                .authorizationEndpoint("/oauth/authorize")
                .tokenRevocationEndpoint("/oauth/revoke")
                .tokenIntrospectionEndpoint("/oauth/introspect")
                .issuer("http://127.0.0.1:3000")
                .build();
    }

    /**
     * 密码编码器 - 用于 OAuth2 客户端密码
     */
    @Bean
    public PasswordEncoder passwordEncoder() {
        // 对于演示目的使用 NoOpPasswordEncoder，生产环境建议使用 BCryptPasswordEncoder
        return NoOpPasswordEncoder.getInstance();
    }
    /*@PostConstruct
    public void oAuth2ClientAuthenticationProvider() {

        oAuth2ClientAuthenticationProvider.setPasswordEncoder(new PasswordEncoder() {
            @Override
            public String encode(CharSequence rawPassword) {
                return rawPassword.toString();
            }

            @Override
            public boolean matches(CharSequence rawPassword, String encodedPassword) {
                return rawPassword.toString().equals(encodedPassword);
            }
        });
    }*/



}
