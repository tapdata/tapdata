package com.tapdata.tm.config;

import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.SecurityContext;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.OAuth2AuthorizationServerConfiguration;
import org.springframework.security.config.annotation.web.configurers.oauth2.server.authorization.OAuth2AuthorizationServerConfigurer;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.server.authorization.JwtEncodingContext;
import org.springframework.security.oauth2.server.authorization.OAuth2TokenCustomizer;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClient;
import org.springframework.security.oauth2.server.authorization.config.ProviderSettings;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.util.matcher.RequestMatcher;

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

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/12/15 ??????2:37
 */
@Configuration
@Slf4j
public class AuthorizationConfig {

    /**
     * ????????? JWT token
     */
    public static class CustomOAuth2TokenCustomizer implements OAuth2TokenCustomizer<JwtEncodingContext> {

        @Override
        public void customize(JwtEncodingContext context) {
            RegisteredClient registeredClient = context.getRegisteredClient();
            Authentication oAuth2ClientAuthenticationToken = context.getPrincipal();
            Set<String> scopes = context.getAuthorizedScopes();
            List<String> roles = new ArrayList<>();
            roles.add("$everyone");
            roles.addAll(scopes);

            // ???????????? Registered Client ???????????? Access Token TTL
            Duration accessTokenTTL = registeredClient.getTokenSettings().accessTokenTimeToLive();
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

            // authorization code ??????????????????????????????????????????, client impl
            if (oAuth2ClientAuthenticationToken instanceof UsernamePasswordAuthenticationToken) {
                UserDetail userDetail = (UserDetail) oAuth2ClientAuthenticationToken.getPrincipal();
                context.getClaims().claim("user_id", userDetail.getUserId())
                        .claim("email", userDetail.getEmail());
            }
        }
    }

    /**
     * ?????? Spring Security ???????????????
     */
    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public SecurityFilterChain authorizationServerSecurityFilterChain(HttpSecurity http) throws Exception {
        http.setSharedObject(OAuth2TokenCustomizer.class, new CustomOAuth2TokenCustomizer());
        OAuth2AuthorizationServerConfigurer<HttpSecurity> authorizationServerConfigurer =
                new OAuth2AuthorizationServerConfigurer<>();
        RequestMatcher endpointsMatcher = authorizationServerConfigurer.getEndpointsMatcher();
        http.headers().frameOptions().sameOrigin();
        return http
                //.requestMatcher(endpointsMatcher)
                .csrf(csrf -> csrf.ignoringRequestMatchers(endpointsMatcher))
                .authorizeRequests(authorizeRequests -> authorizeRequests
                        // ???????????? /oauth/ path ????????????
                        .antMatchers("/oauth/**").authenticated()
                        .anyRequest().permitAll())

                .apply(authorizationServerConfigurer)
                .and()
                .formLogin()
                .and()
                .csrf().disable()
                .build();
    }

    /**
     * ???JWT??????????????? ???????????????
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
     * jwt ??????
     */
    @Bean
    public JwtDecoder jwtDecoder(JWKSource<SecurityContext> jwkSource) {
        return OAuth2AuthorizationServerConfiguration.jwtDecoder(jwkSource);
    }

    /**
     * ?????????????????????????????????????????????token??????????????? ???
     */
    @Bean
    public ProviderSettings providerSettings() {
        return new ProviderSettings()
                .tokenEndpoint("/oauth/token")
                .authorizationEndpoint("/oauth/authorize")
                .tokenRevocationEndpoint("/oauth/revoke")
                .tokenIntrospectionEndpoint("/oauth/introspect")
                // ????????????url??????,????????????????????????????????????
                .issuer("http://127.0.0.1:3000");
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
