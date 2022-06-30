package com.tapdata.tm.oauth2.service;

import com.tapdata.tm.oauth2.entity.OAuth2AuthorizationConsentEntity;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.server.authorization.OAuth2AuthorizationConsent;
import org.springframework.security.oauth2.server.authorization.OAuth2AuthorizationConsentService;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClient;
import org.springframework.security.oauth2.server.authorization.client.RegisteredClientRepository;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/12/17 下午7:38
 */
@Service
public class MongoOAuth2AuthorizationConsentService implements OAuth2AuthorizationConsentService {

    private final MongoOperations mongoOperations;
    private final String collectionName;
    private final RegisteredClientRepository registeredClientRepository;

    public MongoOAuth2AuthorizationConsentService(MongoOperations mongoOperations, RegisteredClientRepository registeredClientRepository) {

        Assert.notNull(mongoOperations, "MongoOperations can't be empty.");

        this.mongoOperations = mongoOperations;
        this.registeredClientRepository = registeredClientRepository;
        this.collectionName = OAuth2AuthorizationConsent.class.getSimpleName();
    }

    @Override
    public void save(OAuth2AuthorizationConsent authorizationConsent) {
        mongoOperations.save(mapperEntity(authorizationConsent), collectionName);
    }

    private OAuth2AuthorizationConsentEntity mapperEntity(OAuth2AuthorizationConsent authorizationConsent) {
        if (authorizationConsent == null) return null;
        OAuth2AuthorizationConsentEntity entity = new OAuth2AuthorizationConsentEntity();
        entity.setRegisteredClientId(authorizationConsent.getRegisteredClientId());
        entity.setPrincipalName(authorizationConsent.getPrincipalName());
        List<String> authorities = Optional.ofNullable(authorizationConsent.getAuthorities()).orElse(Collections.emptySet())
                .stream().map(GrantedAuthority::getAuthority).collect(Collectors.toList());
        entity.setAuthorities(authorities);
        return entity;
    }

    private OAuth2AuthorizationConsent mapperEntity(OAuth2AuthorizationConsentEntity entity) {
        if (entity == null) return null;
        return OAuth2AuthorizationConsent.withId(entity.getRegisteredClientId(), entity.getPrincipalName()).authorities(authorities -> {
            Set<GrantedAuthority> _authorities = Optional.ofNullable(entity.getAuthorities()).orElse(Collections.emptyList())
                    .stream().map(SimpleGrantedAuthority::new).collect(Collectors.toSet());
            authorities.addAll(_authorities);
        }).build();
    }

    @Override
    public void remove(OAuth2AuthorizationConsent authorizationConsent) {
        mongoOperations.remove(
                Query.query(
                        Criteria.where("registeredClientId").is(authorizationConsent.getRegisteredClientId())
                        .and("principalName").is(authorizationConsent.getPrincipalName())
                ), collectionName);
    }

    @Override
    public OAuth2AuthorizationConsent findById(String registeredClientId, String principalName) {
        RegisteredClient registeredClient = registeredClientRepository.findById(registeredClientId);

        if (registeredClient == null) {
            throw new DataRetrievalFailureException(
                    "The RegisteredClient with id '" + registeredClientId + "' was not found in the RegisteredClientRepository.");
        }

        Criteria criteria = Criteria.where("registeredClientId").is(registeredClientId)
                .and("principalName").is(principalName);
        OAuth2AuthorizationConsentEntity entity = mongoOperations.findOne(Query.query(criteria), OAuth2AuthorizationConsentEntity.class, collectionName);
        return mapperEntity(entity);
    }
}
