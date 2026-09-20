package com.tapdata.tm.sso.security;

import jakarta.annotation.PostConstruct;
import net.shibboleth.shared.xml.impl.BasicParserPool;
import org.opensaml.core.config.InitializationService;
import org.opensaml.core.xml.XMLObjectBuilderFactory;
import org.opensaml.core.xml.config.XMLObjectProviderRegistrySupport;
import org.opensaml.core.xml.io.MarshallerFactory;
import org.opensaml.core.xml.io.UnmarshallerFactory;
import org.springframework.stereotype.Component;

/**
 * One-time OpenSAML 5 initialization. OpenSAML must have its module services
 * registered (builders / marshallers / unmarshallers / security config) before any
 * SAML object is created or parsed. This runs exactly once at startup and exposes
 * the shared factories used by the AuthnRequest builder and response validator.
 */
@Component
public class OpenSamlBootstrap {

    private static volatile boolean initialized = false;
    private static BasicParserPool parserPool;

    @PostConstruct
    public synchronized void init() {
        ensureInitialized();
    }

    /**
     * Idempotently initialize OpenSAML and a hardened parser pool.
     */
    public static synchronized void ensureInitialized() {
        if (initialized) {
            return;
        }
        try {
            InitializationService.initialize();
            BasicParserPool pool = new BasicParserPool();
            pool.setNamespaceAware(true);
            // Harden the parser against XXE (DOCTYPE / external entities disabled).
            pool.setExpandEntityReferences(false);
            pool.setXincludeAware(false);
            java.util.Map<String, Boolean> features = new java.util.HashMap<>();
            features.put("http://apache.org/xml/features/disallow-doctype-decl", true);
            features.put("http://xml.org/sax/features/external-general-entities", false);
            features.put("http://xml.org/sax/features/external-parameter-entities", false);
            pool.setBuilderFeatures(features);
            pool.initialize();
            parserPool = pool;
            initialized = true;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to initialize OpenSAML", e);
        }
    }

    public static XMLObjectBuilderFactory getBuilderFactory() {
        ensureInitialized();
        return XMLObjectProviderRegistrySupport.getBuilderFactory();
    }

    public static MarshallerFactory getMarshallerFactory() {
        ensureInitialized();
        return XMLObjectProviderRegistrySupport.getMarshallerFactory();
    }

    public static UnmarshallerFactory getUnmarshallerFactory() {
        ensureInitialized();
        return XMLObjectProviderRegistrySupport.getUnmarshallerFactory();
    }

    public static BasicParserPool getParserPool() {
        ensureInitialized();
        return parserPool;
    }
}
