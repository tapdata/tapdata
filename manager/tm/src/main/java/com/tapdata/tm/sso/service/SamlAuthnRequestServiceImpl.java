package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.dto.AuthnRequestResult;
import com.tapdata.tm.sso.dto.SamlConfig;
import com.tapdata.tm.sso.security.OpenSamlBootstrap;
import com.tapdata.tm.sso.security.SamlPemUtil;
import org.apache.commons.lang3.StringUtils;
import org.opensaml.core.xml.io.Marshaller;
import org.opensaml.saml.common.SAMLObjectBuilder;
import org.opensaml.saml.common.xml.SAMLConstants;
import org.opensaml.saml.saml2.core.AuthnRequest;
import org.opensaml.saml.saml2.core.Issuer;
import org.opensaml.saml.saml2.core.NameIDPolicy;
import org.springframework.stereotype.Service;
import org.w3c.dom.Element;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.Signature;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;

/**
 * OpenSAML-backed {@link SamlAuthnRequestService}. Builds a standards-compliant
 * AuthnRequest and encodes it for the HTTP-Redirect binding (DEFLATE + Base64 +
 * URL-encode), optionally signing the redirect query with the SP private key.
 */
@Service
public class SamlAuthnRequestServiceImpl implements SamlAuthnRequestService {

    private static final String REDIRECT_SIG_ALG = "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256";

    @Override
    public AuthnRequestResult buildRedirect(SamlConfig config, String relayState) {
        if (config == null || StringUtils.isBlank(config.getIdpSsoUrl())) {
            throw new IllegalStateException("SAML IdP SSO URL is not configured");
        }
        OpenSamlBootstrap.ensureInitialized();
        try {
            AuthnRequest authnRequest = buildAuthnRequest(config);
            String xml = marshall(authnRequest);
            String samlRequest = deflateAndBase64(xml);

            StringBuilder query = new StringBuilder();
            query.append("SAMLRequest=").append(urlEncode(samlRequest));
            if (StringUtils.isNotBlank(relayState)) {
                query.append("&RelayState=").append(urlEncode(relayState));
            }

            if (config.isSignAuthnRequest()) {
                query.append("&SigAlg=").append(urlEncode(REDIRECT_SIG_ALG));
                String signature = signQuery(query.toString(), config.getSpPrivateKey());
                query.append("&Signature=").append(urlEncode(signature));
            }

            String separator = config.getIdpSsoUrl().contains("?") ? "&" : "?";
            String redirectUrl = config.getIdpSsoUrl() + separator + query;
            return new AuthnRequestResult(redirectUrl, authnRequest.getID());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build SAML AuthnRequest", e);
        }
    }

    @SuppressWarnings("unchecked")
    private AuthnRequest buildAuthnRequest(SamlConfig config) {
        SAMLObjectBuilder<AuthnRequest> builder = (SAMLObjectBuilder<AuthnRequest>)
                OpenSamlBootstrap.getBuilderFactory().getBuilder(AuthnRequest.DEFAULT_ELEMENT_NAME);
        AuthnRequest authnRequest = builder.buildObject();
        authnRequest.setID("_" + UUID.randomUUID());
        authnRequest.setIssueInstant(Instant.now());
        authnRequest.setDestination(config.getIdpSsoUrl());
        authnRequest.setProtocolBinding(SAMLConstants.SAML2_POST_BINDING_URI);
        authnRequest.setAssertionConsumerServiceURL(config.getSpAcsUrl());
        authnRequest.setForceAuthn(false);
        authnRequest.setIsPassive(false);

        SAMLObjectBuilder<Issuer> issuerBuilder = (SAMLObjectBuilder<Issuer>)
                OpenSamlBootstrap.getBuilderFactory().getBuilder(Issuer.DEFAULT_ELEMENT_NAME);
        Issuer issuer = issuerBuilder.buildObject();
        issuer.setValue(config.getSpEntityId());
        authnRequest.setIssuer(issuer);

        if (StringUtils.isNotBlank(config.getNameIdFormat())) {
            SAMLObjectBuilder<NameIDPolicy> policyBuilder = (SAMLObjectBuilder<NameIDPolicy>)
                    OpenSamlBootstrap.getBuilderFactory().getBuilder(NameIDPolicy.DEFAULT_ELEMENT_NAME);
            NameIDPolicy policy = policyBuilder.buildObject();
            policy.setFormat(config.getNameIdFormat());
            policy.setAllowCreate(true);
            authnRequest.setNameIDPolicy(policy);
        }
        return authnRequest;
    }

    private String marshall(AuthnRequest authnRequest) throws Exception {
        Marshaller marshaller = OpenSamlBootstrap.getMarshallerFactory().getMarshaller(authnRequest);
        Element element = marshaller.marshall(authnRequest);
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        StringWriter writer = new StringWriter();
        transformer.transform(new DOMSource(element), new StreamResult(writer));
        return writer.toString();
    }

    private String deflateAndBase64(String xml) throws Exception {
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        Deflater deflater = new Deflater(Deflater.DEFLATED, true);
        try (DeflaterOutputStream deflaterOut = new DeflaterOutputStream(bytesOut, deflater)) {
            deflaterOut.write(xml.getBytes(StandardCharsets.UTF_8));
        }
        deflater.end();
        return Base64.getEncoder().encodeToString(bytesOut.toByteArray());
    }

    private String signQuery(String query, String privateKeyPem) throws Exception {
        if (StringUtils.isBlank(privateKeyPem)) {
            throw new IllegalStateException("AuthnRequest signing enabled but SP private key is not configured");
        }
        PrivateKey privateKey = SamlPemUtil.parsePrivateKey(privateKeyPem);
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(privateKey);
        signature.update(query.getBytes(StandardCharsets.UTF_8));
        return Base64.getEncoder().encodeToString(signature.sign());
    }

    private String urlEncode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
