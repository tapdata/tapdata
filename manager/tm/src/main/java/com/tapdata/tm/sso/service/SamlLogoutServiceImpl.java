package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.dto.InboundLogout;
import com.tapdata.tm.sso.dto.LogoutRedirectResult;
import com.tapdata.tm.sso.dto.SamlConfig;
import com.tapdata.tm.sso.security.OpenSamlBootstrap;
import com.tapdata.tm.sso.security.SamlPemUtil;
import org.apache.commons.lang3.StringUtils;
import org.opensaml.core.xml.XMLObject;
import org.opensaml.core.xml.io.Marshaller;
import org.opensaml.core.xml.io.Unmarshaller;
import org.opensaml.saml.common.SAMLObjectBuilder;
import org.opensaml.saml.saml2.core.Issuer;
import org.opensaml.saml.saml2.core.LogoutRequest;
import org.opensaml.saml.saml2.core.LogoutResponse;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.core.SessionIndex;
import org.opensaml.saml.saml2.core.Status;
import org.opensaml.saml.saml2.core.StatusCode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.StringWriter;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

/**
 * OpenSAML-backed {@link SamlLogoutService}. Builds standards-compliant LogoutRequest /
 * LogoutResponse messages for the HTTP-Redirect binding (DEFLATE + Base64 + URL-encode,
 * optionally signed with the SP private key) and validates inbound LogoutRequests against
 * the IdP signing certificate, rejecting weak algorithms and replayed message ids.
 */
@Service
public class SamlLogoutServiceImpl implements SamlLogoutService {

    private static final String REDIRECT_SIG_ALG = "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256";

    private static final Set<String> WEAK_SIGNATURE_ALGS = Set.of(
            "http://www.w3.org/2000/09/xmldsig#rsa-sha1",
            "http://www.w3.org/2000/09/xmldsig#dsa-sha1",
            "http://www.w3.org/2000/09/xmldsig#hmac-sha1",
            "http://www.w3.org/2001/04/xmldsig-more#rsa-md5");

    @Autowired
    private SamlReplayCacheService replayCacheService;

    @Override
    public LogoutRedirectResult buildLogoutRequest(SamlConfig config, String nameId, String sessionIndex,
                                                   String relayState) {
        if (config == null || StringUtils.isBlank(config.getIdpSloUrl())) {
            throw new IllegalStateException("SAML IdP SLO URL is not configured");
        }
        if (StringUtils.isBlank(nameId)) {
            throw new IllegalStateException("NameID is required to build a LogoutRequest");
        }
        OpenSamlBootstrap.ensureInitialized();
        try {
            LogoutRequest logoutRequest = buildLogoutRequestObject(config, nameId, sessionIndex);
            String encoded = deflateAndBase64(marshall(logoutRequest));
            String redirectUrl = buildRedirect(config, "SAMLRequest", encoded, relayState);
            return new LogoutRedirectResult(redirectUrl, logoutRequest.getID());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build SAML LogoutRequest", e);
        }
    }

    @Override
    public InboundLogout parseLogoutRequest(SamlConfig config, String samlRequest, String sigAlg,
                                            String signature, String signedQuery) {
        if (config == null) {
            throw new SamlValidationException("SAML is not configured");
        }
        if (StringUtils.isBlank(samlRequest)) {
            throw new SamlValidationException("Missing SAMLRequest");
        }
        OpenSamlBootstrap.ensureInitialized();
        LogoutRequest logoutRequest = (LogoutRequest) inflateAndUnmarshall(samlRequest);

        verifyRedirectSignature(config, sigAlg, signature, signedQuery);
        enforceReplay(logoutRequest.getID());

        NameID nameID = logoutRequest.getNameID();
        if (nameID == null || StringUtils.isBlank(nameID.getValue())) {
            throw new SamlValidationException("LogoutRequest has no NameID");
        }
        InboundLogout result = new InboundLogout();
        result.setRequestId(logoutRequest.getID());
        result.setNameId(nameID.getValue());
        result.setIssuer(logoutRequest.getIssuer() == null ? null : logoutRequest.getIssuer().getValue());
        List<SessionIndex> indexes = logoutRequest.getSessionIndexes();
        if (indexes != null && !indexes.isEmpty()) {
            result.setSessionIndex(indexes.get(0).getValue());
        }
        return result;
    }

    @Override
    public LogoutRedirectResult buildLogoutResponse(SamlConfig config, String inResponseTo, String relayState) {
        if (config == null || StringUtils.isBlank(config.getIdpSloUrl())) {
            throw new IllegalStateException("SAML IdP SLO URL is not configured");
        }
        OpenSamlBootstrap.ensureInitialized();
        try {
            LogoutResponse logoutResponse = buildLogoutResponseObject(config, inResponseTo);
            String encoded = deflateAndBase64(marshall(logoutResponse));
            String redirectUrl = buildRedirect(config, "SAMLResponse", encoded, relayState);
            return new LogoutRedirectResult(redirectUrl, null);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build SAML LogoutResponse", e);
        }
    }

    @SuppressWarnings("unchecked")
    private LogoutRequest buildLogoutRequestObject(SamlConfig config, String nameId, String sessionIndex) {
        SAMLObjectBuilder<LogoutRequest> builder = (SAMLObjectBuilder<LogoutRequest>)
                OpenSamlBootstrap.getBuilderFactory().getBuilder(LogoutRequest.DEFAULT_ELEMENT_NAME);
        LogoutRequest request = builder.buildObject();
        request.setID("_" + UUID.randomUUID());
        request.setIssueInstant(Instant.now());
        request.setDestination(config.getIdpSloUrl());
        request.setIssuer(buildIssuer(config));

        SAMLObjectBuilder<NameID> nameIdBuilder = (SAMLObjectBuilder<NameID>)
                OpenSamlBootstrap.getBuilderFactory().getBuilder(NameID.DEFAULT_ELEMENT_NAME);
        NameID nameID = nameIdBuilder.buildObject();
        nameID.setValue(nameId);
        if (StringUtils.isNotBlank(config.getNameIdFormat())) {
            nameID.setFormat(config.getNameIdFormat());
        }
        request.setNameID(nameID);

        if (StringUtils.isNotBlank(sessionIndex)) {
            SAMLObjectBuilder<SessionIndex> indexBuilder = (SAMLObjectBuilder<SessionIndex>)
                    OpenSamlBootstrap.getBuilderFactory().getBuilder(SessionIndex.DEFAULT_ELEMENT_NAME);
            SessionIndex index = indexBuilder.buildObject();
            index.setValue(sessionIndex);
            request.getSessionIndexes().add(index);
        }
        return request;
    }

    @SuppressWarnings("unchecked")
    private LogoutResponse buildLogoutResponseObject(SamlConfig config, String inResponseTo) {
        SAMLObjectBuilder<LogoutResponse> builder = (SAMLObjectBuilder<LogoutResponse>)
                OpenSamlBootstrap.getBuilderFactory().getBuilder(LogoutResponse.DEFAULT_ELEMENT_NAME);
        LogoutResponse response = builder.buildObject();
        response.setID("_" + UUID.randomUUID());
        response.setIssueInstant(Instant.now());
        response.setDestination(config.getIdpSloUrl());
        if (StringUtils.isNotBlank(inResponseTo)) {
            response.setInResponseTo(inResponseTo);
        }
        response.setIssuer(buildIssuer(config));

        SAMLObjectBuilder<Status> statusBuilder = (SAMLObjectBuilder<Status>)
                OpenSamlBootstrap.getBuilderFactory().getBuilder(Status.DEFAULT_ELEMENT_NAME);
        Status status = statusBuilder.buildObject();
        SAMLObjectBuilder<StatusCode> codeBuilder = (SAMLObjectBuilder<StatusCode>)
                OpenSamlBootstrap.getBuilderFactory().getBuilder(StatusCode.DEFAULT_ELEMENT_NAME);
        StatusCode code = codeBuilder.buildObject();
        code.setValue(StatusCode.SUCCESS);
        status.setStatusCode(code);
        response.setStatus(status);
        return response;
    }

    @SuppressWarnings("unchecked")
    private Issuer buildIssuer(SamlConfig config) {
        SAMLObjectBuilder<Issuer> issuerBuilder = (SAMLObjectBuilder<Issuer>)
                OpenSamlBootstrap.getBuilderFactory().getBuilder(Issuer.DEFAULT_ELEMENT_NAME);
        Issuer issuer = issuerBuilder.buildObject();
        issuer.setValue(config.getSpEntityId());
        return issuer;
    }

    private String buildRedirect(SamlConfig config, String messageParam, String encoded, String relayState)
            throws Exception {
        StringBuilder query = new StringBuilder();
        query.append(messageParam).append("=").append(urlEncode(encoded));
        if (StringUtils.isNotBlank(relayState)) {
            query.append("&RelayState=").append(urlEncode(relayState));
        }
        if (config.isSignAuthnRequest()) {
            query.append("&SigAlg=").append(urlEncode(REDIRECT_SIG_ALG));
            String signature = signQuery(query.toString(), config.getSpPrivateKey());
            query.append("&Signature=").append(urlEncode(signature));
        }
        String separator = config.getIdpSloUrl().contains("?") ? "&" : "?";
        return config.getIdpSloUrl() + separator + query;
    }

    private String marshall(XMLObject object) throws Exception {
        Marshaller marshaller = OpenSamlBootstrap.getMarshallerFactory().getMarshaller(object);
        Element element = marshaller.marshall(object);
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

    private XMLObject inflateAndUnmarshall(String base64) {
        try {
            byte[] deflated = Base64.getMimeDecoder().decode(base64.trim());
            Inflater inflater = new Inflater(true);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (InflaterInputStream in = new InflaterInputStream(new ByteArrayInputStream(deflated), inflater)) {
                byte[] buffer = new byte[1024];
                int read;
                while ((read = in.read(buffer)) != -1) {
                    out.write(buffer, 0, read);
                }
            }
            Document document = OpenSamlBootstrap.getParserPool().parse(new ByteArrayInputStream(out.toByteArray()));
            Element element = document.getDocumentElement();
            Unmarshaller unmarshaller = OpenSamlBootstrap.getUnmarshallerFactory().getUnmarshaller(element);
            return unmarshaller.unmarshall(element);
        } catch (Exception e) {
            throw new SamlValidationException("Unable to parse SAML logout message", e);
        }
    }

    private void verifyRedirectSignature(SamlConfig config, String sigAlg, String signature, String signedQuery) {
        if (StringUtils.isBlank(signature)) {
            // Unsigned inbound LogoutRequest: only accept when the SP does not require signing.
            if (config.isSignAuthnRequest()) {
                throw new SamlValidationException("LogoutRequest is not signed but signing is required");
            }
            return;
        }
        if (StringUtils.isNotBlank(sigAlg) && WEAK_SIGNATURE_ALGS.contains(sigAlg)) {
            throw new SamlValidationException("Weak signature algorithm is not allowed");
        }
        if (StringUtils.isBlank(config.getIdpSigningCertificate())) {
            throw new SamlValidationException("IdP signing certificate is not configured");
        }
        try {
            X509Certificate cert = SamlPemUtil.parseCertificate(config.getIdpSigningCertificate());
            PublicKey publicKey = cert.getPublicKey();
            Signature verifier = Signature.getInstance("SHA256withRSA");
            verifier.initVerify(publicKey);
            verifier.update(signedQuery.getBytes(StandardCharsets.UTF_8));
            if (!verifier.verify(Base64.getDecoder().decode(signature))) {
                throw new SamlValidationException("LogoutRequest signature validation failed");
            }
        } catch (SamlValidationException e) {
            throw e;
        } catch (Exception e) {
            throw new SamlValidationException("LogoutRequest signature validation failed", e);
        }
    }

    private void enforceReplay(String messageId) {
        if (StringUtils.isBlank(messageId)
                || !replayCacheService.recordIfFirstUse("logoutrequest", messageId)) {
            throw new SamlValidationException("LogoutRequest has already been used (replay detected)");
        }
    }

    private String signQuery(String query, String privateKeyPem) throws Exception {
        if (StringUtils.isBlank(privateKeyPem)) {
            throw new IllegalStateException("Logout signing enabled but SP private key is not configured");
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
