package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.dto.SamlAuthenticatedSubject;
import com.tapdata.tm.sso.dto.SamlConfig;
import com.tapdata.tm.sso.security.OpenSamlBootstrap;
import com.tapdata.tm.sso.security.SamlPemUtil;
import org.apache.commons.lang3.StringUtils;
import org.opensaml.core.xml.io.Unmarshaller;
import org.opensaml.saml.saml2.core.Assertion;
import org.opensaml.saml.saml2.core.Attribute;
import org.opensaml.saml.saml2.core.AttributeStatement;
import org.opensaml.saml.saml2.core.Audience;
import org.opensaml.saml.saml2.core.AudienceRestriction;
import org.opensaml.saml.saml2.core.AuthnStatement;
import org.opensaml.saml.saml2.core.Conditions;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.core.Response;
import org.opensaml.saml.saml2.core.StatusCode;
import org.opensaml.saml.saml2.core.Subject;
import org.opensaml.saml.saml2.core.SubjectConfirmation;
import org.opensaml.saml.saml2.core.SubjectConfirmationData;
import org.opensaml.security.x509.BasicX509Credential;
import org.opensaml.xmlsec.signature.Signature;
import org.opensaml.xmlsec.signature.support.SignatureValidator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * OpenSAML-backed {@link SamlResponseValidator}. Enforces signature, time-window,
 * audience, subject-confirmation and single-use checks before returning a trusted
 * subject. Never logs the raw assertion (AC-055).
 */
@Service
public class SamlResponseValidatorImpl implements SamlResponseValidator {

    private static final Set<String> WEAK_SIGNATURE_ALGS = new java.util.HashSet<>(java.util.Arrays.asList(
            "http://www.w3.org/2000/09/xmldsig#rsa-sha1",
            "http://www.w3.org/2000/09/xmldsig#dsa-sha1",
            "http://www.w3.org/2000/09/xmldsig#hmac-sha1",
            "http://www.w3.org/2001/04/xmldsig-more#rsa-md5"));

    @Autowired
    private SamlReplayCacheService replayCacheService;

    @Override
    public SamlAuthenticatedSubject validate(SamlConfig config, String base64SamlResponse, String expectedInResponseTo) {
        if (config == null) {
            throw new SamlValidationException("SAML is not configured");
        }
        if (StringUtils.isBlank(base64SamlResponse)) {
            throw new SamlValidationException("Missing SAMLResponse");
        }
        OpenSamlBootstrap.ensureInitialized();

        Response response = parseResponse(base64SamlResponse);
        checkStatus(response);

        X509Certificate idpCert = parseIdpCert(config);

        // If the Response itself is signed, verify it.
        if (response.getSignature() != null) {
            verifySignature(response.getSignature(), idpCert);
        }

        Assertion assertion = extractAssertion(response);

        boolean assertionSigned = assertion.getSignature() != null;
        if (assertionSigned) {
            verifySignature(assertion.getSignature(), idpCert);
        }
        // At least one signature (Response or Assertion) is mandatory when required.
        if (config.isWantAssertionsSigned() && !assertionSigned) {
            throw new SamlValidationException("Assertion is not signed but signed assertions are required");
        }
        if (response.getSignature() == null && !assertionSigned) {
            throw new SamlValidationException("Neither Response nor Assertion is signed");
        }

        Instant now = Instant.now();
        Duration skew = Duration.ofSeconds(Math.max(0, config.getClockSkewSeconds()));

        validateConditions(assertion.getConditions(), config, now, skew);
        validateSubjectConfirmation(assertion, config, now, skew, expectedInResponseTo);
        enforceReplay(assertion);

        return buildSubject(assertion, config);
    }

    private Response parseResponse(String base64SamlResponse) {
        try {
            byte[] xml = Base64.getMimeDecoder().decode(base64SamlResponse.trim());
            Document document = OpenSamlBootstrap.getParserPool()
                    .parse(new java.io.ByteArrayInputStream(xml));
            Element element = document.getDocumentElement();
            Unmarshaller unmarshaller = OpenSamlBootstrap.getUnmarshallerFactory().getUnmarshaller(element);
            return (Response) unmarshaller.unmarshall(element);
        } catch (Exception e) {
            throw new SamlValidationException("Unable to parse SAMLResponse", e);
        }
    }

    private void checkStatus(Response response) {
        if (response.getStatus() == null || response.getStatus().getStatusCode() == null) {
            throw new SamlValidationException("SAMLResponse has no status");
        }
        String value = response.getStatus().getStatusCode().getValue();
        if (!StatusCode.SUCCESS.equals(value)) {
            throw new SamlValidationException("SAMLResponse status is not Success");
        }
    }

    private X509Certificate parseIdpCert(SamlConfig config) {
        if (StringUtils.isBlank(config.getIdpSigningCertificate())) {
            throw new SamlValidationException("IdP signing certificate is not configured");
        }
        try {
            return SamlPemUtil.parseCertificate(config.getIdpSigningCertificate());
        } catch (Exception e) {
            throw new SamlValidationException("IdP signing certificate is invalid", e);
        }
    }

    private Assertion extractAssertion(Response response) {
        List<Assertion> assertions = response.getAssertions();
        if (assertions == null || assertions.isEmpty()) {
            // Encrypted assertions are not supported in this milestone.
            throw new SamlValidationException("SAMLResponse contains no (unencrypted) assertion");
        }
        return assertions.get(0);
    }

    private void verifySignature(Signature signature, X509Certificate idpCert) {
        String alg = signature.getSignatureAlgorithm();
        if (StringUtils.isNotBlank(alg) && WEAK_SIGNATURE_ALGS.contains(alg)) {
            throw new SamlValidationException("Weak signature algorithm is not allowed");
        }
        try {
            BasicX509Credential credential = new BasicX509Credential(idpCert);
            SignatureValidator.validate(signature, credential);
        } catch (Exception e) {
            throw new SamlValidationException("Signature validation failed", e);
        }
    }

    private void validateConditions(Conditions conditions, SamlConfig config, Instant now, Duration skew) {
        if (conditions == null) {
            throw new SamlValidationException("Assertion has no Conditions");
        }
        Instant notBefore = conditions.getNotBefore();
        if (notBefore != null && now.plus(skew).isBefore(notBefore)) {
            throw new SamlValidationException("Assertion not yet valid (NotBefore)");
        }
        Instant notOnOrAfter = conditions.getNotOnOrAfter();
        if (notOnOrAfter != null && !now.minus(skew).isBefore(notOnOrAfter)) {
            throw new SamlValidationException("Assertion has expired (NotOnOrAfter)");
        }
        validateAudience(conditions, config);
    }

    private void validateAudience(Conditions conditions, SamlConfig config) {
        String expected = config.getSpEntityId();
        if (StringUtils.isBlank(expected)) {
            return;
        }
        List<AudienceRestriction> restrictions = conditions.getAudienceRestrictions();
        if (restrictions == null || restrictions.isEmpty()) {
            throw new SamlValidationException("Assertion has no AudienceRestriction");
        }
        for (AudienceRestriction restriction : restrictions) {
            for (Audience audience : restriction.getAudiences()) {
                if (expected.equals(audience.getURI())) {
                    return;
                }
            }
        }
        throw new SamlValidationException("Assertion audience does not match SP entity id");
    }

    private void validateSubjectConfirmation(Assertion assertion, SamlConfig config, Instant now,
                                             Duration skew, String expectedInResponseTo) {
        Subject subject = assertion.getSubject();
        if (subject == null || subject.getSubjectConfirmations().isEmpty()) {
            throw new SamlValidationException("Assertion has no SubjectConfirmation");
        }
        boolean matched = false;
        for (SubjectConfirmation confirmation : subject.getSubjectConfirmations()) {
            SubjectConfirmationData data = confirmation.getSubjectConfirmationData();
            if (data == null) {
                continue;
            }
            Instant notOnOrAfter = data.getNotOnOrAfter();
            if (notOnOrAfter != null && !now.minus(skew).isBefore(notOnOrAfter)) {
                continue;
            }
            if (StringUtils.isNotBlank(config.getSpAcsUrl()) && StringUtils.isNotBlank(data.getRecipient())
                    && !config.getSpAcsUrl().equals(data.getRecipient())) {
                continue;
            }
            if (expectedInResponseTo != null
                    && !expectedInResponseTo.equals(data.getInResponseTo())) {
                continue;
            }
            if (expectedInResponseTo == null && StringUtils.isNotBlank(data.getInResponseTo())
                    && !config.isIdpInitiatedEnabled()) {
                // Solicited response replayed as unsolicited, or IdP-initiated disabled.
                continue;
            }
            matched = true;
            break;
        }
        if (!matched) {
            throw new SamlValidationException("SubjectConfirmation validation failed");
        }
    }

    private void enforceReplay(Assertion assertion) {
        String assertionId = assertion.getID();
        if (StringUtils.isBlank(assertionId)
                || !replayCacheService.recordIfFirstUse("assertion", assertionId)) {
            throw new SamlValidationException("Assertion has already been used (replay detected)");
        }
    }

    private SamlAuthenticatedSubject buildSubject(Assertion assertion, SamlConfig config) {
        SamlAuthenticatedSubject result = new SamlAuthenticatedSubject();
        NameID nameID = assertion.getSubject() == null ? null : assertion.getSubject().getNameID();
        if (nameID == null || StringUtils.isBlank(nameID.getValue())) {
            throw new SamlValidationException("Assertion has no NameID");
        }
        result.setNameId(nameID.getValue());
        result.setNameIdFormat(nameID.getFormat());
        result.setIdpEntityId(assertion.getIssuer() == null ? config.getIdpEntityId() : assertion.getIssuer().getValue());

        for (AuthnStatement authnStatement : assertion.getAuthnStatements()) {
            if (StringUtils.isNotBlank(authnStatement.getSessionIndex())) {
                result.setSessionIndex(authnStatement.getSessionIndex());
            }
            if (authnStatement.getSessionNotOnOrAfter() != null) {
                result.setSessionNotOnOrAfter(Date.from(authnStatement.getSessionNotOnOrAfter()));
            }
        }

        result.setAttributes(extractAttributes(assertion));
        return result;
    }

    private Map<String, List<String>> extractAttributes(Assertion assertion) {
        Map<String, List<String>> attributes = new LinkedHashMap<>();
        for (AttributeStatement statement : assertion.getAttributeStatements()) {
            for (Attribute attribute : statement.getAttributes()) {
                List<String> values = new ArrayList<>();
                attribute.getAttributeValues().forEach(value -> {
                    if (value.getDOM() != null) {
                        values.add(value.getDOM().getTextContent());
                    }
                });
                attributes.put(attribute.getName(), values);
            }
        }
        return attributes;
    }
}
