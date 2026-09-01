package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.dto.SamlConfigForm;
import com.tapdata.tm.sso.dto.SamlValidationResult;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateFactory;
import java.net.URI;
import java.util.Locale;
import java.util.Set;

/**
 * Default {@link SamlValidationService}. Applies required-field, certificate and
 * algorithm-strength checks. Weak signature algorithms (SHA-1 / MD5) are rejected.
 */
@Service
public class SamlValidationServiceImpl implements SamlValidationService {

    private static final Set<String> WEAK_ALGORITHM_MARKERS = Set.of("sha1", "sha-1", "rsa-sha1", "md5", "dsa-sha1");

    @Override
    public SamlValidationResult validate(SamlConfigForm form) {
        SamlValidationResult result = new SamlValidationResult();
        if (form == null) {
            result.addError("Configuration is empty");
            return result.finish();
        }

        requireField(result, form.getSpEntityId(), "SP entity ID (spEntityId)");
        requireField(result, form.getSpAcsUrl(), "SP ACS URL (spAcsUrl)");
        requireField(result, form.getIdpEntityId(), "IdP entity ID (idpEntityId)");
        requireField(result, form.getIdpSsoUrl(), "IdP SSO URL (idpSsoUrl)");
        requireField(result, form.getIdpSigningCertificate(), "IdP signing certificate (idpSigningCertificate)");

        validateUrl(result, form.getSpAcsUrl(), "SP ACS URL");
        validateUrl(result, form.getSpSloUrl(), "SP SLO URL");
        validateUrl(result, form.getIdpSsoUrl(), "IdP SSO URL");
        validateUrl(result, form.getIdpSloUrl(), "IdP SLO URL");

        validateCertificate(result, form.getIdpSigningCertificate(), "IdP signing certificate");
        validateCertificate(result, form.getSpCertificate(), "SP certificate");
        validateAlgorithm(result, form.getSignatureAlgorithm());

        if (Boolean.FALSE.equals(form.getWantAssertionsSigned())) {
            result.addWarning("Assertion signing is disabled; enabling it is strongly recommended.");
        }
        return result.finish();
    }

    private void requireField(SamlValidationResult result, String value, String label) {
        if (StringUtils.isBlank(value)) {
            result.addError(label + " is required");
        }
    }

    private void validateUrl(SamlValidationResult result, String value, String label) {
        if (StringUtils.isBlank(value)) {
            return;
        }
        try {
            URI uri = URI.create(value.trim());
            boolean http = "http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme());
            if (!http || StringUtils.isBlank(uri.getHost()) || uri.getUserInfo() != null || uri.getFragment() != null) {
                result.addError(label + " must be a valid HTTP or HTTPS URL");
            }
        } catch (IllegalArgumentException e) {
            result.addError(label + " must be a valid HTTP or HTTPS URL");
        }
    }

    private void validateAlgorithm(SamlValidationResult result, String algorithm) {
        if (StringUtils.isBlank(algorithm)) {
            return;
        }
        String normalized = algorithm.toLowerCase(Locale.ROOT);
        for (String marker : WEAK_ALGORITHM_MARKERS) {
            if (normalized.contains(marker)) {
                result.addError("Weak signature algorithm is not allowed: " + algorithm);
                return;
            }
        }
    }

    private void validateCertificate(SamlValidationResult result, String pemOrBase64, String label) {
        if (StringUtils.isBlank(pemOrBase64)) {
            return;
        }
        try {
            String normalized = normalizeCertificate(pemOrBase64);
            CertificateFactory factory = CertificateFactory.getInstance("X.509");
            factory.generateCertificate(new ByteArrayInputStream(normalized.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            result.addError(label + " is not a valid X.509 certificate");
            if (StringUtils.isNotBlank(e.getMessage())) {
                result.addDetail(label + ": " + e.getMessage());
            }
        }
    }

    private String normalizeCertificate(String value) {
        String trimmed = value.trim();
        if (trimmed.contains("-----BEGIN CERTIFICATE-----")) {
            return trimmed;
        }
        String base64 = trimmed.replaceAll("\\s", "");
        return "-----BEGIN CERTIFICATE-----\n" + base64 + "\n-----END CERTIFICATE-----";
    }
}
