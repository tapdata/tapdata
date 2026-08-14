package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.security.OpenSamlBootstrap;
import com.tapdata.tm.sso.security.SamlPemUtil;
import org.opensaml.core.xml.io.Marshaller;
import org.opensaml.saml.saml2.core.Assertion;
import org.opensaml.saml.saml2.core.Audience;
import org.opensaml.saml.saml2.core.AudienceRestriction;
import org.opensaml.saml.saml2.core.AuthnContext;
import org.opensaml.saml.saml2.core.AuthnContextClassRef;
import org.opensaml.saml.saml2.core.AuthnStatement;
import org.opensaml.saml.saml2.core.Conditions;
import org.opensaml.saml.saml2.core.Issuer;
import org.opensaml.saml.saml2.core.NameID;
import org.opensaml.saml.saml2.core.Response;
import org.opensaml.saml.saml2.core.Status;
import org.opensaml.saml.saml2.core.StatusCode;
import org.opensaml.saml.saml2.core.Subject;
import org.opensaml.saml.saml2.core.SubjectConfirmation;
import org.opensaml.saml.saml2.core.SubjectConfirmationData;
import org.opensaml.security.x509.BasicX509Credential;
import org.opensaml.xmlsec.signature.Signature;
import org.opensaml.xmlsec.signature.support.SignatureConstants;
import org.opensaml.xmlsec.signature.support.Signer;
import org.w3c.dom.Element;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Base64;

/**
 * Test-only helper that builds and signs SAML Responses so the validator can be
 * exercised end-to-end.
 */
final class SamlTestAssertions {

    private SamlTestAssertions() {
    }

    @SuppressWarnings("unchecked")
    static <T> T build(javax.xml.namespace.QName qName) {
        org.opensaml.core.xml.XMLObjectBuilder<?> builder =
                OpenSamlBootstrap.getBuilderFactory().getBuilder(qName);
        return (T) builder.buildObject(qName);
    }

    static String buildSignedResponseBase64(String certPem, String privateKeyPem, String spEntityId,
                                            String acsUrl, String idpEntityId, String nameId,
                                            String inResponseTo, Instant notOnOrAfter) throws Exception {
        OpenSamlBootstrap.ensureInitialized();
        Instant now = Instant.now();

        Assertion assertion = build(Assertion.DEFAULT_ELEMENT_NAME);
        assertion.setID("_assertion-" + java.util.UUID.randomUUID());
        assertion.setIssueInstant(now);

        Issuer issuer = build(Issuer.DEFAULT_ELEMENT_NAME);
        issuer.setValue(idpEntityId);
        assertion.setIssuer(issuer);

        NameID name = build(NameID.DEFAULT_ELEMENT_NAME);
        name.setValue(nameId);
        name.setFormat("urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress");
        Subject subject = build(Subject.DEFAULT_ELEMENT_NAME);
        subject.setNameID(name);
        SubjectConfirmation confirmation = build(SubjectConfirmation.DEFAULT_ELEMENT_NAME);
        confirmation.setMethod(SubjectConfirmation.METHOD_BEARER);
        SubjectConfirmationData data = build(SubjectConfirmationData.DEFAULT_ELEMENT_NAME);
        data.setRecipient(acsUrl);
        data.setNotOnOrAfter(notOnOrAfter);
        data.setInResponseTo(inResponseTo);
        confirmation.setSubjectConfirmationData(data);
        subject.getSubjectConfirmations().add(confirmation);
        assertion.setSubject(subject);

        Conditions conditions = build(Conditions.DEFAULT_ELEMENT_NAME);
        conditions.setNotBefore(now.minusSeconds(60));
        conditions.setNotOnOrAfter(notOnOrAfter);
        AudienceRestriction restriction = build(AudienceRestriction.DEFAULT_ELEMENT_NAME);
        Audience audience = build(Audience.DEFAULT_ELEMENT_NAME);
        audience.setURI(spEntityId);
        restriction.getAudiences().add(audience);
        conditions.getAudienceRestrictions().add(restriction);
        assertion.setConditions(conditions);

        AuthnStatement authnStatement = build(AuthnStatement.DEFAULT_ELEMENT_NAME);
        authnStatement.setAuthnInstant(now);
        authnStatement.setSessionIndex("session-index-1");
        AuthnContext authnContext = build(AuthnContext.DEFAULT_ELEMENT_NAME);
        AuthnContextClassRef classRef = build(AuthnContextClassRef.DEFAULT_ELEMENT_NAME);
        classRef.setURI("urn:oasis:names:tc:SAML:2.0:ac:classes:Password");
        authnContext.setAuthnContextClassRef(classRef);
        authnStatement.setAuthnContext(authnContext);
        assertion.getAuthnStatements().add(authnStatement);

        signAssertion(assertion, certPem, privateKeyPem);

        Response response = build(Response.DEFAULT_ELEMENT_NAME);
        response.setID("_response-" + java.util.UUID.randomUUID());
        response.setIssueInstant(now);
        response.setInResponseTo(inResponseTo);
        Issuer responseIssuer = build(Issuer.DEFAULT_ELEMENT_NAME);
        responseIssuer.setValue(idpEntityId);
        response.setIssuer(responseIssuer);
        Status status = build(Status.DEFAULT_ELEMENT_NAME);
        StatusCode statusCode = build(StatusCode.DEFAULT_ELEMENT_NAME);
        statusCode.setValue(StatusCode.SUCCESS);
        status.setStatusCode(statusCode);
        response.setStatus(status);
        response.getAssertions().add(assertion);

        String xml = marshall(response);
        return Base64.getEncoder().encodeToString(xml.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Build a valid signed Response, then mount a classic XML Signature Wrapping attack.
     * <p>
     * The genuine signed Assertion is moved verbatim into a wrapper element (so its bytes
     * are unchanged and its enveloped signature still verifies cryptographically, and it
     * remains first in document order for its ID). A forged Assertion carrying the same ID,
     * a verbatim COPY of the genuine {@code <Signature>}, and attacker-controlled claims is
     * placed as the Response's direct-child Assertion so {@code getAssertions().get(0)}
     * returns it. Apache Santuario's raw {@code SignatureValidator.validate()} passes
     * because the Reference {@code #ID} resolves (document order) to the untouched genuine
     * copy; only {@code SAMLSignatureProfileValidator} rejects it, because the signature's
     * Reference does not resolve to its own enclosing (forged) Assertion.
     */
    static String buildSignatureWrappingResponseBase64(String certPem, String privateKeyPem, String spEntityId,
                                                       String acsUrl, String idpEntityId, String legitNameId,
                                                       String forgedNameId, String inResponseTo,
                                                       Instant notOnOrAfter) throws Exception {
        String legit = new String(Base64.getDecoder().decode(buildSignedResponseBase64(certPem, privateKeyPem,
                spEntityId, acsUrl, idpEntityId, legitNameId, inResponseTo, notOnOrAfter)), StandardCharsets.UTF_8);

        // Locate the (namespace-prefix-agnostic) genuine signed Assertion element.
        java.util.regex.Matcher open = java.util.regex.Pattern
                .compile("<([a-zA-Z0-9]+:)?Assertion[\\s>]").matcher(legit);
        if (!open.find()) {
            throw new IllegalStateException("No Assertion element found in signed response");
        }
        int assertionStart = open.start();
        java.util.regex.Matcher close = java.util.regex.Pattern
                .compile("</([a-zA-Z0-9]+:)?Assertion>").matcher(legit);
        if (!close.find(assertionStart)) {
            throw new IllegalStateException("No Assertion end tag found in signed response");
        }
        int assertionEnd = close.end();
        // The genuine signed assertion, kept byte-for-byte identical so its signature verifies.
        String genuineAssertion = legit.substring(assertionStart, assertionEnd);

        // Extract the genuine assertion's own ID (the value its Signature Reference targets).
        java.util.regex.Matcher idMatcher = java.util.regex.Pattern
                .compile("<([a-zA-Z0-9]+:)?Assertion\\b[^>]*\\bID=\"([^\"]+)\"").matcher(genuineAssertion);
        if (!idMatcher.find()) {
            throw new IllegalStateException("No Assertion ID found in signed response");
        }
        String genuineId = idMatcher.group(2);

        // Forged assertion: a copy that keeps the genuine <Signature> verbatim (its Reference
        // still targets #genuineId) but is given a DIFFERENT own ID so the document has no
        // duplicate IDs (Santuario refuses to resolve references when IDs collide). Its own
        // Assertion ID therefore does NOT match the signature's Reference URI, and its own
        // content is not covered by the signature. Only SAMLSignatureProfileValidator catches
        // this: it requires the signature's Reference to resolve to the enclosing Assertion.
        String forgedAssertion = genuineAssertion
                .replaceFirst("(<([a-zA-Z0-9]+:)?Assertion\\b[^>]*\\bID=\")"
                                + java.util.regex.Pattern.quote(genuineId) + "\"",
                        "$1" + genuineId + "-forged\"")
                .replace(">" + legitNameId + "<", ">" + forgedNameId + "<");

        // Hide the genuine assertion inside a schema-valid samlp:Extensions element (whose
        // content model is ##other, so a saml2:Assertion is allowed). Extensions must sit
        // right after Issuer and before Status. Being earlier in document order, the genuine
        // copy is what #ID resolves to during Santuario's cryptographic validation, while the
        // Response's direct-child Assertion (the forged one) is what getAssertions().get(0)
        // returns and the application consumes.
        int statusIdx = legit.indexOf("<saml2p:Status>");
        if (statusIdx < 0 || statusIdx > assertionStart) {
            throw new IllegalStateException("Unexpected response layout (no Status before Assertion)");
        }
        String extensions = "<saml2p:Extensions>" + genuineAssertion + "</saml2p:Extensions>";

        StringBuilder sb = new StringBuilder(legit.length() + extensions.length());
        sb.append(legit, 0, statusIdx)          // ... </saml2:Issuer>
                .append(extensions)              // hidden genuine assertion (first in doc order)
                .append(legit, statusIdx, assertionStart)   // <saml2p:Status>...</saml2p:Status>
                .append(forgedAssertion)         // forged direct-child assertion (consumed)
                .append(legit, assertionEnd, legit.length()); // </saml2p:Response>
        return Base64.getEncoder().encodeToString(sb.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static void signAssertion(Assertion assertion, String certPem, String privateKeyPem) throws Exception {
        X509Certificate certificate = SamlPemUtil.parseCertificate(certPem);
        PrivateKey privateKey = SamlPemUtil.parsePrivateKey(privateKeyPem);
        BasicX509Credential credential = new BasicX509Credential(certificate, privateKey);

        Signature signature = build(Signature.DEFAULT_ELEMENT_NAME);
        signature.setSigningCredential(credential);
        signature.setSignatureAlgorithm(SignatureConstants.ALGO_ID_SIGNATURE_RSA_SHA256);
        signature.setCanonicalizationAlgorithm(SignatureConstants.ALGO_ID_C14N_EXCL_OMIT_COMMENTS);
        assertion.setSignature(signature);

        OpenSamlBootstrap.getMarshallerFactory().getMarshaller(assertion).marshall(assertion);
        Signer.signObject(signature);
    }

    private static String marshall(Response response) throws Exception {
        Marshaller marshaller = OpenSamlBootstrap.getMarshallerFactory().getMarshaller(response);
        Element element = marshaller.marshall(response);
        Transformer transformer = TransformerFactory.newInstance().newTransformer();
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        StringWriter writer = new StringWriter();
        transformer.transform(new DOMSource(element), new StreamResult(writer));
        return writer.toString();
    }
}
