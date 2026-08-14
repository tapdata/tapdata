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
