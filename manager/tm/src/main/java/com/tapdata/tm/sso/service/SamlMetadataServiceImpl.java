package com.tapdata.tm.sso.service;

import com.tapdata.tm.sso.dto.IdpMetadata;
import com.tapdata.tm.sso.dto.SamlConfig;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

/**
 * Hardened, dependency-light implementation of {@link SamlMetadataService}.
 * <p>
 * IdP metadata is parsed with a {@link DocumentBuilderFactory} configured to reject
 * DTDs and external entities so that malicious metadata cannot trigger XXE (AC-054).
 */
@Service
public class SamlMetadataServiceImpl implements SamlMetadataService {

    private static final String NS_MD = "urn:oasis:names:tc:SAML:2.0:metadata";
    private static final String NS_DS = "http://www.w3.org/2000/09/xmldsig#";
    private static final String BINDING_REDIRECT = "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect";
    private static final String BINDING_POST = "urn:oasis:names:tc:SAML:2.0:bindings:HTTP-POST";

    @Override
    public IdpMetadata parseIdpMetadata(String metadataXml) {
        if (StringUtils.isBlank(metadataXml)) {
            throw new IllegalArgumentException("IdP metadata XML is empty");
        }
        Document doc = parseSecurely(metadataXml);
        Element entityDescriptor = firstElement(doc.getElementsByTagNameNS(NS_MD, "EntityDescriptor"));
        if (entityDescriptor == null) {
            throw new IllegalArgumentException("Not a valid SAML metadata document: EntityDescriptor missing");
        }
        Element idpDescriptor = firstElement(entityDescriptor.getElementsByTagNameNS(NS_MD, "IDPSSODescriptor"));
        if (idpDescriptor == null) {
            throw new IllegalArgumentException("Not an IdP metadata document: IDPSSODescriptor missing");
        }
        return IdpMetadata.builder()
                .idpEntityId(StringUtils.trimToNull(entityDescriptor.getAttribute("entityID")))
                .idpSsoUrl(serviceLocation(idpDescriptor, "SingleSignOnService"))
                .idpSloUrl(serviceLocation(idpDescriptor, "SingleLogoutService"))
                .idpSigningCertificate(signingCertificate(idpDescriptor))
                .build();
    }

    @Override
    public String buildSpMetadata(SamlConfig config) {
        if (config == null || StringUtils.isBlank(config.getSpEntityId())
                || StringUtils.isBlank(config.getSpAcsUrl())) {
            throw new IllegalStateException("SP entity ID and ACS URL are required to build SP metadata");
        }
        StringBuilder sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
        sb.append("<md:EntityDescriptor xmlns:md=\"").append(NS_MD).append("\" entityID=\"")
                .append(escape(config.getSpEntityId())).append("\">\n");
        sb.append("  <md:SPSSODescriptor AuthnRequestsSigned=\"").append(config.isSignAuthnRequest())
                .append("\" WantAssertionsSigned=\"").append(config.isWantAssertionsSigned())
                .append("\" protocolSupportEnumeration=\"urn:oasis:names:tc:SAML:2.0:protocol\">\n");
        String cert = stripPem(config.getSpCertificate());
        if (StringUtils.isNotBlank(cert)) {
            appendKeyDescriptor(sb, "signing", cert);
            appendKeyDescriptor(sb, "encryption", cert);
        }
        sb.append("    <md:AssertionConsumerService Binding=\"").append(BINDING_POST)
                .append("\" Location=\"").append(escape(config.getSpAcsUrl()))
                .append("\" index=\"0\" isDefault=\"true\"/>\n");
        sb.append("  </md:SPSSODescriptor>\n");
        sb.append("</md:EntityDescriptor>\n");
        return sb.toString();
    }

    private void appendKeyDescriptor(StringBuilder sb, String use, String cert) {
        sb.append("    <md:KeyDescriptor use=\"").append(use).append("\">\n");
        sb.append("      <ds:KeyInfo xmlns:ds=\"").append(NS_DS).append("\">\n");
        sb.append("        <ds:X509Data><ds:X509Certificate>").append(cert)
                .append("</ds:X509Certificate></ds:X509Data>\n");
        sb.append("      </ds:KeyInfo>\n");
        sb.append("    </md:KeyDescriptor>\n");
    }

    private String serviceLocation(Element descriptor, String serviceName) {
        NodeList services = descriptor.getElementsByTagNameNS(NS_MD, serviceName);
        String fallback = null;
        for (int i = 0; i < services.getLength(); i++) {
            Element service = (Element) services.item(i);
            String location = StringUtils.trimToNull(service.getAttribute("Location"));
            if (location == null) {
                continue;
            }
            if (BINDING_REDIRECT.equals(service.getAttribute("Binding"))) {
                return location;
            }
            if (fallback == null) {
                fallback = location;
            }
        }
        return fallback;
    }

    private String signingCertificate(Element idpDescriptor) {
        NodeList keyDescriptors = idpDescriptor.getElementsByTagNameNS(NS_MD, "KeyDescriptor");
        String fallback = null;
        for (int i = 0; i < keyDescriptors.getLength(); i++) {
            Element kd = (Element) keyDescriptors.item(i);
            String use = kd.getAttribute("use");
            String cert = firstCertificate(kd);
            if (cert == null) {
                continue;
            }
            if ("signing".equalsIgnoreCase(use) || StringUtils.isBlank(use)) {
                return cert;
            }
            if (fallback == null) {
                fallback = cert;
            }
        }
        return fallback;
    }

    private String firstCertificate(Element keyDescriptor) {
        Element certElement = firstElement(keyDescriptor.getElementsByTagNameNS(NS_DS, "X509Certificate"));
        if (certElement == null) {
            return null;
        }
        String raw = certElement.getTextContent();
        return raw == null ? null : raw.replaceAll("\\s", "");
    }

    private Element firstElement(NodeList nodes) {
        for (int i = 0; i < nodes.getLength(); i++) {
            Node node = nodes.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                return (Element) node;
            }
        }
        return null;
    }

    private Document parseSecurely(String xml) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
            factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
            factory.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            factory.setXIncludeAware(false);
            factory.setExpandEntityReferences(false);
            factory.setNamespaceAware(true);
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
            DocumentBuilder builder = factory.newDocumentBuilder();
            return builder.parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to parse IdP metadata XML: " + e.getMessage(), e);
        }
    }

    private String stripPem(String pem) {
        if (StringUtils.isBlank(pem)) {
            return null;
        }
        return pem.replaceAll("-----BEGIN CERTIFICATE-----", "")
                .replaceAll("-----END CERTIFICATE-----", "")
                .replaceAll("\\s", "");
    }

    private String escape(String value) {
        return value.replace("&", "&amp;").replace("<", "&lt;")
                .replace(">", "&gt;").replace("\"", "&quot;");
    }
}
