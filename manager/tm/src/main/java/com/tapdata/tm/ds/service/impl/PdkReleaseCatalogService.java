package com.tapdata.tm.ds.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

/** Reads the immutable connector release set published by connector-distribution. */
@Service
@Slf4j
public class PdkReleaseCatalogService {
    private static final String BOM_METADATA_PATH = "io/tapdata/connectors-bom/maven-metadata.xml";
    private static final String BOM_PATH = "io/tapdata/connectors-bom/%s/connectors-bom-%s.pom";
    private static final Pattern RELEASE_VERSION = Pattern.compile("\\d+(?:\\.\\d+)*");
    private static final String ARTIFACT_ID_KEY = "artifactId";
    private static final String VERSION_KEY = "version";

    @Value("${pdk.release.repository-url:https://nexus.tapdata.net/repository/maven-releases/}")
    private String repositoryUrl;

    private final AtomicReference<CacheEntry> cacheEntry = new AtomicReference<>();

    private static final long CACHE_TTL_MILLIS = Duration.ofMinutes(5).toMillis();

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build();

    private static final class CacheEntry {
        final List<Map<String, Object>> releases;
        final long timestamp;

        CacheEntry(List<Map<String, Object>> releases, long timestamp) {
            this.releases = releases;
            this.timestamp = timestamp;
        }
    }

    public List<Map<String, Object>> findLatestReleases() {
        CacheEntry cached = cacheEntry.get();
        if (cached != null && System.currentTimeMillis() - cached.timestamp < CACHE_TTL_MILLIS) {
            return cached.releases;
        }
        try {
            String repository = repositoryUrl.endsWith("/") ? repositoryUrl : repositoryUrl + "/";
            Document metadata = parse(fetch(repository + BOM_METADATA_PATH));
            String bomVersion = text(metadata.getDocumentElement(), "release");
            if (bomVersion == null || !RELEASE_VERSION.matcher(bomVersion).matches()) {
                bomVersion = text(metadata.getDocumentElement(), "latest");
            }
            if (bomVersion == null || !RELEASE_VERSION.matcher(bomVersion).matches()) {
                throw new IllegalStateException("Nexus connectors BOM has no formal release version");
            }

            Document bom = parse(fetch(repository + String.format(BOM_PATH, bomVersion, bomVersion)));
            Map<String, String> pdkApiVersions = new HashMap<>();
            NodeList properties = bom.getElementsByTagName("properties");
            if (properties.getLength() > 0) {
                NodeList entries = properties.item(0).getChildNodes();
                for (int i = 0; i < entries.getLength(); i++) {
                    Node entry = entries.item(i);
                    String key = entry.getNodeName();
                    String prefix = "tapdata.connector.";
                    String suffix = ".pdk.api.version";
                    if (key.startsWith(prefix) && key.endsWith(suffix)) {
                        pdkApiVersions.put(key.substring(prefix.length(), key.length() - suffix.length()), entry.getTextContent());
                    }
                }
            }

            List<Map<String, Object>> releases = new ArrayList<>();
            NodeList dependencies = bom.getElementsByTagName("dependency");
            for (int i = 0; i < dependencies.getLength(); i++) {
                Element dependency = (Element) dependencies.item(i);
                String groupId = text(dependency, "groupId");
                String artifactId = text(dependency, ARTIFACT_ID_KEY);
                String version = text(dependency, VERSION_KEY);
                if (!"io.tapdata".equals(groupId) || artifactId == null || !artifactId.endsWith("-connector")
                        || version == null || !RELEASE_VERSION.matcher(version).matches()) {
                    continue;
                }
                String pdkId = toPdkId(artifactId);
                Map<String, Object> release = new HashMap<>();
                release.put(ARTIFACT_ID_KEY, artifactId);
                release.put("pdkId", pdkId);
                release.put(VERSION_KEY, version);
                release.put("pdkAPIVersion", pdkApiVersions.get(artifactId));
                release.put("bomVersion", bomVersion);
                releases.add(Collections.unmodifiableMap(release));
            }
            releases.sort(Comparator.comparing(release -> String.valueOf(release.get("pdkId"))));
            List<Map<String, Object>> immutableReleases = List.copyOf(releases);
            cacheEntry.set(new CacheEntry(immutableReleases, System.currentTimeMillis()));
            return immutableReleases;
        } catch (Exception e) {
            log.warn("Failed to read connector release catalog from Nexus", e);
            if (cached != null) {
                log.info("Using cached connector release catalog after Nexus request failed");
                return cached.releases;
            }
            throw new IllegalStateException("Failed to read connector release catalog from Nexus", e);
        }
    }

    public Map<String, Object> findRelease(String artifactId) {
        return findLatestReleases().stream()
                .filter(release -> artifactId.equals(release.get(ARTIFACT_ID_KEY)))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Connector has no published formal release"));
    }

    private String toPdkId(String artifactId) {
        // A few connector artifacts use a different PDK id for compatibility with
        // their existing registered definitions. Keep those ids stable when the
        // release catalog is joined with the locally registered connector list.
        return switch (artifactId) {
            case "aliyun-mongodb-connector" -> "aliyun-db-mongodb";
            case "aws-clickhouse-connector" -> "aws-clickhouse";
            case "huawei-cloud-gaussdb-connector" -> "huawei-gauss-db";
            case "kafka-avro-connector" -> "kafka_avro";
            case "kafka-enhanced-connector" -> "kafka_enhanced";
            case "mongodb-lower-connector" -> "mongodb3";
            case "oceanbase-mysql-connector" -> "oceanbase";
            case "opengauss-connector" -> "open-gauss";
            default -> artifactId.substring(0, artifactId.length() - "-connector".length());
        };
    }

    public byte[] downloadJar(Map<String, Object> release) {
        try {
            String repository = repositoryUrl.endsWith("/") ? repositoryUrl : repositoryUrl + "/";
            String artifactId = String.valueOf(release.get(ARTIFACT_ID_KEY));
            String version = String.valueOf(release.get(VERSION_KEY));
            String base = repository + "io/tapdata/" + artifactId + "/" + version + "/" + artifactId + "-" + version + ".jar";
            byte[] jarBytes = fetchBytes(base);
            String expectedSha256 = fetch(repository + "io/tapdata/" + artifactId + "/" + version + "/" + artifactId + "-" + version + ".jar.sha256").trim();
            String actualSha256 = java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(jarBytes));
            if (!actualSha256.equalsIgnoreCase(expectedSha256.split("\\s")[0])) {
                throw new IllegalStateException("Connector artifact checksum does not match Nexus SHA-256");
            }
            return jarBytes;
        } catch (Exception e) {
            log.error("Failed to download connector artifact from Nexus", e);
            throw new IllegalStateException("Failed to download connector artifact from Nexus", e);
        }
    }

    private String fetch(String url) throws Exception {
        return new String(fetchBytes(url), StandardCharsets.UTF_8);
    }

    private byte[] fetchBytes(String url) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .header("Accept", "application/xml, text/xml, application/octet-stream, text/plain")
                .GET()
                .build();
        HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() != 200) {
            throw new IllegalStateException("Nexus returned HTTP " + response.statusCode());
        }
        return response.body();
    }

    private Document parse(String xml) throws Exception {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        return factory.newDocumentBuilder().parse(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    }

    private String text(Element element, String tag) {
        NodeList nodes = element.getElementsByTagName(tag);
        if (nodes.getLength() == 0) return null;
        String value = nodes.item(0).getTextContent();
        return value == null || value.isBlank() ? null : value.trim();
    }
}
