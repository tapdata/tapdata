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
import java.net.HttpURLConnection;
import java.net.URI;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntConsumer;
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
    private static final long MAX_ARTIFACT_BYTES = 1L << 30;

    @Value("${pdk.release.repository-url:https://nexus.tapdata.net/repository/maven-releases/}")
    private String repositoryUrl;

    // Independent deployment approval; never populated from Nexus or a registration request.
    @Value("${pdk.release.trusted-sha256:}")
    private String trustedSha256 = "";

    private final AtomicReference<CacheEntry> cacheEntry = new AtomicReference<>();
    private final ReentrantLock refreshLock = new ReentrantLock();

    private static final long CACHE_TTL_MILLIS = Duration.ofMinutes(5).toMillis();

    private static final long MAX_METADATA_BYTES = 4L * 1024 * 1024;
    private static final long MAX_CHECKSUM_BYTES = 4096;
    private static final String DOWNLOAD_LIMIT_ERROR = "Connector artifact exceeds the 1 GiB download limit";
    private final Duration metadataTimeout = Duration.ofSeconds(10);
    private final Duration downloadTimeout = Duration.ofMinutes(15);
    @Value("${pdk.release.read-timeout-millis:30000}")
    private int readTimeoutMillis = 30_000;

    private static final class CacheEntry {
        final List<Map<String, Object>> releases;
        final long timestamp;

        CacheEntry(List<Map<String, Object>> releases, long timestamp) {
            this.releases = releases;
            this.timestamp = timestamp;
        }
    }

    /**
     * Refreshes synchronously using two HTTP requests, each with a ten-second timeout.
     * Only one caller performs Nexus I/O. Concurrent callers get the cached catalog,
     * or fail fast during the first load, so they never queue behind a slow refresh.
     */
    public List<Map<String, Object>> findLatestReleases() {
        CacheEntry cached = cacheEntry.get();
        if (cached != null && System.currentTimeMillis() - cached.timestamp < CACHE_TTL_MILLIS) {
            return cached.releases;
        }
        if (!refreshLock.tryLock()) {
            cached = cacheEntry.get();
            if (cached != null) {
                return cached.releases;
            }
            throw new IllegalStateException("Connector release catalog is loading. Please retry shortly.");
        }
        try {
            // Another caller may have refreshed between our first cache read and acquiring the lock.
            cached = cacheEntry.get();
            if (cached != null && System.currentTimeMillis() - cached.timestamp < CACHE_TTL_MILLIS) {
                return cached.releases;
            }
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
        } finally {
            refreshLock.unlock();
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
            case "aliyun-rds-mssql-connector" -> "aliyun-rds-sqlserver";
            case "aws-clickhouse-connector" -> "aws-clickhouse";
            case "huawei-cloud-gaussdb-connector" -> "huawei-gauss-db";
            case "kafka-avro-connector" -> "kafka_avro";
            case "kafka-enhanced-connector" -> "kafka_enhanced";
            case "mongodb-lower-connector" -> "mongodb3";
            case "mssql-connector" -> "sqlserver";
            case "oceanbase-mysql-connector" -> "oceanbase";
            case "oceanbase-oracle-connector" -> "oceanbase_oracle";
            case "opengauss-connector" -> "open-gauss";
            case "tencent-db-mssql-connector" -> "tencent-db-sqlserver";
            default -> artifactId.substring(0, artifactId.length() - "-connector".length());
        };
    }

    /** Streams the artifact to an owner-only temporary file owned by the caller. */
    public void downloadJar(Map<String, Object> release, Path target, IntConsumer downloadProgress) {
        try {
            if (trustedSha256.isBlank()) {
                throw new IllegalStateException("Connector execution is not approved: configure pdk.release.trusted-sha256 from an independently reviewed release");
            }
            String repository = repositoryUrl.endsWith("/") ? repositoryUrl : repositoryUrl + "/";
            String artifactId = String.valueOf(release.get(ARTIFACT_ID_KEY));
            String version = String.valueOf(release.get(VERSION_KEY));
            String base = repository + "io/tapdata/" + artifactId + "/" + version + "/" + artifactId + "-" + version + ".jar";
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (OutputStream output = new DigestOutputStream(Files.newOutputStream(target), digest)) {
                fetchTo(base, output, downloadProgress, MAX_ARTIFACT_BYTES, downloadTimeout);
            }
            String expectedSha256 = new String(fetchBytes(base + ".sha256", MAX_CHECKSUM_BYTES), StandardCharsets.UTF_8).trim();
            String actualSha256 = HexFormat.of().formatHex(digest.digest());
            if (!actualSha256.equalsIgnoreCase(expectedSha256.split("\\s")[0])) {
                throw new IllegalStateException("Connector artifact checksum does not match Nexus SHA-256");
            }
            boolean approved = java.util.Arrays.stream(trustedSha256.split("[,\\s]+"))
                    .anyMatch(actualSha256::equalsIgnoreCase);
            if (!approved) {
                throw new IllegalStateException("Connector execution is not approved: artifact SHA-256 is absent from pdk.release.trusted-sha256");
            }
        } catch (Exception e) {
            // The caller also owns cleanup after successful download and registration.
            try {
                Files.deleteIfExists(target);
            } catch (IOException cleanupFailure) {
                e.addSuppressed(cleanupFailure);
            }
            log.error("Failed to download connector artifact from Nexus", e);
            throw new IllegalStateException("Failed to download connector artifact from Nexus: " + e.getMessage(), e);
        }
    }

    private String fetch(String url) throws IOException {
        return new String(fetchBytes(url, MAX_METADATA_BYTES), StandardCharsets.UTF_8);
    }

    private byte[] fetchBytes(String url, long maxBytes) throws IOException {
        try (ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            fetchTo(url, output, ignored -> { }, maxBytes, metadataTimeout);
            return output.toByteArray();
        }
    }

    /** Socket idle timeout plus an overall deadline bound both headers and body reads. */
    private void fetchTo(String url, OutputStream output, IntConsumer downloadProgress,
                         long maxBytes, Duration timeout) throws IOException {
        HttpURLConnection connection = (HttpURLConnection) URI.create(url).toURL().openConnection();
        long deadline = System.nanoTime() + timeout.toNanos();
        connection.setInstanceFollowRedirects(false);
        connection.setConnectTimeout((int) Math.min(5000, timeout.toMillis()));
        connection.setReadTimeout((int) Math.min(Math.max(1, readTimeoutMillis), timeout.toMillis()));
        connection.setRequestProperty("Accept", "application/xml, text/xml, application/octet-stream, text/plain");
        try {
            int status = connection.getResponseCode();
            if (status != 200) {
                throw new IOException("Nexus returned HTTP " + status);
            }
            long contentLength = connection.getContentLengthLong();
            checkSize(contentLength, maxBytes);
            try (InputStream input = connection.getInputStream()) {
                byte[] buffer = new byte[8192];
                long total = 0;
                while (true) {
                    long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
                    if (remaining <= 0) throw new SocketTimeoutException("Nexus response exceeded its download deadline");
                    connection.setReadTimeout((int) Math.min(Math.max(1, readTimeoutMillis), remaining));
                    int read = input.read(buffer);
                    if (read == -1) break;
                    total += read;
                    checkSize(total, maxBytes);
                    output.write(buffer, 0, read);
                    if (contentLength > 0) {
                        downloadProgress.accept((int) Math.min(100, total * 100 / contentLength));
                    }
                }
                if (contentLength >= 0 && contentLength != total) {
                    throw new IOException("Nexus response ended before the declared Content-Length");
                }
                downloadProgress.accept(100);
            }
        } finally {
            connection.disconnect();
        }
    }

    private void checkSize(long size, long maxBytes) throws IOException {
        if (size > maxBytes) {
            throw new IOException(maxBytes == MAX_ARTIFACT_BYTES ? DOWNLOAD_LIMIT_ERROR : "Nexus metadata exceeds its size limit");
        }
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
