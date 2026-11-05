package com.tapdata.tm.ds.service.impl;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PdkReleaseCatalogServiceTest {
    private static final String METADATA = "/io/tapdata/connectors-bom/maven-metadata.xml";
    private static final String BOM = "/io/tapdata/connectors-bom/1.2.0/connectors-bom-1.2.0.pom";

    private HttpServer server;
    private final Map<String, byte[]> responses = new HashMap<>();
    private PdkReleaseCatalogService service;

    @BeforeEach
    void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            byte[] body = responses.get(exchange.getRequestURI().getPath());
            int status = body == null ? 404 : 200;
            exchange.sendResponseHeaders(status, body == null ? -1 : body.length);
            if (body != null) {
                try (OutputStream output = exchange.getResponseBody()) {
                    output.write(body);
                }
            }
            exchange.close();
        });
        server.start();
        service = new PdkReleaseCatalogService();
        ReflectionTestUtils.setField(service, "repositoryUrl",
                "http://127.0.0.1:" + server.getAddress().getPort());
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
    }

    private void respond(String path, String body) {
        responses.put(path, body.getBytes(StandardCharsets.UTF_8));
    }

    private void publishBom(String release) {
        respond(METADATA, "<metadata><versioning>" + release + "<latest>1.2.0</latest></versioning></metadata>");
        respond(BOM, "<project><properties>"
                + "<tapdata.connector.mysql-connector.pdk.api.version>2.0.0</tapdata.connector.mysql-connector.pdk.api.version>"
                + "<other.property>x</other.property>"
                + "</properties><dependencyManagement><dependencies>"
                + dependency("io.tapdata", "mysql-connector", "1.0.3")
                + dependency("io.tapdata", "kafka-avro-connector", "1.0.1")
                + dependency("io.tapdata", "opengauss-connector", "1.0.0")
                + dependency("io.tapdata", "snapshot-connector", "1.0-SNAPSHOT")
                + dependency("io.tapdata", "tapdata-api", "1.0.0")
                + dependency("org.other", "foo-connector", "1.0.0")
                + "</dependencies></dependencyManagement></project>");
    }

    private String dependency(String groupId, String artifactId, String version) {
        return "<dependency><groupId>" + groupId + "</groupId><artifactId>" + artifactId
                + "</artifactId><version>" + version + "</version></dependency>";
    }

    @Test
    void findLatestReleasesReadsFormalConnectorsFromBom() {
        publishBom("<release>1.2.0</release>");

        List<Map<String, Object>> releases = service.findLatestReleases();

        assertEquals(3, releases.size());
        assertEquals("kafka_avro", releases.get(0).get("pdkId"));
        assertEquals("mysql", releases.get(1).get("pdkId"));
        assertEquals("open-gauss", releases.get(2).get("pdkId"));
        Map<String, Object> mysql = releases.get(1);
        assertEquals("mysql-connector", mysql.get("artifactId"));
        assertEquals("1.0.3", mysql.get("version"));
        assertEquals("2.0.0", mysql.get("pdkAPIVersion"));
        assertEquals("1.2.0", mysql.get("bomVersion"));
        assertNull(releases.get(0).get("pdkAPIVersion"));
    }

    @Test
    void findLatestReleasesSkipsMissingAndBlankDependencyVersions() {
        publishBom("<release>1.2.0</release>");
        String bom = new String(responses.get(BOM), StandardCharsets.UTF_8);
        respond(BOM, bom.replace("</dependencies>",
                "<dependency><groupId>io.tapdata</groupId><artifactId>missing-connector</artifactId></dependency>"
                        + dependency("io.tapdata", "blank-connector", " ") + "</dependencies>"));

        assertEquals(3, service.findLatestReleases().size());
    }

    @Test
    void concurrentColdLoadFailsFastInsteadOfQueuingNexusRequests() throws Exception {
        verifyConcurrentRefresh(false);
    }

    @Test
    void concurrentRefreshReturnsStaleCatalogWithoutWaiting() throws Exception {
        verifyConcurrentRefresh(true);
    }

    private void verifyConcurrentRefresh(boolean withCache) throws Exception {
        publishBom("<release>1.2.0</release>");
        List<Map<String, Object>> first = withCache ? service.findLatestReleases() : null;
        if (withCache) {
            Object entry = ReflectionTestUtils.getField(service, "cacheEntry");
            Object cached = ((java.util.concurrent.atomic.AtomicReference<?>) entry).get();
            ReflectionTestUtils.setField(cached, "timestamp", 0L);
        }
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        server.createContext(METADATA, exchange -> {
            entered.countDown();
            try {
                if (!release.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Timed out waiting for concurrent caller");
                }
                byte[] body = responses.get(METADATA);
                exchange.sendResponseHeaders(200, body.length);
                try (OutputStream output = exchange.getResponseBody()) {
                    output.write(body);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                exchange.close();
            }
        });
        var executor = Executors.newSingleThreadExecutor();
        try {
            var refreshing = executor.submit(service::findLatestReleases);
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            if (withCache) {
                assertSame(first, service.findLatestReleases());
            } else {
                IllegalStateException error = assertThrows(IllegalStateException.class, service::findLatestReleases);
                assertEquals("Connector release catalog is loading. Please retry shortly.", error.getMessage());
            }
            release.countDown();
            assertEquals(3, refreshing.get(5, TimeUnit.SECONDS).size());
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
        assertEquals(3, service.findLatestReleases().size());
    }

    @Test
    void findLatestReleasesFallsBackToLatestWhenReleaseIsNotFormal() {
        publishBom("<release>1.3.0-SNAPSHOT</release>");

        assertEquals(3, service.findLatestReleases().size());
    }

    @Test
    void findLatestReleasesRejectsBomWithoutFormalVersion() {
        respond(METADATA, "<metadata><versioning><latest>1.3.0-SNAPSHOT</latest></versioning></metadata>");

        assertThrows(IllegalStateException.class, () -> service.findLatestReleases());
    }

    @Test
    void findLatestReleasesUsesCacheWithinTtl() {
        publishBom("<release>1.2.0</release>");
        List<Map<String, Object>> first = service.findLatestReleases();
        responses.clear();

        assertSame(first, service.findLatestReleases());
    }

    @Test
    void findLatestReleasesReturnsStaleCacheWhenNexusFails() {
        publishBom("<release>1.2.0</release>");
        List<Map<String, Object>> first = service.findLatestReleases();
        responses.clear();
        Object entry = ReflectionTestUtils.getField(service, "cacheEntry");
        Object cached = ((java.util.concurrent.atomic.AtomicReference<?>) entry).get();
        ReflectionTestUtils.setField(cached, "timestamp", 0L);

        assertSame(first, service.findLatestReleases());
    }

    @Test
    void findLatestReleasesFailsWithoutCacheWhenNexusFails() {
        assertThrows(IllegalStateException.class, () -> service.findLatestReleases());
    }

    @Test
    void findLatestReleasesRejectsDoctype() {
        respond(METADATA, "<!DOCTYPE metadata [<!ENTITY x \"y\">]><metadata>&x;</metadata>");

        assertThrows(IllegalStateException.class, () -> service.findLatestReleases());
    }

    @Test
    void findReleaseReturnsMatchingArtifactOrFails() {
        publishBom("<release>1.2.0</release>");

        assertEquals("mysql", service.findRelease("mysql-connector").get("pdkId"));
        assertThrows(IllegalArgumentException.class, () -> service.findRelease("missing-connector"));
    }

    @Test
    void downloadJarVerifiesSha256() throws Exception {
        byte[] jar = "jar-content".getBytes(StandardCharsets.UTF_8);
        String sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(jar));
        String path = "/io/tapdata/mysql-connector/1.0.3/mysql-connector-1.0.3.jar";
        responses.put(path, jar);
        respond(path + ".sha256", sha.toUpperCase() + "  mysql-connector-1.0.3.jar\n");

        assertArrayEquals(jar, service.downloadJar(Map.of("artifactId", "mysql-connector", "version", "1.0.3")));
    }

    @Test
    void downloadJarRejectsChecksumMismatch() {
        String path = "/io/tapdata/mysql-connector/1.0.3/mysql-connector-1.0.3.jar";
        respond(path, "jar-content");
        respond(path + ".sha256", "0000");
        Map<String, Object> release = Map.of("artifactId", "mysql-connector", "version", "1.0.3");

        assertThrows(IllegalStateException.class, () -> service.downloadJar(release));
    }
}
