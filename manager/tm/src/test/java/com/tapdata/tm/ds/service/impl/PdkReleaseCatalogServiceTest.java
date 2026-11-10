package com.tapdata.tm.ds.service.impl;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.nio.file.Files;
import java.time.Duration;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
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

    @TempDir
    Path directory;
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
        ReflectionTestUtils.setField(service, "trustedSha256", "unapproved-test-digest");
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
    void toPdkIdPreservesLegacyIdsForMssqlArtifacts() {
        assertEquals("sqlserver", ReflectionTestUtils.invokeMethod(service, "toPdkId", "mssql-connector"));
        assertEquals("aliyun-rds-sqlserver",
                ReflectionTestUtils.invokeMethod(service, "toPdkId", "aliyun-rds-mssql-connector"));
        assertEquals("tencent-db-sqlserver",
                ReflectionTestUtils.invokeMethod(service, "toPdkId", "tencent-db-mssql-connector"));
        assertEquals("oceanbase_oracle",
                ReflectionTestUtils.invokeMethod(service, "toPdkId", "oceanbase-oracle-connector"));
    }

    @Test
    void downloadJarVerifiesSha256() throws Exception {
        byte[] jar = "jar-content".getBytes(StandardCharsets.UTF_8);
        String sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(jar));
        String path = "/io/tapdata/mysql-connector/1.0.3/mysql-connector-1.0.3.jar";
        responses.put(path, jar);
        respond(path + ".sha256", sha.toUpperCase() + "  mysql-connector-1.0.3.jar\n");
        ReflectionTestUtils.setField(service, "trustedSha256", "0".repeat(64) + ",\n" + sha.toUpperCase());

        Path target = directory.resolve("connector.jar");
        java.util.List<Integer> progress = new java.util.ArrayList<>();
        service.downloadJar(Map.of("artifactId", "mysql-connector", "version", "1.0.3"), target, progress::add);
        assertArrayEquals(jar, Files.readAllBytes(target));
        assertEquals(100, progress.get(progress.size() - 1));
    }

    @Test
    void downloadJarFailsClosedWithoutDeploymentApproval() {
        ReflectionTestUtils.setField(service, "trustedSha256", "");
        Path target = directory.resolve("unapproved.jar");
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> service.downloadJar(RELEASE, target, ignored -> {}));
        assertTrue(failure.getMessage().contains("configure pdk.release.trusted-sha256"));
        assertFalse(Files.exists(target));
    }

    @Test
    void matchingNexusChecksumCannotApproveAnUntrustedArtifact() throws Exception {
        byte[] jar = "unapproved-code".getBytes(StandardCharsets.UTF_8);
        responses.put(ARTIFACT, jar);
        respond(ARTIFACT + ".sha256", HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(jar)));
        Path target = directory.resolve("unapproved.jar");
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> service.downloadJar(RELEASE, target, ignored -> {}));
        assertTrue(failure.getMessage().contains("absent from pdk.release.trusted-sha256"));
        assertFalse(Files.exists(target));
    }

    @Test
    void downloadJarRejectsChecksumMismatch() {
        String path = "/io/tapdata/mysql-connector/1.0.3/mysql-connector-1.0.3.jar";
        respond(path, "jar-content");
        respond(path + ".sha256", "0000");
        Map<String, Object> release = Map.of("artifactId", "mysql-connector", "version", "1.0.3");

        Path target = directory.resolve("connector.jar");
        assertThrows(IllegalStateException.class, () -> service.downloadJar(release, target, ignored -> {}));
        assertFalse(Files.exists(target));
    }
    private static final String ARTIFACT = "/io/tapdata/mysql-connector/1.0.3/mysql-connector-1.0.3.jar";
    private static final Map<String, Object> RELEASE = Map.of("artifactId", "mysql-connector", "version", "1.0.3");

    @Test
    void stalledArtifactBodyTimesOutAndDeletesPartialFile() throws Exception {
        assertStalledBodyTimesOut(ARTIFACT, true);
    }

    @Test
    void stalledMetadataBodyTimesOut() throws Exception {
        assertStalledBodyTimesOut(METADATA, false);
    }

    private void assertStalledBodyTimesOut(String path, boolean artifact) throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        server.createContext(path, exchange -> {
            try {
                exchange.sendResponseHeaders(200, 100);
                exchange.getResponseBody().write(1);
                exchange.getResponseBody().flush();
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                exchange.close();
            }
        });
        ReflectionTestUtils.setField(service, "readTimeoutMillis", 100);
        Path target = directory.resolve("partial.jar");
        try {
            assertTimeoutPreemptively(Duration.ofSeconds(3), () -> {
                if (artifact) {
                    assertThrows(IllegalStateException.class, () -> service.downloadJar(RELEASE, target, ignored -> {}));
                    assertFalse(Files.exists(target));
                } else {
                    assertThrows(IllegalStateException.class, service::findLatestReleases);
                }
            });
        } finally {
            release.countDown();
        }
    }

    @Test
    void oversizedContentLengthIsRejectedBeforeReadingArtifactBody() {
        server.createContext(ARTIFACT, exchange -> {
            exchange.sendResponseHeaders(200, (1L << 30) + 1);
            exchange.close();
        });
        Path target = directory.resolve("oversized.jar");
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> service.downloadJar(RELEASE, target, ignored -> {}));
        assertTrue(failure.getCause().getMessage().contains("1 GiB"));
        assertFalse(Files.exists(target));
    }

    @Test
    void chunkedResponseEnforcesStreamingLimitAndAcceptsExactLimit() throws Exception {
        byte[] body = new byte[33];
        server.createContext("/chunked", exchange -> {
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream output = exchange.getResponseBody()) { output.write(body); }
            exchange.close();
        });
        String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/chunked";
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        assertThrows(java.lang.reflect.UndeclaredThrowableException.class, () -> ReflectionTestUtils.invokeMethod(service, "fetchTo",
                url, output, (java.util.function.IntConsumer) ignored -> {}, 32L, Duration.ofSeconds(1)));
        output.reset();
        ReflectionTestUtils.invokeMethod(service, "fetchTo",
                url, output, (java.util.function.IntConsumer) ignored -> {}, 33L, Duration.ofSeconds(1));
        assertEquals(33, output.size());
    }

    @Test
    void slowTrickleCannotBypassOverallDeadline() {
        server.createContext(ARTIFACT, exchange -> {
            exchange.sendResponseHeaders(200, 1000);
            try (OutputStream output = exchange.getResponseBody()) {
                for (int i = 0; i < 1000; i++) {
                    output.write(1);
                    output.flush();
                    Thread.sleep(20);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException expectedDisconnect) {
                // Client aborts at the overall deadline even though the socket remains active.
            } finally {
                exchange.close();
            }
        });
        ReflectionTestUtils.setField(service, "downloadTimeout", Duration.ofMillis(150));
        Path target = directory.resolve("trickle.jar");
        assertTimeoutPreemptively(Duration.ofSeconds(3), () ->
                assertThrows(IllegalStateException.class, () -> service.downloadJar(RELEASE, target, ignored -> {})));
        assertFalse(Files.exists(target));
    }

}
