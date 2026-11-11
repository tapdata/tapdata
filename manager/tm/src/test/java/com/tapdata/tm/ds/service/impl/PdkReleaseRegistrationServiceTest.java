package com.tapdata.tm.ds.service.impl;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.PdkSourceDto;
import io.tapdata.pdk.core.constants.DataSourceQCType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.springframework.test.util.ReflectionTestUtils;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.web.multipart.MultipartFile;
import io.tapdata.registration.RegistrationTestConnector;
import io.tapdata.pdk.registration.RegistrationArchive;
import io.tapdata.pdk.registration.RegistrationMetadata;
import com.alibaba.fastjson.JSON;
import java.io.InputStream;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.jar.Attributes;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import org.springframework.data.mongodb.core.query.Query;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.mock;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verifyNoInteractions;

class PdkReleaseRegistrationServiceTest {
    private static final byte[] INVALID_ARCHIVE = {1, 2, 3};

    private PdkReleaseCatalogService catalogService;
    private PkdSourceService pkdSourceService;
    private PdkReleaseRegistrationService service;
    private UserDetail user;
    private SettingsService settingsService;
    private MongoTemplate mongoTemplate;
    private Map<String, Object> release;
    private final List<Path> downloadedFiles = new ArrayList<>();

    @BeforeEach
    void setUp() {
        catalogService = mock(PdkReleaseCatalogService.class);
        pkdSourceService = mock(PkdSourceService.class);
        user = mock(UserDetail.class);
        settingsService = mock(SettingsService.class);
        mongoTemplate = mock(MongoTemplate.class);
        service = new PdkReleaseRegistrationService(new DirectExecutorService());
        service.setReleaseCatalogService(catalogService);
        service.setPkdSourceService(pkdSourceService);
        service.setSettingsService(settingsService);
        service.setMongoTemplate(mongoTemplate);
        release = new HashMap<>();
        release.put("artifactId", "mysql-connector");
        release.put("pdkId", "mysql");
        release.put("version", "1.0.3");
        release.put("pdkAPIVersion", "2.0.0");
        when(catalogService.findRelease("mysql-connector")).thenReturn(release);
        useReleaseJar(INVALID_ARCHIVE);
        // Snapshot while upload owns the file; its backing temporary file must be deleted afterwards.
        doAnswer(invocation -> {
            MultipartFile[] files = invocation.getArgument(0);
            for (int i = 0; i < files.length; i++) {
                MultipartFile file = files[i];
                files[i] = new org.springframework.mock.web.MockMultipartFile(file.getName(),
                        file.getOriginalFilename(), file.getContentType(), file.getInputStream());
            }
            return null;
        }).when(pkdSourceService).uploadReleasedPdk(any(), anyList(), any(), anyBoolean());
        impact(false);
    }

    private void impact(boolean hasInFlightResources) {
        when(pkdSourceService.registrationImpact("mysql", "io.tapdata", user))
                .thenReturn(Map.of("hasInFlightResources", hasInFlightResources));
    }

    @AfterEach
    void tearDown() {
        service.shutdownRegistrationExecutor();
        for (Path path : downloadedFiles) assertFalse(Files.exists(path), "Temporary jar was not deleted");
    }

    private void useReleaseJar(byte[] jar) {
        doAnswer(invocation -> {
            Path target = invocation.getArgument(1);
            downloadedFiles.add(target);
            Files.write(target, jar);
            return null;
        }).when(catalogService).downloadJar(eq(release), any(Path.class), any());
    }

    private static byte[] zip(Map<String, byte[]> entries) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().put(Attributes.Name.IMPLEMENTATION_VERSION, "1.0.3");
        manifest.getMainAttributes().put(Attributes.Name.IMPLEMENTATION_VENDOR, "io.tapdata");
        manifest.getMainAttributes().putValue("Authentication", "GA");
        manifest.getMainAttributes().putValue("PDK-API-Version", "2.0.0");
        try (JarOutputStream zip = new JarOutputStream(output, manifest)) {
            zip.putNextEntry(new ZipEntry("icons/"));
            zip.closeEntry();
            for (Map.Entry<String, byte[]> entry : entries.entrySet()) {
                zip.putNextEntry(new ZipEntry(entry.getKey()));
                zip.write(entry.getValue());
                zip.closeEntry();
            }
        }
        return output.toByteArray();
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static Map<String, byte[]> connectorEntries() throws Exception {
        Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("spec_mysql.json", utf8("{\"properties\":{\"id\":\"mysql\",\"name\":\"MySQL\",\"realName\":\"MySQL\","
                + "\"icon\":\"assets/mysql.png\",\"tags\":[\"Database\"]},"
                + "\"configOptions\":{\"capabilities\":[{\"id\":\"dml_insert_policy\",\"alternatives\":[\"ignore\"]},{\"name\":\"no-id\"}],"
                + "\"supportDDL\":{\"events\":[\"new_field_event\"]}},"
                + "\"messages\":{\"default\":\"en_US\",\"en_US\":{\"doc\":\"docs/mysql_en_US.md\"}}}"));
        // Same id, but not referenced by @TapConnectorClass: must never become a second source.
        entries.put("spec_other.json", utf8("{\"properties\":{\"id\":\"mysql\",\"name\":\"Wrong spec\"}}"));
        entries.put("broken.json", utf8("not json"));
        entries.put("docs/mysql_en_US.md", utf8("# MySQL"));
        entries.put("docs/unreferenced.md", utf8("unrelated"));
        entries.put("assets/mysql.png", new byte[]{9});
        String classPath = RegistrationTestConnector.class.getName().replace('.', '/') + ".class";
        try (InputStream input = RegistrationTestConnector.class.getClassLoader().getResourceAsStream(classPath)) {
            entries.put(classPath, input.readAllBytes());
        }
        return entries;
    }

    private static byte[] connectorJar() throws Exception {
        return zip(connectorEntries());
    }

    @Test
    void downloadAndRegisterUploadsJarDocsIconsAndSpecification() throws Exception {
        byte[] jar = connectorJar();
        useReleaseJar(jar);

        service.downloadAndRegister("mysql-connector", "mysql", false, false, user);

        ArgumentCaptor<MultipartFile[]> files = ArgumentCaptor.forClass(MultipartFile[].class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<PdkSourceDto>> sources = ArgumentCaptor.forClass(List.class);
        verify(pkdSourceService).uploadReleasedPdk(files.capture(), sources.capture(), eq(user), eq(true));

        List<String> names = Arrays.stream(files.getValue()).map(MultipartFile::getOriginalFilename).toList();
        assertEquals(List.of("mysql-connector-1.0.3.jar", "assets/mysql.png", "docs/mysql_en_US.md"), names);
        MultipartFile uploadedJar = files.getValue()[0];
        assertArrayEquals(jar, uploadedJar.getBytes());
        assertEquals("file", uploadedJar.getName());
        assertEquals("application/java-archive", uploadedJar.getContentType());
        assertFalse(uploadedJar.isEmpty());
        assertEquals(jar.length, uploadedJar.getSize());
        assertArrayEquals(jar, uploadedJar.getInputStream().readAllBytes());
        File target = File.createTempFile("connector", ".jar");
        try {
            uploadedJar.transferTo(target);
            assertArrayEquals(jar, Files.readAllBytes(target.toPath()));
        } finally {
            Files.deleteIfExists(target.toPath());
        }

        assertEquals(1, sources.getValue().size());
        PdkSourceDto source = sources.getValue().get(0);
        assertEquals("mysql", source.getId());
        assertEquals("MySQL", source.getName());
        assertEquals("io.tapdata", source.getGroup());
        assertEquals("1.0.3", source.getVersion());
        assertEquals("2.0.0", source.getPdkAPIVersion());
        assertEquals(DataSourceQCType.GA, source.getQcType());
        assertEquals(List.of("Database"), source.getTags());
        assertEquals("en_US", source.getMessages().get("default"));
        assertEquals("source", source.getType());
        assertTrue(source.getCapabilities().stream().anyMatch(cap -> "dml_insert_policy".equals(cap.getId())
                && List.of("ignore").equals(cap.getAlternatives())));
        assertTrue(source.getCapabilities().stream().anyMatch(cap -> "new_field_event".equals(cap.getId())));
        assertTrue(source.getCapabilities().size() > 2); // includes functions registered by connector code
        assertTrue(source.getTapTypeDataTypeMap().contains("varchar"));
        assertTrue(JSON.parseObject(source.getExpression()).size() > 0); // CLI default data types
        verifyNoInteractions(mongoTemplate);

        // Compare with exactly the metadata builder used by manual CLI registration.
        File cliJar = File.createTempFile("cli-registration-", ".jar");
        Files.write(cliJar.toPath(), jar);
        try (RegistrationArchive archive = new RegistrationArchive()) {
            PdkSourceDto manual = JSON.toJavaObject(RegistrationMetadata.build(
                    archive.load(cliJar).iterator().next(), path -> {}), PdkSourceDto.class);
            assertEquals(JSON.toJSONString(manual), JSON.toJSONString(source));
        } finally {
            Files.deleteIfExists(cliJar.toPath());
        }
    }

    @Test
    void downloadAndRegisterFiltersUnrelatedConnectors() throws Exception {
        Map<String, byte[]> entries = connectorEntries();
        entries.put("spec_unrelated.json", utf8("{\"properties\":{\"id\":\"unrelated\","
                + "\"name\":\"Unrelated\",\"icon\":\"assets/unrelated.png\"}}"));
        entries.put("assets/unrelated.png", new byte[]{8});
        String classPath = io.tapdata.registration.UnrelatedRegistrationTestConnector.class
                .getName().replace('.', '/') + ".class";
        try (InputStream input = RegistrationTestConnector.class.getClassLoader().getResourceAsStream(classPath)) {
            entries.put(classPath, input.readAllBytes());
        }
        useReleaseJar(zip(entries));

        service.downloadAndRegister("mysql-connector", "mysql", false, false, user);

        ArgumentCaptor<MultipartFile[]> files = ArgumentCaptor.forClass(MultipartFile[].class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<PdkSourceDto>> sources = ArgumentCaptor.forClass(List.class);
        verify(pkdSourceService).uploadReleasedPdk(files.capture(), sources.capture(), eq(user), eq(true));
        assertEquals(List.of("mysql"), sources.getValue().stream().map(PdkSourceDto::getId).toList());
        assertFalse(Arrays.stream(files.getValue())
                .anyMatch(file -> "assets/unrelated.png".equals(file.getOriginalFilename())));
    }

    @Test
    void downloadAndRegisterFailsWhenLoadedConnectorDoesNotMatch() throws Exception {
        Map<String, byte[]> entries = connectorEntries();
        entries.put("spec_mysql.json", utf8("{\"properties\":{\"id\":\"unrelated\",\"name\":\"Unrelated\"}}"));
        useReleaseJar(zip(entries));

        BizException error = assertThrows(BizException.class,
                () -> service.downloadAndRegister("mysql-connector", "mysql", false, false, user));

        assertEquals("Released connector artifact does not contain its PDK specification", error.getMessage());
        verify(pkdSourceService, never()).uploadReleasedPdk(any(), anyList(), any(), anyBoolean());
    }

    @Test
    void encryptedReleaseUsesTheSameLoaderAsManualRegistration() throws Exception {
        byte[] plain = connectorJar();
        File encrypted = File.createTempFile("encrypted-release-", ".jar");
        try {
            Files.write(encrypted.toPath(), plain);
            io.tapdata.encryptor.JarEncryptor.encryptJar(encrypted.getAbsolutePath());
            useReleaseJar(Files.readAllBytes(encrypted.toPath()));
            service.downloadAndRegister("mysql-connector", "mysql", true, false, user);
            ArgumentCaptor<MultipartFile[]> files = ArgumentCaptor.forClass(MultipartFile[].class);
            verify(pkdSourceService).uploadReleasedPdk(files.capture(), anyList(), eq(user), eq(false));
            assertArrayEquals(plain, files.getValue()[0].getBytes());
        } finally {
            Files.deleteIfExists(encrypted.toPath());
        }
    }

    @Test
    void downloadAndRegisterRejectsMismatchedPdkId() {
        assertThrows(BizException.class,
                () -> service.downloadAndRegister("mysql-connector", "postgres", false, false, user));
        verify(catalogService, never()).downloadJar(any(), any(Path.class), any());
    }

    @Test
    void downloadAndRegisterStopsWhenResourcesAreInFlight() throws Exception {
        impact(true);
        useReleaseJar(connectorJar());

        assertThrows(BizException.class,
                () -> service.downloadAndRegister("mysql-connector", "mysql", false, false, user));
        verify(pkdSourceService).registrationImpact("mysql", "io.tapdata", user);
        verify(pkdSourceService, never()).uploadReleasedPdk(any(), anyList(), any(), anyBoolean());
    }

    @Test
    void downloadAndRegisterIgnoresInFlightResourcesWhenRequested() throws Exception {
        impact(true);
        useReleaseJar(connectorJar());

        service.downloadAndRegister("mysql-connector", "mysql", false, true, user);

        verify(pkdSourceService).uploadReleasedPdk(any(), anyList(), eq(user), eq(true));
    }

    @Test
    void checksResourcesOnceAfterDownload() throws Exception {
        doAnswer(invocation -> {
            impact(true);
            Path target = invocation.getArgument(1);
            downloadedFiles.add(target);
            Files.write(target, connectorJar());
            return null;
        }).when(catalogService).downloadJar(eq(release), any(Path.class), any());

        assertThrows(BizException.class,
                () -> service.downloadAndRegister("mysql-connector", "mysql", false, false, user));

        verify(pkdSourceService).registrationImpact("mysql", "io.tapdata", user);
        verify(pkdSourceService, never()).uploadReleasedPdk(any(), anyList(), any(), anyBoolean());
    }

    private void assertRegistrationDenied(UserDetail login) {
        assertThrows(BizException.class, () -> service.registrationImpact("mysql", login));
        assertThrows(BizException.class,
                () -> service.downloadAndRegister("mysql-connector", "mysql", true, true, login));
        verifyNoInteractions(catalogService, pkdSourceService);
    }

    @Test
    void serviceRejectsCloudBeforeAnyIo() {
        when(settingsService.isCloud()).thenReturn(true);
        assertRegistrationDenied(user);
    }

    @Test
    void serviceRejectsFreeAuthBeforeAnyIo() {
        when(user.isFreeAuth()).thenReturn(true);
        assertRegistrationDenied(user);
    }

    @Test
    void serviceRejectsMissingUserBeforeAnyIo() {
        assertRegistrationDenied(null);
    }

    @Test
    void downloadAndRegisterRejectsNonJarRelease() {
        BizException e = assertThrows(BizException.class,
                () -> service.downloadAndRegister("mysql-connector", "mysql", false, false, user));
        verify(pkdSourceService, never()).uploadReleasedPdk(any(), anyList(), any(), anyBoolean());
    }

    @Test
    void downloadAndRegisterFailsWithoutSpecification() throws Exception {
        useReleaseJar(zip(Map.of("README.md", utf8("readme"))));

        assertThrows(BizException.class,
                () -> service.downloadAndRegister("mysql-connector", "mysql", false, false, user));
        verify(pkdSourceService, never()).uploadReleasedPdk(any(), anyList(), any(), anyBoolean());
    }

    @Test
    void downloadAndRegisterAcceptsLargeArchives() throws Exception {
        Map<String, byte[]> entries = connectorEntries();
        entries.put("dependency/large.bin", new byte[64 * 1024 * 1024 + 1]);
        for (int i = 0; i < 10_001; i++) {
            entries.put("dependency/e" + i, new byte[0]);
        }
        useReleaseJar(zip(entries));

        service.downloadAndRegister("mysql-connector", "mysql", false, false, user);

        verify(pkdSourceService).uploadReleasedPdk(any(), anyList(), eq(user), eq(true));
    }

    @Test
    void downloadAndRegisterRejectsInvalidArchive() throws Exception {
        // Cut inside the local file header so the entry name is truncated and ZipInputStream fails.
        byte[] archive = zip(Map.of("spec.json", utf8("{\"properties\":{\"id\":\"mysql\"}}")));
        useReleaseJar(Arrays.copyOf(archive, 30));

        BizException e = assertThrows(BizException.class,
                () -> service.downloadAndRegister("mysql-connector", "mysql", false, false, user));
        assertTrue(e.getMessage().contains("specification"));
        verify(pkdSourceService, never()).uploadReleasedPdk(any(), anyList(), any(), anyBoolean());
    }

    @Test
    void startRegistrationPersistsAndCompletesJob() throws Exception {
        useReleaseJar(connectorJar());

        Map<String, Object> job = service.startRegistration("mysql-connector", "mysql", false, false, user);

        assertEquals("completed", job.get("status"));
        assertEquals(100, job.get("progress"));
        ArgumentCaptor<org.bson.Document> saved = ArgumentCaptor.forClass(org.bson.Document.class);
        verify(mongoTemplate, atLeastOnce()).save(saved.capture(), eq("pdkRegistrationJobs"));
        verify(catalogService).findRelease("mysql-connector");
        assertTrue(saved.getAllValues().stream().anyMatch(document -> "queued".equals(document.getString("status"))));
        org.bson.Document finalJob = saved.getValue();
        assertEquals("completed", finalJob.getString("status"));
    }

    @Test
    void startRegistrationPersistsFailedJob() {
        Map<String, Object> job = service.startRegistration("mysql-connector", "mysql", false, false, user);

        assertEquals("failed", job.get("status"));
        assertEquals("Released connector artifact does not contain its PDK specification", job.get("error"));
    }

    @Test
    void startRegistrationRejectsSynchronousResourceImpact() {
        impact(true);

        assertThrows(BizException.class,
                () -> service.startRegistration("mysql-connector", "mysql", false, false, user));

        verify(mongoTemplate, never()).save(any(), eq("pdkRegistrationJobs"));
        verify(catalogService, never()).downloadJar(any(), any(Path.class), any());
    }

    @Test
    void startRegistrationReturnsResourceImpactChangedErrorCode() throws Exception {
        impact(false);
        doAnswer(invocation -> {
            impact(true);
            Path target = invocation.getArgument(1);
            downloadedFiles.add(target);
            Files.write(target, connectorJar());
            return null;
        }).when(catalogService).downloadJar(eq(release), any(Path.class), any());

        Map<String, Object> job = service.startRegistration("mysql-connector", "mysql", false, false, user);

        assertEquals("failed", job.get("status"));
        assertEquals("RESOURCE_IMPACT_CHANGED", job.get("errorCode"));
        verify(pkdSourceService, never()).uploadReleasedPdk(any(), anyList(), any(), anyBoolean());
    }

    @Test
    void getRegistrationJobRejectsForeignOwnerAndMissingJob() {
        when(user.getUserId()).thenReturn("current-user");
        when(mongoTemplate.findById("missing", org.bson.Document.class, "pdkRegistrationJobs"))
                .thenReturn(null);
        when(mongoTemplate.findById("foreign", org.bson.Document.class, "pdkRegistrationJobs"))
                .thenReturn(jobDocument("foreign", "other-user", "downloading", 1L));

        assertThrows(BizException.class, () -> service.getRegistrationJob("missing", user));
        assertThrows(BizException.class, () -> service.getRegistrationJob("foreign", user));
    }

    @Test
    void getRegistrationJobMarksStaleJobInterrupted() {
        when(user.getUserId()).thenReturn("current-user");
        when(mongoTemplate.findById("interrupted", org.bson.Document.class, "pdkRegistrationJobs"))
                .thenReturn(jobDocument("interrupted", "current-user", "registering", 1L));

        when(mongoTemplate.findAndModify(any(Query.class), any(org.springframework.data.mongodb.core.query.Update.class),
                any(org.springframework.data.mongodb.core.FindAndModifyOptions.class), eq(org.bson.Document.class),
                eq("pdkRegistrationJobs"))).thenReturn(jobDocument("interrupted", "current-user", "failed", 2L)
                .append("error", "Connector registration was interrupted"));
        Map<String, Object> job = service.getRegistrationJob("interrupted", user);

        assertEquals("failed", job.get("status"));
        assertEquals("Connector registration was interrupted", job.get("error"));
        ArgumentCaptor<Query> query = ArgumentCaptor.forClass(Query.class);
        verify(mongoTemplate).findAndModify(query.capture(), any(org.springframework.data.mongodb.core.query.Update.class),
                any(org.springframework.data.mongodb.core.FindAndModifyOptions.class), eq(org.bson.Document.class),
                eq("pdkRegistrationJobs"));
        assertEquals(1L, query.getValue().getQueryObject().get("updatedAt"));
        assertEquals("registering", query.getValue().getQueryObject().get("status"));
        verify(mongoTemplate, never()).save(any(), eq("pdkRegistrationJobs"));
    }

    @Test
    void getRegistrationJobKeepsRecentJobActive() {
        when(user.getUserId()).thenReturn("current-user");
        when(mongoTemplate.findById("active", org.bson.Document.class, "pdkRegistrationJobs"))
                .thenReturn(jobDocument("active", "current-user", "downloading", System.currentTimeMillis()));

        Map<String, Object> job = service.getRegistrationJob("active", user);

        assertEquals("downloading", job.get("status"));
        verify(mongoTemplate, never()).save(any(), eq("pdkRegistrationJobs"));
    }

    @Test
    void heartbeatRetriesAfterPersistenceFailureAndKeepsLocalJobAlive() {
        service.shutdownRegistrationExecutor();
        var queued = mock(java.util.concurrent.ExecutorService.class);
        service = new PdkReleaseRegistrationService(queued);
        service.setReleaseCatalogService(catalogService);
        service.setPkdSourceService(pkdSourceService);
        service.setSettingsService(settingsService);
        service.setMongoTemplate(mongoTemplate);
        when(user.getUserId()).thenReturn("current-user");
        Map<String, Object> started = service.startRegistration("mysql-connector", "mysql", false, false, user);
        String id = (String) started.get("jobId");
        Map<?, ?> active = (Map<?, ?>) ReflectionTestUtils.getField(service, "activeJobs");
        Object job = active.get(id);
        doThrow(new IllegalStateException("Mongo unavailable")).doReturn(null)
                .when(mongoTemplate).save(any(org.bson.Document.class), eq("pdkRegistrationJobs"));
        assertDoesNotThrow(() -> ReflectionTestUtils.invokeMethod(service, "heartbeatJob", job));
        assertDoesNotThrow(() -> ReflectionTestUtils.invokeMethod(service, "heartbeatJob", job));
        verify(mongoTemplate, times(3)).save(any(org.bson.Document.class), eq("pdkRegistrationJobs"));
        when(mongoTemplate.findById(id, org.bson.Document.class, "pdkRegistrationJobs"))
                .thenReturn(jobDocument(id, "current-user", "queued", 1L));
        assertEquals("queued", service.getRegistrationJob(id, user).get("status"));
        verify(mongoTemplate, never()).findAndModify(any(Query.class), any(), any(), any(Class.class), anyString());
    }

    @Test
    void interruptionDetectionDoesNotOverwriteConcurrentCompletion() {
        when(user.getUserId()).thenReturn("current-user");
        when(mongoTemplate.findById("racing", org.bson.Document.class, "pdkRegistrationJobs"))
                .thenReturn(jobDocument("racing", "current-user", "registering", 1L),
                        jobDocument("racing", "current-user", "completed", System.currentTimeMillis()));
        // CAS returns null because a completion changed status/updatedAt in the meantime.
        assertEquals("completed", service.getRegistrationJob("racing", user).get("status"));
        verify(mongoTemplate, never()).save(any(), eq("pdkRegistrationJobs"));
    }

    @Test
    void registrationImpactUsesSharedReleaseValidation() {
        assertThrows(BizException.class, () -> service.registrationImpact("mysql-connector", "postgres", user));
        verifyNoInteractions(pkdSourceService);
    }

    @Test
    void failedUploadStillDeletesTemporaryJar() throws Exception {
        useReleaseJar(connectorJar());
        doThrow(new IllegalStateException("upload failed")).when(pkdSourceService)
                .uploadReleasedPdk(any(), anyList(), any(), anyBoolean());
        assertThrows(IllegalStateException.class,
                () -> service.downloadAndRegister("mysql-connector", "mysql", true, false, user));
    }

    private static org.bson.Document jobDocument(String id, String owner, String status, long updatedAt) {
        return new org.bson.Document("_id", id).append("ownerId", owner).append("pdkId", "mysql")
                .append("status", status).append("progress", 42).append("updatedAt", updatedAt);
    }

    private static final class DirectExecutorService extends AbstractExecutorService {
        private boolean shutdown;

        @Override public void shutdown() { shutdown = true; }
        @Override public List<Runnable> shutdownNow() { shutdown = true; return List.of(); }
        @Override public boolean isShutdown() { return shutdown; }
        @Override public boolean isTerminated() { return shutdown; }
        @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return shutdown; }
        @Override public void execute(Runnable command) { command.run(); }
    }
}
