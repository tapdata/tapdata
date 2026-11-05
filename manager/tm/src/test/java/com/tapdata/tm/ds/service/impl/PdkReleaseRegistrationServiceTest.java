package com.tapdata.tm.ds.service.impl;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.PdkSourceDto;
import io.tapdata.pdk.core.constants.DataSourceQCType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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
    private Map<String, Object> release;

    @BeforeEach
    void setUp() {
        catalogService = mock(PdkReleaseCatalogService.class);
        pkdSourceService = mock(PkdSourceService.class);
        user = mock(UserDetail.class);
        settingsService = mock(SettingsService.class);
        service = new PdkReleaseRegistrationService();
        service.setReleaseCatalogService(catalogService);
        service.setPkdSourceService(pkdSourceService);
        service.setSettingsService(settingsService);
        release = new HashMap<>();
        release.put("artifactId", "mysql-connector");
        release.put("pdkId", "mysql");
        release.put("version", "1.0.3");
        release.put("pdkAPIVersion", "2.0.0");
        when(catalogService.findRelease("mysql-connector")).thenReturn(release);
        when(catalogService.downloadJar(release)).thenReturn(INVALID_ARCHIVE);
        impact(false);
    }

    private void impact(boolean hasInFlightResources) {
        when(pkdSourceService.registrationImpact("mysql", "io.tapdata", user))
                .thenReturn(Map.of("hasInFlightResources", hasInFlightResources));
    }

    private void useReleaseJar(byte[] jar) {
        when(catalogService.downloadJar(release)).thenReturn(jar);
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
        verify(catalogService, never()).downloadJar(any());
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
        when(catalogService.downloadJar(release)).thenAnswer(invocation -> {
            impact(true);
            return connectorJar();
        });

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
}
