package com.tapdata.tm.ds.service.impl;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.dto.PdkSourceDto;
import io.tapdata.encryptor.JarEncryptor;
import io.tapdata.pdk.core.constants.DataSourceQCType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.test.util.ReflectionTestUtils;
import java.io.ByteArrayInputStream;

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
import java.util.zip.ZipOutputStream;

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
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verifyNoInteractions;

class PdkReleaseRegistrationServiceTest {
    private static final byte[] ENCRYPTED = {1, 2, 3};

    private PdkReleaseCatalogService catalogService;
    private PkdSourceService pkdSourceService;
    private PdkReleaseRegistrationService service;
    private UserDetail user;
    private SettingsService settingsService;
    private MockedStatic<JarEncryptor> jarEncryptor;
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
        jarEncryptor = mockStatic(JarEncryptor.class);
        release = new HashMap<>();
        release.put("artifactId", "mysql-connector");
        release.put("pdkId", "mysql");
        release.put("version", "1.0.3");
        release.put("pdkAPIVersion", "2.0.0");
        when(catalogService.findRelease("mysql-connector")).thenReturn(release);
        when(catalogService.downloadJar(release)).thenReturn(ENCRYPTED);
        impact(false);
    }

    @AfterEach
    void tearDown() {
        jarEncryptor.close();
    }

    private void impact(boolean hasInFlightResources) {
        when(pkdSourceService.registrationImpact("mysql", "io.tapdata", user))
                .thenReturn(Map.of("hasInFlightResources", hasInFlightResources));
    }

    private void decryptsTo(byte[] jar) {
        jarEncryptor.when(() -> JarEncryptor.decryptJarBytes(ENCRYPTED)).thenReturn(jar);
    }

    private static byte[] zip(Map<String, byte[]> entries) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(output)) {
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

    private static byte[] connectorJar() throws Exception {
        Map<String, byte[]> entries = new LinkedHashMap<>();
        entries.put("spec_mysql.json", utf8("{\"properties\":{\"id\":\"mysql\",\"name\":\"MySQL\",\"realName\":\"MySQL\","
                + "\"icon\":\"icons/mysql.png\",\"tags\":[\"Database\"]},"
                + "\"manifest\":{\"Authentication\":\"GA\"},"
                + "\"configOptions\":{\"capabilities\":[{\"id\":\"dml_insert_policy\"},{\"name\":\"no-id\"}]},"
                + "\"messages\":{\"default\":\"en_US\"},\"dataTypes\":{\"int\":{}}}"));
        entries.put("spec_other.json", utf8("{\"properties\":{\"id\":\"other\"}}"));
        entries.put("broken.json", utf8("not json"));
        entries.put("docs/mysql_en_US.md", utf8("# MySQL"));
        entries.put("icons/mysql.png", new byte[]{9});
        entries.put("io/tapdata/Connector.class", new byte[]{0});
        return zip(entries);
    }

    @Test
    void downloadAndRegisterUploadsJarDocsIconsAndSpecification() throws Exception {
        byte[] jar = connectorJar();
        decryptsTo(jar);

        service.downloadAndRegister("mysql-connector", "mysql", false, false, user);

        ArgumentCaptor<MultipartFile[]> files = ArgumentCaptor.forClass(MultipartFile[].class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<PdkSourceDto>> sources = ArgumentCaptor.forClass(List.class);
        verify(pkdSourceService).uploadPdk(files.capture(), sources.capture(), eq(true), eq(user), eq(true));

        List<String> names = Arrays.stream(files.getValue()).map(MultipartFile::getOriginalFilename).toList();
        assertEquals(List.of("mysql-connector-1.0.3.jar", "docs/mysql_en_US.md", "icons/mysql.png"), names);
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
        assertTrue(source.getConfigOptions().containsKey("capabilities"));
        assertEquals(1, source.getCapabilities().size());
        assertEquals("dml_insert_policy", source.getCapabilities().get(0).getId());
    }

    @Test
    void downloadAndRegisterDefaultsOptionalSpecificationFields() throws Exception {
        decryptsTo(zip(Map.of("spec.json", utf8("{\"properties\":{\"id\":\"mysql\"}}"))));

        service.downloadAndRegister("mysql-connector", "mysql", true, false, user);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<PdkSourceDto>> sources = ArgumentCaptor.forClass(List.class);
        verify(pkdSourceService).uploadPdk(any(), sources.capture(), eq(true), eq(user), eq(false));
        PdkSourceDto source = sources.getValue().get(0);
        assertEquals(DataSourceQCType.Alpha, source.getQcType());
        assertTrue(source.getConfigOptions().isEmpty());
        assertTrue(source.getCapabilities().isEmpty());
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
        decryptsTo(connectorJar());

        assertThrows(BizException.class,
                () -> service.downloadAndRegister("mysql-connector", "mysql", false, false, user));
        verify(pkdSourceService).registrationImpact("mysql", "io.tapdata", user);
        verify(pkdSourceService, never()).uploadPdk(any(), anyList(), anyBoolean(), any(), anyBoolean());
    }

    @Test
    void downloadAndRegisterIgnoresInFlightResourcesWhenRequested() throws Exception {
        impact(true);
        decryptsTo(connectorJar());

        service.downloadAndRegister("mysql-connector", "mysql", false, true, user);

        verify(pkdSourceService).uploadPdk(any(), anyList(), eq(true), eq(user), eq(true));
    }

    @Test
    void checksResourcesOnceAfterDownload() throws Exception {
        decryptsTo(connectorJar());
        when(catalogService.downloadJar(release)).thenAnswer(invocation -> {
            impact(true);
            return ENCRYPTED;
        });

        assertThrows(BizException.class,
                () -> service.downloadAndRegister("mysql-connector", "mysql", false, false, user));

        verify(pkdSourceService).registrationImpact("mysql", "io.tapdata", user);
        verify(pkdSourceService, never()).uploadPdk(any(), anyList(), anyBoolean(), any(), anyBoolean());
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
    void readEntryEnforcesRemainingTotalBudget() {
        assertArrayEquals(new byte[3], ReflectionTestUtils.invokeMethod(service, "readEntry",
                new ByteArrayInputStream(new byte[3]), 3L));
        BizException error = assertThrows(BizException.class, () -> ReflectionTestUtils.invokeMethod(service,
                "readEntry", new ByteArrayInputStream(new byte[4]), 3L));
        assertEquals("Released connector artifact is too large", error.getMessage());
    }

    @Test
    void downloadAndRegisterFailsWhenDecryptionFails() {
        jarEncryptor.when(() -> JarEncryptor.decryptJarBytes(ENCRYPTED)).thenThrow(new IllegalStateException("bad"));

        assertThrows(BizException.class,
                () -> service.downloadAndRegister("mysql-connector", "mysql", false, false, user));
    }

    @Test
    void downloadAndRegisterFailsWithoutSpecification() throws Exception {
        decryptsTo(zip(Map.of("README.md", utf8("readme"))));

        assertThrows(BizException.class,
                () -> service.downloadAndRegister("mysql-connector", "mysql", false, false, user));
        verify(pkdSourceService, never()).uploadPdk(any(), anyList(), anyBoolean(), any(), anyBoolean());
    }

    @Test
    void downloadAndRegisterRejectsOversizedEntry() throws Exception {
        byte[] large = new byte[(int) PdkReleaseRegistrationService.MAX_ENTRY_BYTES + 1];
        decryptsTo(zip(Map.of("large.bin", large)));

        BizException e = assertThrows(BizException.class,
                () -> service.downloadAndRegister("mysql-connector", "mysql", false, false, user));
        assertEquals("Released connector artifact entry is too large", e.getMessage());
    }

    @Test
    void downloadAndRegisterRejectsTooManyEntries() throws Exception {
        Map<String, byte[]> entries = new LinkedHashMap<>();
        for (int i = 0; i < PdkReleaseRegistrationService.MAX_ENTRIES; i++) {
            entries.put("e" + i, new byte[0]);
        }
        decryptsTo(zip(entries));

        BizException e = assertThrows(BizException.class,
                () -> service.downloadAndRegister("mysql-connector", "mysql", false, false, user));
        assertEquals("Released connector artifact has too many entries", e.getMessage());
    }

    @Test
    void downloadAndRegisterRejectsInvalidArchive() {
        decryptsTo(new byte[]{'P', 'K', 3, 4, 0, 0});

        assertThrows(BizException.class,
                () -> service.downloadAndRegister("mysql-connector", "mysql", false, false, user));
    }
}
