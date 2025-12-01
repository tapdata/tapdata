package com.tapdata.tm.openapi.generator.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for MavenPackagingUtil
 *
 * @author samuel
 * @Description
 * @create 2025-11-26 17:57
 **/
@ExtendWith(MockitoExtension.class)
class MavenPackagingUtilTest {

    @TempDir
    Path tempDir;

    private Path sourceDir;
    private Path pomFile;

    @BeforeEach
    void setUp() throws IOException {
        sourceDir = tempDir.resolve("source");
        Files.createDirectories(sourceDir);
        pomFile = sourceDir.resolve("pom.xml");
        Files.writeString(pomFile, "<project></project>");
    }

    @Test
    void testPackageToJar_SourceDirNotExists() {
        // Given
        Path nonExistentDir = tempDir.resolve("non-existent");

        // When
        MavenPackagingUtil.PackagingResult result = MavenPackagingUtil.packageToJar(nonExistentDir);

        // Then
        assertFalse(result.isSuccess());
        assertNotNull(result.getErrorMessage());
        assertTrue(result.getErrorMessage().contains("does not exist") ||
                   result.getErrorMessage().contains("not found"));
    }

    @Test
    void testPackageToJar_SourceDirIsFile() throws IOException {
        // Given
        Path fileAsDir = tempDir.resolve("file.txt");
        Files.writeString(fileAsDir, "content");

        // When
        MavenPackagingUtil.PackagingResult result = MavenPackagingUtil.packageToJar(fileAsDir);

        // Then
        assertFalse(result.isSuccess());
        assertNotNull(result.getErrorMessage());
        assertTrue(result.getErrorMessage().contains("not a directory") ||
                   result.getErrorMessage().contains("does not exist"));
    }

    @Test
    void testPackageToJar_NoPomXml() throws IOException {
        // Given
        Path dirWithoutPom = tempDir.resolve("no-pom");
        Files.createDirectories(dirWithoutPom);

        // When
        MavenPackagingUtil.PackagingResult result = MavenPackagingUtil.packageToJar(dirWithoutPom);

        // Then
        assertFalse(result.isSuccess());
        assertNotNull(result.getErrorMessage());
        assertTrue(result.getErrorMessage().contains("pom.xml not found"));
    }

    @Test
    void testPackageToJar_MavenExecutionSuccess() throws Exception {
        // Given
        Path targetDir = sourceDir.resolve("target");
        Files.createDirectories(targetDir);
        Path jarFile = targetDir.resolve("test-sdk-1.0.0.jar");
        Files.writeString(jarFile, "fake jar content");

        Process mockProcess = mock(Process.class);
        InputStream mockInputStream = new ByteArrayInputStream("BUILD SUCCESS".getBytes());
        when(mockProcess.getInputStream()).thenReturn(mockInputStream);
        when(mockProcess.waitFor()).thenReturn(0);

        try (MockedConstruction<ProcessBuilder> mockedConstruction = mockConstruction(ProcessBuilder.class,
                (mock, context) -> {
                    when(mock.directory(any())).thenReturn(mock);
                    when(mock.redirectErrorStream(anyBoolean())).thenReturn(mock);
                    when(mock.start()).thenReturn(mockProcess);
                })) {

            // When
            MavenPackagingUtil.PackagingResult result = MavenPackagingUtil.packageToJar(sourceDir);

            // Then
            assertTrue(result.isSuccess());
            assertNotNull(result.getJarPath());
            assertEquals(jarFile, result.getJarPath());
            assertNull(result.getErrorMessage());
        }
    }

    @Test
    void testPackageToJar_MavenExecutionFailure() throws Exception {
        // Given
        Process mockProcess = mock(Process.class);
        InputStream mockInputStream = new ByteArrayInputStream("BUILD FAILURE".getBytes());
        when(mockProcess.getInputStream()).thenReturn(mockInputStream);
        when(mockProcess.waitFor()).thenReturn(1);

        try (MockedConstruction<ProcessBuilder> mockedConstruction = mockConstruction(ProcessBuilder.class,
                (mock, context) -> {
                    when(mock.directory(any())).thenReturn(mock);
                    when(mock.redirectErrorStream(anyBoolean())).thenReturn(mock);
                    when(mock.start()).thenReturn(mockProcess);
                })) {

            // When
            MavenPackagingUtil.PackagingResult result = MavenPackagingUtil.packageToJar(sourceDir);

            // Then
            assertFalse(result.isSuccess());
            assertNotNull(result.getErrorMessage());
            assertTrue(result.getErrorMessage().contains("failed"));
        }
    }

    @Test
    void testPackageToJar_ProcessInterrupted() throws Exception {
        // Given
        Process mockProcess = mock(Process.class);
        InputStream mockInputStream = new ByteArrayInputStream("".getBytes());
        when(mockProcess.getInputStream()).thenReturn(mockInputStream);
        when(mockProcess.waitFor()).thenThrow(new InterruptedException("Process interrupted"));

        try (MockedConstruction<ProcessBuilder> mockedConstruction = mockConstruction(ProcessBuilder.class,
                (mock, context) -> {
                    when(mock.directory(any())).thenReturn(mock);
                    when(mock.redirectErrorStream(anyBoolean())).thenReturn(mock);
                    when(mock.start()).thenReturn(mockProcess);
                })) {

            // When
            MavenPackagingUtil.PackagingResult result = MavenPackagingUtil.packageToJar(sourceDir);

            // Then
            assertFalse(result.isSuccess());
            assertNotNull(result.getErrorMessage());
            assertTrue(result.getErrorMessage().contains("interrupted"));
            assertTrue(Thread.currentThread().isInterrupted());
            // Clear the interrupted status
            Thread.interrupted();
        }
    }

    @Test
    void testPackageToJar_ProcessStartException() throws Exception {
        // Given
        try (MockedConstruction<ProcessBuilder> mockedConstruction = mockConstruction(ProcessBuilder.class,
                (mock, context) -> {
                    when(mock.directory(any())).thenReturn(mock);
                    when(mock.redirectErrorStream(anyBoolean())).thenReturn(mock);
                    when(mock.start()).thenThrow(new IOException("Cannot start process"));
                })) {

            // When
            MavenPackagingUtil.PackagingResult result = MavenPackagingUtil.packageToJar(sourceDir);

            // Then
            assertFalse(result.isSuccess());
            assertNotNull(result.getErrorMessage());
            assertTrue(result.getErrorMessage().contains("Exception"));
        }
    }

    @Test
    void testPackageToJar_JarNotFoundAfterSuccess() throws Exception {
        // Given - no jar file in target directory
        Path targetDir = sourceDir.resolve("target");
        Files.createDirectories(targetDir);

        Process mockProcess = mock(Process.class);
        InputStream mockInputStream = new ByteArrayInputStream("BUILD SUCCESS".getBytes());
        when(mockProcess.getInputStream()).thenReturn(mockInputStream);
        when(mockProcess.waitFor()).thenReturn(0);

        try (MockedConstruction<ProcessBuilder> mockedConstruction = mockConstruction(ProcessBuilder.class,
                (mock, context) -> {
                    when(mock.directory(any())).thenReturn(mock);
                    when(mock.redirectErrorStream(anyBoolean())).thenReturn(mock);
                    when(mock.start()).thenReturn(mockProcess);
                })) {

            // When
            MavenPackagingUtil.PackagingResult result = MavenPackagingUtil.packageToJar(sourceDir);

            // Then
            assertFalse(result.isSuccess());
            assertNotNull(result.getErrorMessage());
            assertTrue(result.getErrorMessage().contains("JAR file not found"));
        }
    }

    @Test
    void testPackageToJar_FilterOutOriginalJar() throws Exception {
        // Given
        Path targetDir = sourceDir.resolve("target");
        Files.createDirectories(targetDir);
        Path originalJar = targetDir.resolve("original-test-sdk-1.0.0.jar");
        Path actualJar = targetDir.resolve("test-sdk-1.0.0.jar");
        Files.writeString(originalJar, "original jar");
        Files.writeString(actualJar, "actual jar");

        Process mockProcess = mock(Process.class);
        InputStream mockInputStream = new ByteArrayInputStream("BUILD SUCCESS".getBytes());
        when(mockProcess.getInputStream()).thenReturn(mockInputStream);
        when(mockProcess.waitFor()).thenReturn(0);

        try (MockedConstruction<ProcessBuilder> mockedConstruction = mockConstruction(ProcessBuilder.class,
                (mock, context) -> {
                    when(mock.directory(any())).thenReturn(mock);
                    when(mock.redirectErrorStream(anyBoolean())).thenReturn(mock);
                    when(mock.start()).thenReturn(mockProcess);
                })) {

            // When
            MavenPackagingUtil.PackagingResult result = MavenPackagingUtil.packageToJar(sourceDir);

            // Then
            assertTrue(result.isSuccess());
            assertNotNull(result.getJarPath());
            assertFalse(result.getJarPath().toString().contains("original-"));
        }
    }

    @Test
    void testPackageToJar_FilterOutSourcesJar() throws Exception {
        // Given
        Path targetDir = sourceDir.resolve("target");
        Files.createDirectories(targetDir);
        Path sourcesJar = targetDir.resolve("test-sdk-1.0.0-sources.jar");
        Path actualJar = targetDir.resolve("test-sdk-1.0.0.jar");
        Files.writeString(sourcesJar, "sources jar");
        Files.writeString(actualJar, "actual jar");

        Process mockProcess = mock(Process.class);
        InputStream mockInputStream = new ByteArrayInputStream("BUILD SUCCESS".getBytes());
        when(mockProcess.getInputStream()).thenReturn(mockInputStream);
        when(mockProcess.waitFor()).thenReturn(0);

        try (MockedConstruction<ProcessBuilder> mockedConstruction = mockConstruction(ProcessBuilder.class,
                (mock, context) -> {
                    when(mock.directory(any())).thenReturn(mock);
                    when(mock.redirectErrorStream(anyBoolean())).thenReturn(mock);
                    when(mock.start()).thenReturn(mockProcess);
                })) {

            // When
            MavenPackagingUtil.PackagingResult result = MavenPackagingUtil.packageToJar(sourceDir);

            // Then
            assertTrue(result.isSuccess());
            assertNotNull(result.getJarPath());
            assertFalse(result.getJarPath().toString().contains("sources"));
        }
    }

    @Test
    void testPackageToJar_FilterOutJavadocJar() throws Exception {
        // Given
        Path targetDir = sourceDir.resolve("target");
        Files.createDirectories(targetDir);
        Path javadocJar = targetDir.resolve("test-sdk-1.0.0-javadoc.jar");
        Path actualJar = targetDir.resolve("test-sdk-1.0.0.jar");
        Files.writeString(javadocJar, "javadoc jar");
        Files.writeString(actualJar, "actual jar");

        Process mockProcess = mock(Process.class);
        InputStream mockInputStream = new ByteArrayInputStream("BUILD SUCCESS".getBytes());
        when(mockProcess.getInputStream()).thenReturn(mockInputStream);
        when(mockProcess.waitFor()).thenReturn(0);

        try (MockedConstruction<ProcessBuilder> mockedConstruction = mockConstruction(ProcessBuilder.class,
                (mock, context) -> {
                    when(mock.directory(any())).thenReturn(mock);
                    when(mock.redirectErrorStream(anyBoolean())).thenReturn(mock);
                    when(mock.start()).thenReturn(mockProcess);
                })) {

            // When
            MavenPackagingUtil.PackagingResult result = MavenPackagingUtil.packageToJar(sourceDir);

            // Then
            assertTrue(result.isSuccess());
            assertNotNull(result.getJarPath());
            assertFalse(result.getJarPath().toString().contains("javadoc"));
        }
    }

    @Test
    void testPackagingResult_Success() {
        // Given
        Path jarPath = tempDir.resolve("test.jar");
        String output = "BUILD SUCCESS";

        // When
        MavenPackagingUtil.PackagingResult result = MavenPackagingUtil.PackagingResult.success(jarPath, output);

        // Then
        assertTrue(result.isSuccess());
        assertEquals(jarPath, result.getJarPath());
        assertEquals(output, result.getOutput());
        assertNull(result.getErrorMessage());
    }

    @Test
    void testPackagingResult_Failure() {
        // Given
        String errorMessage = "Build failed";

        // When
        MavenPackagingUtil.PackagingResult result = MavenPackagingUtil.PackagingResult.failure(errorMessage);

        // Then
        assertFalse(result.isSuccess());
        assertNull(result.getJarPath());
        assertNull(result.getOutput());
        assertEquals(errorMessage, result.getErrorMessage());
    }

    @Test
    void testPackageToJar_TargetDirNotExists() throws Exception {
        // Given - no target directory created
        Process mockProcess = mock(Process.class);
        InputStream mockInputStream = new ByteArrayInputStream("BUILD SUCCESS".getBytes());
        when(mockProcess.getInputStream()).thenReturn(mockInputStream);
        when(mockProcess.waitFor()).thenReturn(0);

        try (MockedConstruction<ProcessBuilder> mockedConstruction = mockConstruction(ProcessBuilder.class,
                (mock, context) -> {
                    when(mock.directory(any())).thenReturn(mock);
                    when(mock.redirectErrorStream(anyBoolean())).thenReturn(mock);
                    when(mock.start()).thenReturn(mockProcess);
                })) {

            // When
            MavenPackagingUtil.PackagingResult result = MavenPackagingUtil.packageToJar(sourceDir);

            // Then
            assertFalse(result.isSuccess());
            assertNotNull(result.getErrorMessage());
            assertTrue(result.getErrorMessage().contains("JAR file not found"));
        }
    }

    @Test
    void testPackageToJar_OnlyOriginalJarExists() throws Exception {
        // Given
        Path targetDir = sourceDir.resolve("target");
        Files.createDirectories(targetDir);
        Path originalJar = targetDir.resolve("original-test-sdk-1.0.0.jar");
        Files.writeString(originalJar, "original jar");

        Process mockProcess = mock(Process.class);
        InputStream mockInputStream = new ByteArrayInputStream("BUILD SUCCESS".getBytes());
        when(mockProcess.getInputStream()).thenReturn(mockInputStream);
        when(mockProcess.waitFor()).thenReturn(0);

        try (MockedConstruction<ProcessBuilder> mockedConstruction = mockConstruction(ProcessBuilder.class,
                (mock, context) -> {
                    when(mock.directory(any())).thenReturn(mock);
                    when(mock.redirectErrorStream(anyBoolean())).thenReturn(mock);
                    when(mock.start()).thenReturn(mockProcess);
                })) {

            // When
            MavenPackagingUtil.PackagingResult result = MavenPackagingUtil.packageToJar(sourceDir);

            // Then
            assertFalse(result.isSuccess());
            assertNotNull(result.getErrorMessage());
            assertTrue(result.getErrorMessage().contains("JAR file not found"));
        }
    }
}