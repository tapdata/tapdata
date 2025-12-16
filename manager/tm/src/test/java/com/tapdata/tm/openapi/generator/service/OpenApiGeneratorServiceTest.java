package com.tapdata.tm.openapi.generator.service;

import com.tapdata.tm.application.dto.ApplicationDto;
import com.tapdata.tm.application.service.ApplicationService;
import com.tapdata.tm.file.service.FileService;
import com.tapdata.tm.openapi.generator.config.OpenApiGeneratorProperties;
import com.tapdata.tm.openapi.generator.dto.CodeGenerationRequest;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for OpenApiGeneratorService
 *
 * @author samuel
 * @Description
 * @create 2025-11-26 18:12
 **/
@ExtendWith(MockitoExtension.class)
class OpenApiGeneratorServiceTest {

    @Mock
    private ApplicationService applicationService;

    @Mock
    private FileService fileService;

    @TempDir
    Path tempDir;

    private OpenApiGeneratorService service;
    private OpenApiGeneratorProperties properties;

    @BeforeEach
    void setUp() {
        properties = new OpenApiGeneratorProperties();
        properties.getTemp().setDir(tempDir.toString());
        properties.getJava().setVersion(11);

        service = new OpenApiGeneratorService(properties, applicationService, fileService);
    }

    @Test
    void testConstructor() {
        // When
        OpenApiGeneratorService newService = new OpenApiGeneratorService(properties, applicationService, fileService);

        // Then
        assertNotNull(newService);
    }

    @Test
    void testInit_Success() throws IOException {
        // Given - create necessary directories and files for initialization
        Path jarPath = tempDir.resolve("openapi-generator").resolve("openapi-generator-cli.jar");
        Files.createDirectories(jarPath.getParent());
        Files.createFile(jarPath);

        Path templatePath = tempDir.resolve("openapi-generator").resolve("templates");
        Files.createDirectories(templatePath);

        // Set properties to use the temp directory
        properties.getJar().setPath(jarPath.toString());
        properties.getTemplate().setPath(templatePath.toString());

        OpenApiGeneratorService testService = new OpenApiGeneratorService(properties, applicationService, fileService);

        // When
        testService.init();

        // Then - verify initialization completed without exceptions
        Object resolvedJarPath = ReflectionTestUtils.getField(testService, "resolvedJarPath");
        Object resolvedTemplatePath = ReflectionTestUtils.getField(testService, "resolvedTemplatePath");
        Object resolvedTempDir = ReflectionTestUtils.getField(testService, "resolvedTempDir");
        Object restTemplate = ReflectionTestUtils.getField(testService, "restTemplate");
        Object openApiJsonProcessor = ReflectionTestUtils.getField(testService, "openApiJsonProcessor");

        assertNotNull(resolvedJarPath);
        assertNotNull(resolvedTemplatePath);
        assertNotNull(resolvedTempDir);
        assertNotNull(restTemplate);
        assertNotNull(openApiJsonProcessor);
    }

    @Test
    void testInit_Failure_JarNotFound() {
        // Given - no JAR file exists
        properties.getJar().setPath(tempDir.resolve("non-existent.jar").toString());

        OpenApiGeneratorService testService = new OpenApiGeneratorService(properties, applicationService, fileService);

        // When
        testService.init();

        // Then - verify fields are null after failed initialization
        Object resolvedJarPath = ReflectionTestUtils.getField(testService, "resolvedJarPath");
        Object resolvedTemplatePath = ReflectionTestUtils.getField(testService, "resolvedTemplatePath");
        Object resolvedTempDir = ReflectionTestUtils.getField(testService, "resolvedTempDir");

        assertNull(resolvedJarPath);
        assertNull(resolvedTemplatePath);
        assertNull(resolvedTempDir);
    }

    @Test
    void testGenerateCodeEnhanced_UnsupportedLanguage() {
        // Given
        CodeGenerationRequest request = createBasicRequest();
        request.setLan("python"); // Unsupported language

        // When
        OpenApiGeneratorService.EnhancedGenerationResult result = service.generateCodeEnhanced(request);

        // Then
        assertFalse(result.isSuccess());
        assertNotNull(result.getErrorMessage());
        assertTrue(result.getErrorMessage().contains("Unsupported language"));
    }

    @Test
    void testGenerateCodeEnhanced_JavaVersionValidationFailure() {
        // Given
        CodeGenerationRequest request = createBasicRequest();

        // Mock Java version to be less than 11
        String oldJavaVersion = System.getProperty("java.version");
        try {
            System.setProperty("java.version", "1.8.0_291");

            // When
            OpenApiGeneratorService.EnhancedGenerationResult result = service.generateCodeEnhanced(request);

            // Then
            assertFalse(result.isSuccess());
            assertNotNull(result.getErrorMessage());
        } finally {
            // Restore original Java version
            System.setProperty("java.version", oldJavaVersion);
        }
    }

    @Test
    void testEnhancedGenerationResult_Success() {
        // Given
        String zipId = "zip123";
        Long zipSize = 1024L;
        String jarId = "jar456";
        Long jarSize = 2048L;
        String jarError = null;

        // When
        OpenApiGeneratorService.EnhancedGenerationResult result =
            OpenApiGeneratorService.EnhancedGenerationResult.success(zipId, zipSize, jarId, jarSize, jarError);

        // Then
        assertTrue(result.isSuccess());
        assertEquals(zipId, result.getZipGridfsId());
        assertEquals(zipSize, result.getZipSize());
        assertEquals(jarId, result.getJarGridfsId());
        assertEquals(jarSize, result.getJarSize());
        assertNull(result.getJarError());
        assertNull(result.getErrorMessage());
    }

    @Test
    void testEnhancedGenerationResult_SuccessWithJarError() {
        // Given
        String zipId = "zip123";
        Long zipSize = 1024L;
        String jarError = "JAR creation failed";

        // When
        OpenApiGeneratorService.EnhancedGenerationResult result =
            OpenApiGeneratorService.EnhancedGenerationResult.success(zipId, zipSize, null, null, jarError);

        // Then
        assertTrue(result.isSuccess());
        assertEquals(zipId, result.getZipGridfsId());
        assertEquals(zipSize, result.getZipSize());
        assertNull(result.getJarGridfsId());
        assertNull(result.getJarSize());
        assertEquals(jarError, result.getJarError());
        assertNull(result.getErrorMessage());
    }

    @Test
    void testEnhancedGenerationResult_Failure() {
        // Given
        String errorMessage = "Generation failed";

        // When
        OpenApiGeneratorService.EnhancedGenerationResult result =
            OpenApiGeneratorService.EnhancedGenerationResult.failure(errorMessage);

        // Then
        assertFalse(result.isSuccess());
        assertNull(result.getZipGridfsId());
        assertNull(result.getZipSize());
        assertNull(result.getJarGridfsId());
        assertNull(result.getJarSize());
        assertNull(result.getJarError());
        assertEquals(errorMessage, result.getErrorMessage());
    }

    @Test
    void testParseJavaMajorVersion_OldFormat() throws Exception {
        // Given
        String javaVersion = "1.8.0_291";

        // When
        int majorVersion = (int) ReflectionTestUtils.invokeMethod(service, "parseJavaMajorVersion", javaVersion);

        // Then
        assertEquals(8, majorVersion);
    }

    @Test
    void testParseJavaMajorVersion_NewFormat() throws Exception {
        // Given
        String javaVersion = "11.0.12";

        // When
        int majorVersion = (int) ReflectionTestUtils.invokeMethod(service, "parseJavaMajorVersion", javaVersion);

        // Then
        assertEquals(11, majorVersion);
    }

    @Test
    void testParseJavaMajorVersion_Java17() throws Exception {
        // Given
        String javaVersion = "17.0.1";

        // When
        int majorVersion = (int) ReflectionTestUtils.invokeMethod(service, "parseJavaMajorVersion", javaVersion);

        // Then
        assertEquals(17, majorVersion);
    }

    @Test
    void testParseJavaMajorVersion_NullInput() {
        // When & Then
        assertThrows(NumberFormatException.class, () -> {
            ReflectionTestUtils.invokeMethod(service, "parseJavaMajorVersion", (String) null);
        });
    }

    @Test
    void testParseJavaMajorVersion_EmptyInput() {
        // When & Then
        assertThrows(NumberFormatException.class, () -> {
            ReflectionTestUtils.invokeMethod(service, "parseJavaMajorVersion", "");
        });
    }

    @Test
    void testParseJavaMajorVersion_InvalidFormat() {
        // When & Then
        assertThrows(NumberFormatException.class, () -> {
            ReflectionTestUtils.invokeMethod(service, "parseJavaMajorVersion", "invalid");
        });
    }

    @Test
    void testExtractVersionFromJavaVersionOutput_Java8() throws Exception {
        // Given
        String output = "java version \"1.8.0_291\"\nJava(TM) SE Runtime Environment";

        // When
        String version = (String) ReflectionTestUtils.invokeMethod(service, "extractVersionFromJavaVersionOutput", output);

        // Then
        assertEquals("1.8.0_291", version);
    }

    @Test
    void testExtractVersionFromJavaVersionOutput_Java11() throws Exception {
        // Given
        String output = "openjdk version \"11.0.12\"\nOpenJDK Runtime Environment";

        // When
        String version = (String) ReflectionTestUtils.invokeMethod(service, "extractVersionFromJavaVersionOutput", output);

        // Then
        assertEquals("11.0.12", version);
    }

    @Test
    void testExtractVersionFromJavaVersionOutput_NoVersion() {
        // Given
        String output = "No version information";

        // When & Then
        assertThrows(NumberFormatException.class, () -> {
            ReflectionTestUtils.invokeMethod(service, "extractVersionFromJavaVersionOutput", output);
        });
    }

    @Test
    void testValidateOas_WithoutOpenApiJson() throws Exception {
        // Given
        CodeGenerationRequest request = createBasicRequest();
        request.setOas("http://example.com/api");

        // When
        ReflectionTestUtils.invokeMethod(service, "validateOas", request);

        // Then
        assertEquals("http://example.com/api/openapi.json", request.getOas());
    }

    @Test
    void testValidateOas_WithOpenApiJson() throws Exception {
        // Given
        CodeGenerationRequest request = createBasicRequest();
        request.setOas("http://example.com/api/openapi.json");

        // When
        ReflectionTestUtils.invokeMethod(service, "validateOas", request);

        // Then
        assertEquals("http://example.com/api/openapi.json", request.getOas());
    }

    @Test
    void testGetAdditionalProps_WithInterfaceOnlyTrue() throws Exception {
        // Given
        CodeGenerationRequest request = createBasicRequest();
        request.setInterfaceOnly(true);
        request.setVersion("1.0.0");
        request.setRequestAddress("http://localhost:3000");

        ApplicationDto applicationDto = new ApplicationDto();
        applicationDto.setClientId("client123");
        applicationDto.setClientSecret("secret456");

        // When
        String additionalProps = (String) ReflectionTestUtils.invokeMethod(
            service, "getAdditionalProps", request, 11, applicationDto);

        // Then
        assertNotNull(additionalProps);
        assertTrue(additionalProps.contains("javaVersion=11"));
        assertTrue(additionalProps.contains("artifactVersion=1.0.0"));
        assertTrue(additionalProps.contains("interfaceOnly=true"));
        assertTrue(additionalProps.contains("tapClientId=client123"));
        assertTrue(additionalProps.contains("tapClientSecret=secret456"));
    }

    @Test
    void testGetAdditionalProps_WithInterfaceOnlyFalse() throws Exception {
        // Given
        CodeGenerationRequest request = createBasicRequest();
        request.setInterfaceOnly(false);
        request.setVersion("2.0.0");
        request.setRequestAddress("http://localhost:3000");

        ApplicationDto applicationDto = new ApplicationDto();
        applicationDto.setClientId("client789");
        applicationDto.setClientSecret("secret012");

        // When
        String additionalProps = (String) ReflectionTestUtils.invokeMethod(
            service, "getAdditionalProps", request, 17, applicationDto);

        // Then
        assertNotNull(additionalProps);
        assertTrue(additionalProps.contains("javaVersion=17"));
        assertTrue(additionalProps.contains("artifactVersion=2.0.0"));
        assertTrue(additionalProps.contains("interfaceOnly=false"));
    }

    /**
     * Helper method to create a basic CodeGenerationRequest for testing
     */
    private CodeGenerationRequest createBasicRequest() {
        CodeGenerationRequest request = new CodeGenerationRequest();
        request.setOas("http://example.com/api/openapi.json");
        request.setLan("spring");
        request.setPackageName("io.tapdata.test");
        request.setArtifactId("test-sdk");
        request.setGroupId("io.tapdata");
        request.setVersion("1.0.0");
        request.setTemplateLibrary("spring-cloud");
        request.setRequestAddress("http://localhost:3000");
        request.setClientId(new ObjectId().toString());
        request.setModuleIds(Arrays.asList("module1", "module2"));
        return request;
    }
}