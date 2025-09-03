package com.tapdata.tm.openapi.generator.util;

import com.tapdata.tm.openapi.generator.dto.CodeGenerationRequest;
import com.tapdata.tm.openapi.generator.exception.CodeGenerationException;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.oas.models.*;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Test class for OpenApiJsonProcessor
 *
 * @author sam
 * @date 2024/12/19
 */
@ExtendWith(MockitoExtension.class)
class OpenApiJsonProcessorTest {

    @Mock
    private RestTemplate restTemplate;

    private OpenApiJsonProcessor processor;

    @TempDir
    Path tempDir;

    private CodeGenerationRequest request;

    @BeforeEach
    void setUp() {
        processor = new OpenApiJsonProcessor(restTemplate);

        request = new CodeGenerationRequest();
        request.setOas("https://example.com/api/openapi.json");
        request.setArtifactId("test-sdk");
        request.setModuleIds(Arrays.asList("module1", "module2"));
    }

    @Test
    void testProcessOpenapiJson_Success() throws Exception {
        // Given
        String validOpenApiJson = createValidOpenApiJson();
        when(restTemplate.getForObject(anyString(), eq(String.class))).thenReturn(validOpenApiJson);

        // When
        Path result = processor.processOpenapiJson(request, tempDir);

        // Then
        assertNotNull(result);
        assertTrue(Files.exists(result));
        assertTrue(result.getFileName().toString().startsWith("openapi-"));
        assertTrue(result.getFileName().toString().endsWith(".json"));

        // Verify the content is valid JSON
        String content = Files.readString(result);
        assertNotNull(content);
        assertFalse(content.trim().isEmpty());

        // Verify RestTemplate was called
        verify(restTemplate).getForObject(request.getOas(), String.class);
    }

    @Test
    void testProcessOpenapiJson_EmptyResponse() {
        // Given
        when(restTemplate.getForObject(anyString(), eq(String.class))).thenReturn("");

        // When & Then
        CodeGenerationException exception = assertThrows(CodeGenerationException.class,
            () -> processor.processOpenapiJson(request, tempDir));

        assertTrue(exception.getMessage().contains("Received empty response"));
        verify(restTemplate).getForObject(request.getOas(), String.class);
    }

    @Test
    void testProcessOpenapiJson_NullResponse() {
        // Given
        when(restTemplate.getForObject(anyString(), eq(String.class))).thenReturn(null);

        // When & Then
        CodeGenerationException exception = assertThrows(CodeGenerationException.class,
            () -> processor.processOpenapiJson(request, tempDir));

        assertTrue(exception.getMessage().contains("Received empty response"));
        verify(restTemplate).getForObject(request.getOas(), String.class);
    }

    @Test
    void testProcessOpenapiJson_RestTemplateException() {
        // Given
        when(restTemplate.getForObject(anyString(), eq(String.class)))
            .thenThrow(new RestClientException("Network error"));

        // When & Then
        CodeGenerationException exception = assertThrows(CodeGenerationException.class,
            () -> processor.processOpenapiJson(request, tempDir));

        assertTrue(exception.getMessage().contains("Failed to download OpenAPI JSON"));
        assertTrue(exception.getCause() instanceof RestClientException);
        verify(restTemplate).getForObject(request.getOas(), String.class);
    }

    @Test
    void testProcessOpenapiJson_InvalidJson() {
        // Given
        String invalidJson = "{ invalid json content";
        when(restTemplate.getForObject(anyString(), eq(String.class))).thenReturn(invalidJson);

        // When & Then
        CodeGenerationException exception = assertThrows(CodeGenerationException.class,
            () -> processor.processOpenapiJson(request, tempDir));

        assertTrue(exception.getMessage().contains("Invalid JSON format"));
        verify(restTemplate).getForObject(request.getOas(), String.class);
    }

    @Test
    void testProcessOpenapiJson_InvalidOpenApiSpec() {
        // Given
        String invalidOpenApiJson = "{ \"notOpenApi\": \"content\" }";
        when(restTemplate.getForObject(anyString(), eq(String.class))).thenReturn(invalidOpenApiJson);

        // When & Then
        CodeGenerationException exception = assertThrows(CodeGenerationException.class,
            () -> processor.processOpenapiJson(request, tempDir));

        assertTrue(exception.getMessage().contains("Failed to parse OpenAPI JSON"));
        verify(restTemplate).getForObject(request.getOas(), String.class);
    }

    @Test
    void testProcessOpenapiJson_WithModuleFiltering() throws Exception {
        // Given
        String openApiJsonWithModules = createOpenApiJsonWithModules();
        when(restTemplate.getForObject(anyString(), eq(String.class))).thenReturn(openApiJsonWithModules);

        // When
        Path result = processor.processOpenapiJson(request, tempDir);

        // Then
        assertNotNull(result);
        assertTrue(Files.exists(result));

        // Verify the processed content
        String content = Files.readString(result);
        assertNotNull(content);

        // The content should be filtered based on moduleIds
        assertTrue(content.contains("module1") || content.contains("module2"));
        verify(restTemplate).getForObject(request.getOas(), String.class);
    }

    @Test
    void testProcessOpenapiJson_EmptyModuleIds() throws Exception {
        // Given
        request.setModuleIds(Collections.emptyList());
        String validOpenApiJson = createValidOpenApiJson();
        when(restTemplate.getForObject(anyString(), eq(String.class))).thenReturn(validOpenApiJson);

        // When
        Path result = processor.processOpenapiJson(request, tempDir);

        // Then
        assertNotNull(result);
        assertTrue(Files.exists(result));
        verify(restTemplate).getForObject(request.getOas(), String.class);
    }

    @Test
    void testProcessOpenapiJson_NullModuleIds() throws Exception {
        // Given
        request.setModuleIds(null);
        String validOpenApiJson = createValidOpenApiJson();
        when(restTemplate.getForObject(anyString(), eq(String.class))).thenReturn(validOpenApiJson);

        // When
        Path result = processor.processOpenapiJson(request, tempDir);

        // Then
        assertNotNull(result);
        assertTrue(Files.exists(result));
        verify(restTemplate).getForObject(request.getOas(), String.class);
    }

    @Test
    void testCreateSecureTempDirectoryForJson() throws IOException {
        // When
        Path secureTempDir = OpenApiJsonProcessor.createSecureTempDirectoryForJson(tempDir);

        // Then
        assertNotNull(secureTempDir);
        assertTrue(Files.exists(secureTempDir));
        assertTrue(Files.isDirectory(secureTempDir));
        assertTrue(secureTempDir.getFileName().toString().startsWith("openapi-json-"));
        assertTrue(secureTempDir.getParent().equals(tempDir));
    }

    @Test
    void testProcessOpenapiJson_WithPageLimitParameters() throws Exception {
        // Given
        String openApiJsonWithPageLimit = createOpenApiJsonWithPageLimitParameters();
        when(restTemplate.getForObject(anyString(), eq(String.class))).thenReturn(openApiJsonWithPageLimit);

        // When
        Path result = processor.processOpenapiJson(request, tempDir);

        // Then
        assertNotNull(result);
        assertTrue(Files.exists(result));

        String content = Files.readString(result);
        assertNotNull(content);

        // Verify page and limit parameters are processed correctly
        assertTrue(content.contains("integer"));
        verify(restTemplate).getForObject(request.getOas(), String.class);
    }

    @Test
    void testProcessOpenapiJson_WithCountFieldModification() throws Exception {
        // Given
        String openApiJsonWithCountFields = createOpenApiJsonWithCountFields();
        when(restTemplate.getForObject(anyString(), eq(String.class))).thenReturn(openApiJsonWithCountFields);

        // When
        Path result = processor.processOpenapiJson(request, tempDir);

        // Then
        assertNotNull(result);
        assertTrue(Files.exists(result));

        String content = Files.readString(result);
        assertNotNull(content);
        verify(restTemplate).getForObject(request.getOas(), String.class);
    }

    @Test
    void testProcessOpenapiJson_WithNon200ResponseRemoval() throws Exception {
        // Given
        String openApiJsonWithMultipleResponses = createOpenApiJsonWithMultipleResponses();
        when(restTemplate.getForObject(anyString(), eq(String.class))).thenReturn(openApiJsonWithMultipleResponses);

        // When
        Path result = processor.processOpenapiJson(request, tempDir);

        // Then
        assertNotNull(result);
        assertTrue(Files.exists(result));

        String content = Files.readString(result);
        assertNotNull(content);
        // Should only contain 200 responses
        assertTrue(content.contains("\"200\""));
        verify(restTemplate).getForObject(request.getOas(), String.class);
    }

    @Test
    void testProcessOpenapiJson_WithTagModification() throws Exception {
        // Given
        String openApiJsonWithTags = createOpenApiJsonWithTags();
        when(restTemplate.getForObject(anyString(), eq(String.class))).thenReturn(openApiJsonWithTags);

        // When
        Path result = processor.processOpenapiJson(request, tempDir);

        // Then
        assertNotNull(result);
        assertTrue(Files.exists(result));

        String content = Files.readString(result);
        assertNotNull(content);
        // Should contain the artifactId as tag
        assertTrue(content.contains(request.getArtifactId()));
        verify(restTemplate).getForObject(request.getOas(), String.class);
    }

    @Test
    void testProcessOpenapiJson_LargeFile() throws Exception {
        // Given
        String largeOpenApiJson = createLargeOpenApiJson();
        when(restTemplate.getForObject(anyString(), eq(String.class))).thenReturn(largeOpenApiJson);

        // When
        Path result = processor.processOpenapiJson(request, tempDir);

        // Then
        assertNotNull(result);
        assertTrue(Files.exists(result));
        assertTrue(Files.size(result) > 0);
        verify(restTemplate).getForObject(request.getOas(), String.class);
    }

    // Helper methods to create test data

    private String createValidOpenApiJson() {
        OpenAPI openAPI = new OpenAPI();
        openAPI.setOpenapi("3.0.0");

        Info info = new Info();
        info.setTitle("Test API");
        info.setVersion("1.0.0");
        openAPI.setInfo(info);

        Paths paths = new Paths();
        PathItem pathItem = new PathItem();

        Operation getOperation = new Operation();
        getOperation.setOperationId("getTest");
        getOperation.addExtension("x-api-id", "module1");

        ApiResponses responses = new ApiResponses();
        ApiResponse response200 = new ApiResponse();
        response200.setDescription("Success");
        responses.addApiResponse("200", response200);
        getOperation.setResponses(responses);

        pathItem.setGet(getOperation);
        paths.addPathItem("/test", pathItem);
        openAPI.setPaths(paths);

        try {
            return Json.pretty(openAPI);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create test OpenAPI JSON", e);
        }
    }

    private String createOpenApiJsonWithModules() {
        OpenAPI openAPI = new OpenAPI();
        openAPI.setOpenapi("3.0.0");

        Info info = new Info();
        info.setTitle("Test API with Modules");
        info.setVersion("1.0.0");
        openAPI.setInfo(info);

        Paths paths = new Paths();

        // Add operation with module1
        PathItem pathItem1 = new PathItem();
        Operation getOperation1 = new Operation();
        getOperation1.setOperationId("getModule1");
        getOperation1.addExtension("x-api-id", "module1");

        ApiResponses responses1 = new ApiResponses();
        ApiResponse response200_1 = new ApiResponse();
        response200_1.setDescription("Success");
        responses1.addApiResponse("200", response200_1);
        getOperation1.setResponses(responses1);

        pathItem1.setGet(getOperation1);
        paths.addPathItem("/module1", pathItem1);

        // Add operation with module2
        PathItem pathItem2 = new PathItem();
        Operation getOperation2 = new Operation();
        getOperation2.setOperationId("getModule2");
        getOperation2.addExtension("x-api-id", "module2");

        ApiResponses responses2 = new ApiResponses();
        ApiResponse response200_2 = new ApiResponse();
        response200_2.setDescription("Success");
        responses2.addApiResponse("200", response200_2);
        getOperation2.setResponses(responses2);

        pathItem2.setGet(getOperation2);
        paths.addPathItem("/module2", pathItem2);

        // Add operation with module3 (should be filtered out)
        PathItem pathItem3 = new PathItem();
        Operation getOperation3 = new Operation();
        getOperation3.setOperationId("getModule3");
        getOperation3.addExtension("x-api-id", "module3");

        ApiResponses responses3 = new ApiResponses();
        ApiResponse response200_3 = new ApiResponse();
        response200_3.setDescription("Success");
        responses3.addApiResponse("200", response200_3);
        getOperation3.setResponses(responses3);

        pathItem3.setGet(getOperation3);
        paths.addPathItem("/module3", pathItem3);

        openAPI.setPaths(paths);

        try {
            return Json.pretty(openAPI);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create test OpenAPI JSON with modules", e);
        }
    }

    private String createOpenApiJsonWithPageLimitParameters() {
        OpenAPI openAPI = new OpenAPI();
        openAPI.setOpenapi("3.0.0");

        Info info = new Info();
        info.setTitle("Test API with Page Limit");
        info.setVersion("1.0.0");
        openAPI.setInfo(info);

        Paths paths = new Paths();
        PathItem pathItem = new PathItem();

        Operation getOperation = new Operation();
        getOperation.setOperationId("getWithPageLimit");
        getOperation.addExtension("x-api-id", "module1");

        // Add page parameter
        Parameter pageParam = new Parameter();
        pageParam.setName("page");
        pageParam.setIn("query");
        Schema pageSchema = new Schema();
        pageSchema.setType("string");
        pageParam.setSchema(pageSchema);
        getOperation.addParametersItem(pageParam);

        // Add limit parameter
        Parameter limitParam = new Parameter();
        limitParam.setName("limit");
        limitParam.setIn("query");
        Schema limitSchema = new Schema();
        limitSchema.setType("string");
        limitParam.setSchema(limitSchema);
        getOperation.addParametersItem(limitParam);

        // Add filename parameter (should be filtered out)
        Parameter filenameParam = new Parameter();
        filenameParam.setName("filename");
        filenameParam.setIn("query");
        Schema filenameSchema = new Schema();
        filenameSchema.setType("string");
        filenameParam.setSchema(filenameSchema);
        getOperation.addParametersItem(filenameParam);

        ApiResponses responses = new ApiResponses();
        ApiResponse response200 = new ApiResponse();
        response200.setDescription("Success");
        responses.addApiResponse("200", response200);
        getOperation.setResponses(responses);

        pathItem.setGet(getOperation);
        paths.addPathItem("/test", pathItem);
        openAPI.setPaths(paths);

        try {
            return Json.pretty(openAPI);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create test OpenAPI JSON with page limit", e);
        }
    }

    private String createOpenApiJsonWithCountFields() {
        OpenAPI openAPI = new OpenAPI();
        openAPI.setOpenapi("3.0.0");

        Info info = new Info();
        info.setTitle("Test API with Count Fields");
        info.setVersion("1.0.0");
        openAPI.setInfo(info);

        Paths paths = new Paths();
        PathItem pathItem = new PathItem();

        Operation getOperation = new Operation();
        getOperation.setOperationId("getWithCount");
        getOperation.addExtension("x-api-id", "module1");

        ApiResponses responses = new ApiResponses();
        ApiResponse response200 = new ApiResponse();
        response200.setDescription("Success");

        Content content = new Content();
        MediaType mediaType = new MediaType();

        Schema responseSchema = new Schema();
        responseSchema.setType("object");
        Map<String, Schema> properties = new HashMap<>();

        Schema countSchema = new Schema();
        countSchema.setType("integer");
        properties.put("count", countSchema);

        Schema totalCountSchema = new Schema();
        totalCountSchema.setType("integer");
        properties.put("totalCount", totalCountSchema);

        responseSchema.setProperties(properties);
        mediaType.setSchema(responseSchema);
        content.addMediaType("application/json", mediaType);
        response200.setContent(content);

        responses.addApiResponse("200", response200);
        getOperation.setResponses(responses);

        pathItem.setGet(getOperation);
        paths.addPathItem("/test", pathItem);
        openAPI.setPaths(paths);

        try {
            return Json.pretty(openAPI);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create test OpenAPI JSON with count fields", e);
        }
    }

    private String createOpenApiJsonWithMultipleResponses() {
        OpenAPI openAPI = new OpenAPI();
        openAPI.setOpenapi("3.0.0");

        Info info = new Info();
        info.setTitle("Test API with Multiple Responses");
        info.setVersion("1.0.0");
        openAPI.setInfo(info);

        Paths paths = new Paths();
        PathItem pathItem = new PathItem();

        Operation getOperation = new Operation();
        getOperation.setOperationId("getWithMultipleResponses");
        getOperation.addExtension("x-api-id", "module1");

        ApiResponses responses = new ApiResponses();

        // Add 200 response
        ApiResponse response200 = new ApiResponse();
        response200.setDescription("Success");
        responses.addApiResponse("200", response200);

        // Add 400 response (should be removed)
        ApiResponse response400 = new ApiResponse();
        response400.setDescription("Bad Request");
        responses.addApiResponse("400", response400);

        // Add 500 response (should be removed)
        ApiResponse response500 = new ApiResponse();
        response500.setDescription("Internal Server Error");
        responses.addApiResponse("500", response500);

        getOperation.setResponses(responses);

        pathItem.setGet(getOperation);
        paths.addPathItem("/test", pathItem);
        openAPI.setPaths(paths);

        try {
            return Json.pretty(openAPI);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create test OpenAPI JSON with multiple responses", e);
        }
    }

    private String createOpenApiJsonWithTags() {
        OpenAPI openAPI = new OpenAPI();
        openAPI.setOpenapi("3.0.0");

        Info info = new Info();
        info.setTitle("Test API with Tags");
        info.setVersion("1.0.0");
        openAPI.setInfo(info);

        Paths paths = new Paths();
        PathItem pathItem = new PathItem();

        Operation getOperation = new Operation();
        getOperation.setOperationId("getWithTags");
        getOperation.addExtension("x-api-id", "module1");
        getOperation.addTagsItem("original-tag");

        ApiResponses responses = new ApiResponses();
        ApiResponse response200 = new ApiResponse();
        response200.setDescription("Success");
        responses.addApiResponse("200", response200);
        getOperation.setResponses(responses);

        pathItem.setGet(getOperation);
        paths.addPathItem("/test", pathItem);
        openAPI.setPaths(paths);

        try {
            return Json.pretty(openAPI);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create test OpenAPI JSON with tags", e);
        }
    }

    private String createLargeOpenApiJson() {
        OpenAPI openAPI = new OpenAPI();
        openAPI.setOpenapi("3.0.0");

        Info info = new Info();
        info.setTitle("Large Test API");
        info.setVersion("1.0.0");
        info.setDescription("This is a large OpenAPI specification for testing purposes with many endpoints and schemas");
        openAPI.setInfo(info);

        Paths paths = new Paths();

        // Create multiple paths to make it large
        for (int i = 1; i <= 50; i++) {
            PathItem pathItem = new PathItem();

            Operation getOperation = new Operation();
            getOperation.setOperationId("getEndpoint" + i);
            getOperation.addExtension("x-api-id", "module" + (i % 3 + 1)); // Cycle through module1, module2, module3
            getOperation.setDescription("This is endpoint number " + i + " with a long description to make the file larger");

            ApiResponses responses = new ApiResponses();
            ApiResponse response200 = new ApiResponse();
            response200.setDescription("Success response for endpoint " + i);
            responses.addApiResponse("200", response200);
            getOperation.setResponses(responses);

            pathItem.setGet(getOperation);
            paths.addPathItem("/endpoint" + i, pathItem);
        }

        openAPI.setPaths(paths);

        try {
            return Json.pretty(openAPI);
        } catch (Exception e) {
            throw new RuntimeException("Failed to create large test OpenAPI JSON", e);
        }
    }
}
