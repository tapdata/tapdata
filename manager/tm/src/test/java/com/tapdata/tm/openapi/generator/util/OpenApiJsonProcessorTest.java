package com.tapdata.tm.openapi.generator.util;

import com.tapdata.tm.openapi.generator.dto.CodeGenerationRequest;
import com.tapdata.tm.openapi.generator.exception.CodeGenerationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.client.RestTemplate;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

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

    @BeforeEach
    void setUp() {
        processor = new OpenApiJsonProcessor(restTemplate);
    }

    @Test
    void testProcessOpenapiJson_Success() throws Exception {
        // Given
        String mockOpenApiJson = """
            {
              "openapi": "3.0.0",
              "info": {
                "title": "Test API",
                "version": "1.0.0"
              },
              "paths": {
                "/test": {
                  "get": {
                    "x-api-id": "test-api",
                    "summary": "Test endpoint",
                    "parameters": [
                      {
                        "name": "page",
                        "in": "query",
                        "schema": {
                          "type": "string"
                        }
                      }
                    ]
                  }
                }
              },
              "components": {
                "securitySchemes": {
                  "OAuth2": {
                    "type": "oauth2",
                    "flows": {
                      "implicit": {
                        "authorizationUrl": "https://example.com/auth"
                      },
                      "clientCredentials": {
                        "tokenUrl": "https://example.com/token"
                      }
                    }
                  }
                }
              }
            }
            """;

        CodeGenerationRequest request = new CodeGenerationRequest();
        request.setOas("https://example.com/openapi.json");
        request.setModuleIds(Arrays.asList("test-api"));

        when(restTemplate.getForObject(eq("https://example.com/openapi.json"), eq(String.class)))
            .thenReturn(mockOpenApiJson);

        Path tempDir = Files.createTempDirectory("test-openapi");

        // When
        Path result = processor.processOpenapiJson(request, tempDir);

        // Then
        assertNotNull(result);
        assertTrue(Files.exists(result));
        assertTrue(result.toString().contains("openapi-"));
        assertTrue(result.toString().endsWith(".json"));

        // Verify the processed content
        String processedContent = Files.readString(result);
        assertNotNull(processedContent);
        assertTrue(processedContent.contains("Test API"));
        
        // Verify that implicit OAuth flow was removed
        assertFalse(processedContent.contains("implicit"));
        assertTrue(processedContent.contains("clientCredentials"));

        // Cleanup
        Files.deleteIfExists(result);
        Files.deleteIfExists(tempDir);
    }

    @Test
    void testProcessOpenapiJson_InvalidUrl() {
        // Given
        CodeGenerationRequest request = new CodeGenerationRequest();
        request.setOas("https://invalid-url.com/openapi.json");

        when(restTemplate.getForObject(anyString(), eq(String.class)))
            .thenThrow(new RuntimeException("Connection failed"));

        Path tempDir;
        try {
            tempDir = Files.createTempDirectory("test-openapi");
        } catch (Exception e) {
            fail("Failed to create temp directory");
            return;
        }

        // When & Then
        assertThrows(CodeGenerationException.class, () -> {
            processor.processOpenapiJson(request, tempDir);
        });

        // Cleanup
        try {
            Files.deleteIfExists(tempDir);
        } catch (Exception ignored) {
        }
    }

    @Test
    void testCreateSecureTempDirectoryForJson() throws Exception {
        // Given
        Path baseTempDir = Files.createTempDirectory("test-base");

        // When
        Path result = OpenApiJsonProcessor.createSecureTempDirectoryForJson(baseTempDir);

        // Then
        assertNotNull(result);
        assertTrue(Files.exists(result));
        assertTrue(Files.isDirectory(result));
        assertTrue(result.toString().contains("openapi-json-"));

        // Cleanup
        Files.deleteIfExists(result);
        Files.deleteIfExists(baseTempDir);
    }

    @Test
    void testProcessOpenapiJson_WithFilterParameter() throws Exception {
        // Given - OpenAPI JSON with GET operation containing filter parameter
        String mockOpenApiJson = """
            {
              "openapi": "3.0.0",
              "info": {
                "title": "Test API",
                "version": "1.0.0"
              },
              "paths": {
                "/users": {
                  "x-table-name": "users",
                  "get": {
                    "x-api-id": "test-api",
                    "summary": "Get users",
                    "parameters": [
                      {
                        "name": "filter",
                        "in": "query",
                        "schema": {
                          "type": "object",
                          "properties": {
                            "name": {
                              "type": "string"
                            },
                            "age": {
                              "type": "integer"
                            }
                          }
                        }
                      },
                      {
                        "name": "page",
                        "in": "query",
                        "schema": {
                          "type": "string"
                        }
                      }
                    ]
                  }
                }
              }
            }
            """;

        CodeGenerationRequest request = new CodeGenerationRequest();
        request.setOas("https://example.com/openapi.json");
        request.setModuleIds(Arrays.asList("test-api"));

        when(restTemplate.getForObject(eq("https://example.com/openapi.json"), eq(String.class)))
            .thenReturn(mockOpenApiJson);

        Path tempDir = Files.createTempDirectory("test-openapi");

        // When
        Path result = processor.processOpenapiJson(request, tempDir);

        // Then
        assertNotNull(result);
        assertTrue(Files.exists(result));

        // Verify the processed content
        String processedContent = Files.readString(result);
        assertNotNull(processedContent);

        // Verify that filter schema was extracted to components/schemas
        assertTrue(processedContent.contains("\"components\""));
        assertTrue(processedContent.contains("\"schemas\""));
        assertTrue(processedContent.contains("\"users_filter\""));

        // Verify that filter parameter now uses $ref
        assertTrue(processedContent.contains("\"$ref\" : \"#/components/schemas/users_filter\""));

        // Cleanup
        Files.deleteIfExists(result);
        Files.deleteIfExists(tempDir);
    }

    @Test
    void testProcessOpenapiJson_WithFilterTransformation() throws Exception {
        // Given - OpenAPI JSON with GET operation containing filter parameter with offset and skip
        String mockOpenApiJson = """
            {
              "openapi": "3.0.0",
              "info": {
                "title": "Test API",
                "version": "1.0.0"
              },
              "paths": {
                "/orders": {
                  "x-table-name": "orders",
                  "get": {
                    "x-api-id": "test-api",
                    "summary": "Get orders",
                    "parameters": [
                      {
                        "name": "filter",
                        "in": "query",
                        "schema": {
                          "type": "object",
                          "properties": {
                            "where": {
                              "type": "object"
                            },
                            "offset": {
                              "type": "integer",
                              "description": "Zero-based offset"
                            },
                            "skip": {
                              "type": "integer",
                              "description": "Zero-based skip documents"
                            },
                            "limit": {
                              "type": "integer"
                            },
                            "order": {
                              "type": "object"
                            }
                          }
                        }
                      }
                    ]
                  }
                }
              }
            }
            """;

        when(restTemplate.getForObject(anyString(), eq(String.class))).thenReturn(mockOpenApiJson);

        CodeGenerationRequest request = new CodeGenerationRequest();
        request.setOas("http://example.com/openapi.json");
        request.setModuleIds(Arrays.asList("test-api"));

        Path tempDir = Files.createTempDirectory("test-openapi");

        // When
        Path result = processor.processOpenapiJson(request, tempDir);

        // Then
        assertNotNull(result);
        assertTrue(Files.exists(result));

        // Verify the processed content
        String processedContent = Files.readString(result);
        assertNotNull(processedContent);

        // Verify that filter schema was extracted to components/schemas
        assertTrue(processedContent.contains("\"components\""));
        assertTrue(processedContent.contains("\"schemas\""));
        assertTrue(processedContent.contains("\"orders_filter\""));

        // Verify that filter parameter now uses $ref
        assertTrue(processedContent.contains("\"$ref\" : \"#/components/schemas/orders_filter\""));

        // Verify transformation: offset should be removed
        assertFalse(processedContent.contains("\"offset\""));

        // Verify transformation: skip should be changed to page
        assertFalse(processedContent.contains("\"skip\""));
        assertTrue(processedContent.contains("\"page\""));

        // Verify other properties are preserved
        assertTrue(processedContent.contains("\"where\""));
        assertTrue(processedContent.contains("\"limit\""));
        assertTrue(processedContent.contains("\"order\""));

        // Cleanup
        Files.deleteIfExists(result);
        Files.deleteIfExists(tempDir);
    }

    @Test
    void testProcessOpenapiJson_SkipCustomerQueryPostOperations() throws Exception {
        // Given
        String mockOpenApiJson = """
            {
              "openapi": "3.0.0",
              "info": {
                "title": "Test API",
                "version": "1.0.0"
              },
              "paths": {
                "/test": {
                  "get": {
                    "x-api-id": "test-api",
                    "summary": "Test GET endpoint"
                  },
                  "post": {
                    "x-api-id": "test-api",
                    "x-operation-name": "customerQueryData",
                    "summary": "Test POST endpoint that should be skipped"
                  }
                },
                "/test2": {
                  "post": {
                    "x-api-id": "test-api-2",
                    "x-operation-name": "normalOperation",
                    "summary": "Test POST endpoint that should be kept"
                  }
                },
                "/test3": {
                  "post": {
                    "x-api-id": "test-api-3",
                    "x-operation-name": "customerQuerySomething",
                    "summary": "Another POST endpoint that should be skipped"
                  }
                }
              }
            }
            """;

        CodeGenerationRequest request = new CodeGenerationRequest();
        request.setOas("https://example.com/openapi.json");
        request.setModuleIds(Arrays.asList("test-api", "test-api-2", "test-api-3"));

        when(restTemplate.getForObject(eq("https://example.com/openapi.json"), eq(String.class)))
            .thenReturn(mockOpenApiJson);

        Path tempDir = Files.createTempDirectory("test-openapi");

        // When
        Path result = processor.processOpenapiJson(request, tempDir);

        // Then
        assertNotNull(result);
        assertTrue(Files.exists(result));

        // Verify the processed content
        String processedContent = Files.readString(result);
        assertNotNull(processedContent);

        // Verify that GET operation is kept
        assertTrue(processedContent.contains("Test GET endpoint"));

        // Verify that POST operations with x-operation-name starting with "customerQuery" are removed
        assertFalse(processedContent.contains("customerQueryData"));
        assertFalse(processedContent.contains("customerQuerySomething"));
        assertFalse(processedContent.contains("Test POST endpoint that should be skipped"));
        assertFalse(processedContent.contains("Another POST endpoint that should be skipped"));

        // Verify that normal POST operation is kept
        assertTrue(processedContent.contains("normalOperation"));
        assertTrue(processedContent.contains("Test POST endpoint that should be kept"));

        // Cleanup
        Files.deleteIfExists(result);
        Files.deleteIfExists(tempDir);
    }
}
