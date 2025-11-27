package com.tapdata.tm.openapi.generator.util;

import com.tapdata.tm.openapi.generator.dto.CodeGenerationRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.web.client.RestTemplate;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Test class for OpenApiJsonProcessor count field modification functionality
 *
 * @author sam
 * @date 2024/12/19
 */
public class OpenApiJsonProcessorCountTest {

    @Mock
    private RestTemplate restTemplate;

    private OpenApiJsonProcessor processor;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        processor = new OpenApiJsonProcessor(restTemplate);
    }

    @Test
    void testProcessOpenapiJsonWithCountFieldModification() throws Exception {
        // Given
        String mockOpenApiJson = """
            {
              "openapi": "3.0.0",
              "info": {
                "title": "Test API",
                "version": "1.0.0"
              },
              "paths": {
                "/users": {
                  "get": {
                    "x-api-id": "users-list",
                    "responses": {
                      "200": {
                        "description": "Success",
                        "content": {
                          "application/json": {
                            "schema": {
                              "type": "object",
                              "properties": {
                                "count": {
                                  "type": "string",
                                  "description": "Total count of users"
                                },
                                "data": {
                                  "type": "array",
                                  "items": {
                                    "type": "object",
                                    "properties": {
                                      "id": {"type": "string"},
                                      "name": {"type": "string"}
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            """;

        CodeGenerationRequest request = new CodeGenerationRequest();
        request.setOas("http://example.com/openapi.json");
        request.setModuleIds(Arrays.asList("users-list"));

        when(restTemplate.getForObject(eq("http://example.com/openapi.json"), eq(String.class)))
            .thenReturn(mockOpenApiJson);

        Path tempDir = Files.createTempDirectory("test-openapi-count");

        // When
        Path result = processor.processOpenapiJson(request, tempDir);

        // Then
        assertNotNull(result);
        assertTrue(Files.exists(result));

        // Verify the processed content
        String processedContent = Files.readString(result);
        assertNotNull(processedContent);

        // Verify that count field was modified to integer with int32 format
        assertTrue(processedContent.contains("\"count\""));
        assertTrue(processedContent.contains("\"type\" : \"integer\""));
        assertTrue(processedContent.contains("\"format\" : \"int32\""));

        // Verify that the original string type was replaced
        assertFalse(processedContent.contains("\"type\" : \"string\"") && 
                   processedContent.contains("\"count\"") && 
                   processedContent.indexOf("\"type\" : \"string\"") < processedContent.indexOf("\"count\""));

        // Cleanup
        Files.deleteIfExists(result);
        Files.deleteIfExists(tempDir);
    }

    @Test
    void testProcessOpenapiJsonWithNestedCountField() throws Exception {
        // Given
        String mockOpenApiJson = """
            {
              "openapi": "3.0.0",
              "info": {
                "title": "Test API",
                "version": "1.0.0"
              },
              "paths": {
                "/orders": {
                  "get": {
                    "x-api-id": "orders-list",
                    "responses": {
                      "200": {
                        "description": "Success",
                        "content": {
                          "application/json": {
                            "schema": {
                              "type": "object",
                              "properties": {
                                "result": {
                                  "type": "object",
                                  "properties": {
                                    "count": {
                                      "type": "number",
                                      "description": "Total count of orders"
                                    },
                                    "items": {
                                      "type": "array",
                                      "items": {
                                        "type": "object",
                                        "properties": {
                                          "id": {"type": "string"},
                                          "total": {"type": "number"}
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
            """;

        CodeGenerationRequest request = new CodeGenerationRequest();
        request.setOas("http://example.com/openapi.json");
        request.setModuleIds(Arrays.asList("orders-list"));

        when(restTemplate.getForObject(eq("http://example.com/openapi.json"), eq(String.class)))
            .thenReturn(mockOpenApiJson);

        Path tempDir = Files.createTempDirectory("test-openapi-nested-count");

        // When
        Path result = processor.processOpenapiJson(request, tempDir);

        // Then
        assertNotNull(result);
        assertTrue(Files.exists(result));

        // Verify the processed content
        String processedContent = Files.readString(result);
        assertNotNull(processedContent);

        // Verify that nested count field was modified to integer with int32 format
        assertTrue(processedContent.contains("\"count\""));
        assertTrue(processedContent.contains("\"type\" : \"integer\""));
        assertTrue(processedContent.contains("\"format\" : \"int32\""));

        // Cleanup
        Files.deleteIfExists(result);
        Files.deleteIfExists(tempDir);
    }
}
