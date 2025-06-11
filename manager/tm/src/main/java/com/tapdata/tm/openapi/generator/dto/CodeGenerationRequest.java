package com.tapdata.tm.openapi.generator.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

import jakarta.validation.constraints.NotBlank;

/**
 * OpenAPI code generation request DTO
 *
 * @author tapdata
 * @date 2024/12/19
 */
@Data
@Schema(description = "OpenAPI code generation request")
public class CodeGenerationRequest {

    @NotBlank(message = "OpenAPI specification URL cannot be empty")
    @Schema(description = "URL of the OpenAPI specification file", example = "https://example.com/api/openapi.json", required = true)
    private String oas;

    @Schema(description = "Programming language for code generation (only 'java' is supported)", example = "java", defaultValue = "java")
    private String lan = "java";

    @Schema(description = "Package name for generated code", example = "io.tapdata.sdk", defaultValue = "io.tapdata.sdk")
    private String packageName = "io.tapdata.sdk";

    @Schema(description = "Artifact ID", example = "tapdata-sdk", defaultValue = "tapdata-sdk")
    private String artifactId = "tapdata-sdk";

    @Schema(description = "Group ID", example = "io.tapdata", defaultValue = "io.tapdata")
    private String groupId = "io.tapdata";

    @Schema(description = "Version", example = "1.0.0", defaultValue = "1.0.0")
    private String version = "1.0.0";

    @NotBlank(message = "Api Server Client Id cannot be empty")
    @Schema(description = "Api Server Client Id")
    private String clientId;

    @NotBlank(message = "Request address cannot be empty")
    @Schema(description = "Request address", example = "http://127.0.0.1:3030")
    private String requestAddress;
}
