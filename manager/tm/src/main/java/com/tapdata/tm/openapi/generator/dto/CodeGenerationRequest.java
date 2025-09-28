package com.tapdata.tm.openapi.generator.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

import java.util.List;

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
    @Schema(description = "URL of the OpenAPI specification file", example = "https://example.com/api/openapi.json", requiredMode = Schema.RequiredMode.REQUIRED)
    private String oas;

    @Schema(description = "Programming language for code generation (only 'spring' is supported)", example = "spring", defaultValue = "spring")
    private String lan = "spring";

    @Schema(description = "Package name for generated code", example = "io.tapdata", defaultValue = "io.tapdata")
    private String packageName = "io.tapdata";

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

    @Schema(description = "Template library", example = "spring-cloud")
    private String templateLibrary = "spring-cloud";

    @Schema(description = "Module ids")
    private List<String> moduleIds;

    @Schema(description = "Whether to generate FeignClient file, true: only generate interface, false: generate interface and FeignClient file", example = "true")
    private Boolean interfaceOnly = true;
}
