package com.tapdata.tm.openapi.generator.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.openapi.generator.dto.CodeGenerationRequest;
import com.tapdata.tm.openapi.generator.exception.CodeGenerationException;
import com.tapdata.tm.openapi.generator.service.OpenApiGeneratorService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;

/**
 * OpenAPI code generator Controller
 * Provides HTTP API for generating Java SDK code based on OpenAPI specifications
 * Currently only supports Java language
 *
 * @author tapdata
 * @date 2024/12/19
 */
@Tag(name = "OpenAPI Generator", description = "OpenAPI Java code generation related interfaces")
@RestController
@RequestMapping("/api/openapi/generator")
@Slf4j
public class OpenApiGeneratorController extends BaseController {

    @Autowired
    private OpenApiGeneratorService openApiGeneratorService;

    /**
     * Generate SDK code and download JAR package
     *
     * @param request Code generation request parameters
     * @return JAR file download response
     */
    @Operation(
        summary = "Generate Java SDK code",
        description = "Generate Java SDK code based on OpenAPI specification and download as JAR package (only supports Java language)"
    )
    @ApiResponses(value = {
        @ApiResponse(
            responseCode = "200",
            description = "Code generation successful, returns JAR file",
            content = @Content(
                mediaType = "application/octet-stream",
                schema = @Schema(type = "string", format = "binary")
            )
        ),
        @ApiResponse(
            responseCode = "400",
            description = "Invalid request parameters",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(implementation = ResponseMessage.class)
            )
        ),
        @ApiResponse(
            responseCode = "500",
            description = "Internal server error",
            content = @Content(
                mediaType = "application/json",
                schema = @Schema(implementation = ResponseMessage.class)
            )
        )
    })
    @PostMapping("/generate")
    public ResponseEntity<InputStreamResource> generateCode(
            @Valid @RequestBody CodeGenerationRequest request) {
        
        try {
            log.info("Received code generation request: oas={}, lan={}, packageName={}, artifactId={}, groupId={}",
                    request.getOas(), request.getLan(), request.getPackageName(),
                    request.getArtifactId(), request.getGroupId());

            return openApiGeneratorService.generateCode(request);

        } catch (CodeGenerationException e) {
            log.error("Code generation failed", e);
            throw e; // Re-throw custom exception
        } catch (Exception e) {
            log.error("Code generation failed", e);
            throw new CodeGenerationException("Code generation failed: " + e.getMessage(), e);
        }
    }

    /**
     * Generate code using GET method (simplified version)
     *
     * @param oas URL of OpenAPI specification file
     * @param lan Programming language for generation, defaults to java
     * @return JAR file download response
     */
    @Operation(
        summary = "Generate Java SDK code (GET method)",
        description = "Generate Java SDK code using GET request with parameters passed via URL (only supports Java language)"
    )
    @GetMapping("/generate")
    public ResponseEntity<InputStreamResource> generateCodeByGet(
            @Parameter(description = "URL of OpenAPI specification file", required = true, example = "https://example.com/api/openapi.json")
            @RequestParam String oas,

            @Parameter(description = "Programming language for generation", example = "java")
            @RequestParam(defaultValue = "java") String lan) {

        try {
            log.info("Received GET code generation request: oas={}, lan={}", oas, lan);

            CodeGenerationRequest request = new CodeGenerationRequest();
            request.setOas(oas);
            request.setLan(lan);
            // Use default values
            request.setPackageName("io.tapdata.sdk");
            request.setArtifactId("tapdata-sdk");
            request.setGroupId("io.tapdata");

            return openApiGeneratorService.generateCode(request);

        } catch (CodeGenerationException e) {
            log.error("Code generation failed", e);
            throw e; // Re-throw custom exception
        } catch (Exception e) {
            log.error("Code generation failed", e);
            throw new CodeGenerationException("Code generation failed: " + e.getMessage(), e);
        }
    }

    /**
     * Get list of supported programming languages
     *
     * @return List of supported languages
     */
    @Operation(summary = "Get supported programming languages", description = "Returns list of programming languages supported by OpenAPI Generator")
    @GetMapping("/languages")
    public ResponseMessage<String[]> getSupportedLanguages() {
        // Currently only Java language is supported
        String[] languages = {"java"};
        return success(languages);
    }

    /**
     * Health check endpoint
     *
     * @return Service status
     */
    @Operation(summary = "Health check", description = "Check if OpenAPI Generator service is running normally")
    @GetMapping("/health")
    public ResponseMessage<String> health() {
        return success("OpenAPI Generator service is running normally");
    }
}
