package com.tapdata.tm.openapi.generator.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;

/**
 * OpenAPI code generation response DTO
 *
 * @author tapdata
 * @date 2024/12/19
 */
@Data
@Schema(description = "OpenAPI code generation response")
public class CodeGenerationResponse {

    @Schema(description = "Generation status", example = "success")
    private String status;

    @Schema(description = "Response message", example = "Code generation successful")
    private String message;

    @Schema(description = "Generated file name", example = "tapdata-sdk-java.zip")
    private String fileName;

    @Schema(description = "File size in bytes", example = "1024000")
    private Long fileSize;

    public static CodeGenerationResponse success(String fileName, Long fileSize) {
        CodeGenerationResponse response = new CodeGenerationResponse();
        response.setStatus("success");
        response.setMessage("Code generation successful");
        response.setFileName(fileName);
        response.setFileSize(fileSize);
        return response;
    }

    public static CodeGenerationResponse error(String message) {
        CodeGenerationResponse response = new CodeGenerationResponse();
        response.setStatus("error");
        response.setMessage(message);
        return response;
    }
}
