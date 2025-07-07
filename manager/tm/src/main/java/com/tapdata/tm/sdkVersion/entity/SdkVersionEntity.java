package com.tapdata.tm.sdkVersion.entity;

import com.tapdata.tm.sdk.service.GenerateStatus;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;


/**
 * sdkVersion
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("SdkVersion")
public class SdkVersionEntity extends BaseEntity {
    @Schema(description = "Version number", example = "1.0.0")
    private String version;

    @Schema(description = "SDK ID reference")
    private String sdkId;

    @Schema(description = "Last generate status")
    private GenerateStatus generateStatus;

    @Schema(description = "Generation error message")
    private String generationErrorMessage;

    @Schema(description = "ZIP file GridFS ID")
    private String zipGridfsId;

    @Schema(description = "ZIP file size in bytes")
    private Long zipSizeOfByte;

    @Schema(description = "JAR file GridFS ID")
    private String jarGridfsId;

    @Schema(description = "JAR file size in bytes")
    private Long jarSizeOfByte;

    @Schema(description = "JAR generation error message")
    private String jarGenerationErrorMessage;

    private List<String> moduleIds;
    private String clientId;
}