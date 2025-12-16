package com.tapdata.tm.sdk.entity;

import com.tapdata.tm.base.entity.BaseEntity;
import com.tapdata.tm.sdk.service.GenerateStatus;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Date;
import java.util.List;


/**
 * SDK Manager
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("Sdk")
public class SDKEntity extends BaseEntity {
	@NotBlank(message = "OpenAPI specification URL cannot be empty")
	@Schema(description = "URL of the OpenAPI specification file", example = "https://example.com/api/openapi.json", requiredMode = Schema.RequiredMode.REQUIRED)
	private String oas;

	@Schema(description = "Programming language for code generation (only 'spring' is supported)", example = "spring", defaultValue = "spring")
	private String lan = "spring";

	@Schema(description = "Package name for generated code", example = "io.tapdata", defaultValue = "io.tapdata.sdk")
	private String packageName = "io.tapdata";

	@Schema(description = "Artifact ID", example = "tapdata-sdk", defaultValue = "tapdata-sdk")
	private String artifactId = "tapdata-sdk";

	@Schema(description = "Group ID", example = "io.tapdata", defaultValue = "io.tapdata")
	private String groupId = "io.tapdata";

	private String lastClientId;

	@NotBlank(message = "Request address cannot be empty")
	@Schema(description = "Request address", example = "http://127.0.0.1:3030")
	private String requestAddress;

	@Schema(description = "Template library", example = "spring-cloud")
	private String templateLibrary = "spring-cloud";

	@Schema(description = "Last generated version")
	private String lastGeneratedVersion;

	@Schema(description = "Last generation time")
	private Date lastGenerationTime;

	@Schema(description = "Generation error message")
	private String generationErrorMessage;

	@Schema(description = "Last generate status")
	private GenerateStatus lastGenerateStatus;

	@Schema(description = "Last generated ZIP file GridFS ID")
	private String lastZipGridfsId;

	@Schema(description = "Last generated ZIP file size in bytes")
	private Long lastZipSizeOfByte;

	@Schema(description = "Last generated JAR file GridFS ID")
	private String lastJarGridfsId;

	@Schema(description = "Last generated JAR file size in bytes")
	private Long lastJarSizeOfByte;

	@Schema(description = "Last JAR generation error message")
	private String lastJarGenerationErrorMessage;

	private List<String> lastModuleIds;
}