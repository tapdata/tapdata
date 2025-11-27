package com.tapdata.tm.sdkVersion.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.sdk.service.GenerateStatus;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;


/**
 * sdkVersion
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class SdkVersionDto extends BaseDto {

	@Schema(description = "Version", example = "1.0.0", defaultValue = "1.0.0")
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