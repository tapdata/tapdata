package com.tapdata.tm.sdkModule.dto;

import com.tapdata.tm.module.dto.ModulesDto;
import lombok.Data;
import lombok.EqualsAndHashCode;
import io.swagger.v3.oas.annotations.media.Schema;


/**
 * sdkModule
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class SdkModuleDto extends ModulesDto {
    @Schema(description = "SDK ID reference")
    private String sdkId;

    @Schema(description = "SDK Version ID reference")
    private String sdkVersionId;
}