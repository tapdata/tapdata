package com.tapdata.tm.metadatadefinition.dto;

import com.tapdata.tm.modules.dto.ModulesDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class ApiAppDetail extends MetadataDefinitionDto {
    private List<ModulesDto> apis;
}
