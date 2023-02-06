package com.tapdata.tm.metadatadefinition.vo;

import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class MetaDataDefinitionVo extends MetadataDefinitionDto {
    private String userName;
}
