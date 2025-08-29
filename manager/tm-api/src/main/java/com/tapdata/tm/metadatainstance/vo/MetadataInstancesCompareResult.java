package com.tapdata.tm.metadatainstance.vo;

import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.commons.schema.MetadataInstancesCompareDto;
import lombok.Data;

import java.util.Date;
import java.util.List;


@Data
public class MetadataInstancesCompareResult {
    private Page<MetadataInstancesCompareDto> compareDtos;
    private List<MetadataInstancesCompareDto> invalidApplyDtos;
    private String status;
    private Date finishTime;
}
