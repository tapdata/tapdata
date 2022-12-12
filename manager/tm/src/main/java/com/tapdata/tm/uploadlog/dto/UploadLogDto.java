package com.tapdata.tm.uploadlog.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class UploadLogDto extends BaseDto {
    private String agentId;
    private String uploadAddr;
    private String logBucket;
    private String accessKeySecret;
    private String accessKeyId;
    private String ossRegion;
    private String tmInfoEngineId;

}
