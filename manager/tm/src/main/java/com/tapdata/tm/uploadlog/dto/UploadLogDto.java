package com.tapdata.tm.uploadlog.dto;

import lombok.Data;

@Data
public class UploadLogDto {
    private String agentId;
    private String uploadAddr;
    private String logBucket;
    private String accessKeySecret;
    private String accessKeyId;
    private String securityToken;
    private String ossRegion;
    private String tmInfoEngineId;
    private String userId;
    private String id;
}
