package com.tapdata.tm.featurecheck.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;

import java.util.List;

@Data
public class FeatureCheckDto extends BaseDto {
    private String featureType;

    private String featureCode;

    private String minAgentVersion;

    private String description;

    private List<String> supportedAgents;
}
