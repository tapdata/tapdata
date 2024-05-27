package com.tapdata.tm.featurecheck.dto;

import lombok.Data;

import java.util.List;

@Data
public class FeatureCheckResult {
    private List<AgentDto> agents;

    private List<String> eligibleAgents; // 符合所有规则的实例ID列表

    private List<FeatureCheckDto> result;
}
