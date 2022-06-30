package com.tapdata.tm.monitor.entity;

import lombok.Data;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.Map;

/**
 * 静态统计表
 */
@Document("AgentEnvironment")
@Data
public class AgentEnvironmentEntity {
    private String id;
    private Map<String, String> tags;
    private Map ss;
}
