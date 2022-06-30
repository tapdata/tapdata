package com.tapdata.tm.dataflowinsight.entity;

import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * DataFlowInsight
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("DataFlowInsight")
public class DataFlowInsightEntity extends BaseEntity {

    private String statsType;

    private String dataFlowId;

    private String statsTime;

    private String granularity;

    private Map<String, Object> statsData;

}