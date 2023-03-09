package com.tapdata.tm.shareCdcTableMetrics.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import lombok.NonNull;
import org.springframework.data.mongodb.core.mapping.Document;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * ShareCdcTableMetrics
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("ShareCdcTableMetrics")
public class ShareCdcTableMetricsEntity extends BaseEntity {
    private String taskId;
    private String nodeId;
    private String connectionId;
    private Long startCdcTime;
    private Long firstEventTime;
    private Long currentEventTime;
    private Long count;
    private Long allCount;
}