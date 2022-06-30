package com.tapdata.tm.dataflowsdebug.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.springframework.data.mongodb.core.mapping.Document;


/**
 * DataFlowsDebug
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("DataFlowsDebug")
public class DataFlowsDebugEntity extends BaseEntity {
    private String name;

}