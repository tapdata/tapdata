package com.tapdata.tm.transform.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import com.tapdata.tm.base.entity.BaseEntity;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.data.mongodb.core.mapping.Document;

import java.util.List;
import java.util.Map;


/**
 * MetadataTransformer
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Document("MetadataTransformer")
public class MetadataTransformerEntity extends BaseEntity {
    private String name;
    private String dataFlowId;
    private String stageId;
    private int beginTimestamp;
    private int finished;
    private int hashCode;
    private long pingTime;
    private String sinkDbType;
    private String status;
    private List<Map<String, Boolean>> tableName;
    private int total;
    private String version;
    private long usedTimestamp;

}