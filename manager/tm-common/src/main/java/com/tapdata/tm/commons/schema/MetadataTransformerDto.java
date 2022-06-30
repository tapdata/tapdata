package com.tapdata.tm.commons.schema;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.*;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;


/**
 * MetadataTransformer
 */
@EqualsAndHashCode(callSuper = true)
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MetadataTransformerDto extends BaseDto {
    private String dataFlowId;
    private String stageId;
    private long beginTimestamp;
    private int finished;
    private int hashCode;
    private long pingTime;
    private String sinkDbType;
    private String status;
    private Map<String, Boolean> tableName;
    private int total;
    private String version;
    private long usedTimestamp;

    public enum StatusEnum {
        running,
        done,
        error,
    }

}