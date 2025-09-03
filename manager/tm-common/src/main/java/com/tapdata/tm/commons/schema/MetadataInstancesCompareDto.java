package com.tapdata.tm.commons.schema;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.*;

import java.util.Date;
import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MetadataInstancesCompareDto extends BaseDto {
    public static final String TYPE_COMPARE = "compare";
    public static final String TYPE_APPLY = "apply";
    public static final String TYPE_STATUS = "status";
    public static final String STATUS_RUNNING = "running";
    public static final String STATUS_DONE = "done";
    public static final String STATUS_ERROR = "error";
    public static final String STATUS_TIMEOUT = "timeOut";
    private String qualifiedName;
    private String tableName;
    private String nodeId;
    private String taskId;
    private List<DifferenceField> differenceFieldList;
    /**
     * compare: 差异结果
     * apply: 应用配置
     */
    private String type;
    private String status;
    private Date targetSchemaLoadTime;

    public static MetadataInstancesCompareDto createMetadataInstancesCompareDtoStatus(String nodeId) {
        return MetadataInstancesCompareDto.builder()
                .nodeId(nodeId)
                .status(STATUS_RUNNING)
                .type(TYPE_STATUS)
                .build();
    }

    public static MetadataInstancesCompareDto createMetadataInstancesCompareDtoCompare(String taskId,String nodeId, String tableName, String qualifiedName, List<DifferenceField> differenceFieldList) {
        return MetadataInstancesCompareDto.builder()
                .nodeId(nodeId)
                .tableName(tableName)
                .qualifiedName(qualifiedName)
                .differenceFieldList(differenceFieldList)
                .taskId(taskId)
                .type(TYPE_COMPARE)
                .build();
    }
    public static MetadataInstancesCompareDto createMetadataInstancesCompareDtoApply(String nodeId, String tableName, String qualifiedName, List<DifferenceField> differenceFieldList) {
        return MetadataInstancesCompareDto.builder()
                .nodeId(nodeId)
                .tableName(tableName)
                .qualifiedName(qualifiedName)
                .differenceFieldList(differenceFieldList)
                .type(TYPE_APPLY)
                .build();
    }
}
