package com.tapdata.tm.metadatainstance.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MigrateTableInfoDto {
    private String taskId;
    private String tableName;
    @Schema(description = "当前节点的nodeId")
    private String nodeId;
    private List<Field> fields;

    @NoArgsConstructor
    @Data
    public static
    class Field {
        private String fieldName;
        private String fieldType;
        private String defaultValue;
        @Schema(description = "true 启用默认值，false 不启用默认值")
        private boolean useDefaultValue = true;
    }
}
