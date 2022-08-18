package com.tapdata.tm.metadatainstance.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class MigrateTableInfoDto {
    private String taskId;
    private String tableName;
    private List<Field> fields;

    @NoArgsConstructor
    @Getter
    @Setter
    public static
    class Field {
        private String fieldName;
        private String fieldType;
//        private String defaultValue;
        @Schema(description = "true 启用默认值，false 不启用默认值")
        private boolean useDefaultValue = true;
    }
}
