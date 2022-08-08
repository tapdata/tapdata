package com.tapdata.tm.metadatainstance.dto;

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
        private String defaultValue;
    }
}
