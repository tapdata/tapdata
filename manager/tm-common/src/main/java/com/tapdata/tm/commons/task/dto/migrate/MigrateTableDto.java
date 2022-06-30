package com.tapdata.tm.commons.task.dto.migrate;

import com.tapdata.tm.commons.schema.Field;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class MigrateTableDto {
    private String databaseType;
    private String originalName;
    private List<Field> fieldList;
}
