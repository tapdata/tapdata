package com.tapdata.tm.commons.dag;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: Zed
 * @Date: 2022/3/7
 * @Description:
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class FieldsMapping {
    private String targetFieldName;
    private String sourceFieldName;
    private String sourceFieldType;
    private String type;
    private String defaultValue;
    private boolean useDefaultValue = true;

    private Boolean isShow;
    private String migrateType;

    private int primary_key_position;
}
