package com.tapdata.tm.commons.dag;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: Zed
 * @Date: 2022/3/7
 * @Description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FieldsMapping {
    private String targetFieldName;
    private String sourceFieldName;
    private String sourceFieldType;
    private String type;
}
