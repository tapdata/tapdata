package com.tapdata.tm.vo;

import io.tapdata.entity.schema.TapField;
import io.tapdata.entity.schema.TapTable;
import lombok.Data;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

/**
 * 任务节点-表和字段溯源
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/3/27 18:51 Create
 */
@Data
public class TaskNodeTableFieldTraceVo {
    private String sourceTable;
    private List<String> sourceFields;
    private String targetTable;
    private LinkedHashMap<String, String> fieldMap;
    private List<String> targetFields;

    public static TaskNodeTableFieldTraceVo ofTargetTable(TapTable targetTable, Set<String> sourceFields) {
        TaskNodeTableFieldTraceVo ins = new TaskNodeTableFieldTraceVo();
        ins.setSourceTable(targetTable.getAncestorsName());
        ins.setTargetTable(targetTable.getName());
        ins.setFieldMap(new LinkedHashMap<>());
        ins.setSourceFields(new ArrayList<>());
        ins.setTargetFields(new ArrayList<>());
        LinkedHashMap<String, TapField> fieldMap = targetTable.getNameFieldMap();
        if (null != fieldMap) {
            for (TapField field : fieldMap.values()) {
                String sourceFieldName = field.getOriginalFieldName();
                if (sourceFields.contains(sourceFieldName)) {
                    ins.getSourceFields().add(sourceFieldName);
                    ins.getTargetFields().add(field.getName());
                    ins.getFieldMap().put(sourceFieldName, field.getName());
                }
            }
        }
        if (ins.getSourceFields().isEmpty()) {
            return null;
        }
        return ins;
    }
}
