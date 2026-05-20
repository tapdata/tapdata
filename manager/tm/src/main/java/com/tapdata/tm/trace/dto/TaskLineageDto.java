package com.tapdata.tm.trace.dto;

import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.lineage.dto.TableLineageDto;
import lombok.EqualsAndHashCode;

import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/20 16:30 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
public class TaskLineageDto extends TableLineageDto {
    /**
     * <nodeId, <filedName, originName>>
     * */
    Map<String, Map<String, String>> fieldNameMapping;

    public TaskLineageDto(Dag dag) {
        super(dag);
    }

    public Map<String, Map<String, String>> getFieldNameMapping() {
        return fieldNameMapping;
    }

    public void setFieldNameMapping(Map<String, Map<String, String>> fieldNameMapping) {
        this.fieldNameMapping = fieldNameMapping;
    }
}
