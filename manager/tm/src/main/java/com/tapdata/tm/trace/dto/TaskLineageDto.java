package com.tapdata.tm.trace.dto;

import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.lineage.dto.TableLineageDto;
import com.tapdata.tm.trace.service.bloodline.BloodlineFinder;
import lombok.EqualsAndHashCode;

import java.util.List;
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

    /**
     * <nodeId, List<BloodlineFinder.FieldNameMapping>>
     * */
    Map<String, List<BloodlineFinder.FieldNameMapping>> updateConditionFieldList;

    public TaskLineageDto(Dag dag) {
        super(dag);
    }

    public Map<String, Map<String, String>> getFieldNameMapping() {
        return fieldNameMapping;
    }

    public void setFieldNameMapping(Map<String, Map<String, String>> fieldNameMapping) {
        this.fieldNameMapping = fieldNameMapping;
    }

    public Map<String, List<BloodlineFinder.FieldNameMapping>> getUpdateConditionFieldList() {
        return updateConditionFieldList;
    }

    public void setUpdateConditionFieldList(Map<String, List<BloodlineFinder.FieldNameMapping>> updateConditionFieldList) {
        this.updateConditionFieldList = updateConditionFieldList;
    }
}
