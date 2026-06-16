package com.tapdata.tm.trace.dto;

import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.trace.dto.boodline.FieldNameMapping;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/20 16:30 Create
 * @description
 */
@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class TaskLineageDto extends TaskLineageBaseDto {
    /**
     * <nodeId, <filedName, originName>>
     */
    Map<String, Map<String, String>> fieldNameMapping;

    /**
     * <nodeId, List<BloodlineFinder.FieldNameMapping>>
     */
    Map<String, List<FieldNameMapping>> updateConditionFieldList;

    public TaskLineageDto(Dag dag) {
        super(dag);
    }
}
