package com.tapdata.tm.trace.dto;

import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.lineage.dto.TableLineageDto;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/22 15:34 Create
 * @description
 */
@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class TaskLineageBaseDto extends TableLineageDto {
    /**
     * <nodeId, <currentTableFieldName, sourceFieldName>>
     */
    Map<String, Map<String, String>> traceFilterFieldNameMapping;

    public TaskLineageBaseDto(Dag dag) {
        super(dag);
    }
}
