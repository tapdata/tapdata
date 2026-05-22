package com.tapdata.tm.trace.dto;

import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.lineage.dto.TableLineageDto;
import lombok.EqualsAndHashCode;

import java.util.Map;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/22 15:34 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
public class TaskLineageBaseDto extends TableLineageDto {
    /**
     * <nodeId, <targetTraceFieldName, currentTableFieldName>>
     * */
    Map<String, Map<String, String>> traceFilterFieldNameMapping;

    public TaskLineageBaseDto(Dag dag) {
        super(dag);
    }

    public Map<String, Map<String, String>> getTraceFilterFieldNameMapping() {
        return traceFilterFieldNameMapping;
    }

    public void setTraceFilterFieldNameMapping(Map<String, Map<String, String>> traceFilterFieldNameMapping) {
        this.traceFilterFieldNameMapping = traceFilterFieldNameMapping;
    }
}
