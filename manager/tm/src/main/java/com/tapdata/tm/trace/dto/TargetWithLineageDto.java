package com.tapdata.tm.trace.dto;

import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.lineage.dto.TableLineageDto;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * @author <a href="2749984520@qq.com">Gavin'Xiao</a>
 * @author <a href="https://github.com/11000100111010101100111">Gavin'Xiao</a>
 * @version v1.0 2026/5/22 10:05 Create
 * @description
 */
@EqualsAndHashCode(callSuper = true)
public class TargetWithLineageDto extends TaskLineageBaseDto {
    /**
     * List of update condition fields for the target table
     */
    List<String> targetTableUpdateFields;

    public TargetWithLineageDto(Dag dag) {
        super(dag);
    }

    public List<String> getTargetTableUpdateFields() {
        return targetTableUpdateFields;
    }

    public void setTargetTableUpdateFields(List<String> targetTableUpdateFields) {
        this.targetTableUpdateFields = targetTableUpdateFields;
    }
}
