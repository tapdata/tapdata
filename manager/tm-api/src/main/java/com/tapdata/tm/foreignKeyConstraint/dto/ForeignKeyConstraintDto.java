package com.tapdata.tm.foreignKeyConstraint.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(callSuper = true)
@Data
public class ForeignKeyConstraintDto extends BaseDto {
    private String taskId;
    private List<String> sqlList;
}
