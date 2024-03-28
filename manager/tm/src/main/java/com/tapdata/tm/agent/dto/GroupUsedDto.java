package com.tapdata.tm.agent.dto;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;


/**
 * @author Gavin
 *
 * */
@EqualsAndHashCode(callSuper = true)
@Data
public class GroupUsedDto extends GroupDto {
    List<DataSourceConnectionDto> usedInConnection;
    List<TaskDto> usedInTask;

    Boolean deleted;
    String deleteMsg;
}
