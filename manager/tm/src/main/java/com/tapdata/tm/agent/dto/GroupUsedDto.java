package com.tapdata.tm.agent.dto;

import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
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
    List<Object> usedInTask;

    Boolean deleted;
    String deleteMsg;
}
