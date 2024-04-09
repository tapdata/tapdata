package com.tapdata.tm.agent.dto;

import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * @author Gavin
 *
 * */
@EqualsAndHashCode(callSuper = true)
@Data
public class GroupDto extends BaseDto {
    String name;
    String groupId;

    List<String> agentIds;

    Boolean hasDelete;
}
