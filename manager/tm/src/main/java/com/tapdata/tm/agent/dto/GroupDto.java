package com.tapdata.tm.agent.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tapdata.tm.base.exception.BizException;
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
    @JsonProperty("group_id")
    String groupId;

    @JsonProperty("agent_ids")
    List<String> agentIds;

    @JsonProperty("is_delete")
    Boolean hasDelete;
}
