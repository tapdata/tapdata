package com.tapdata.tm.agent.dto;

import com.google.common.collect.Lists;
import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Gavin
 *
 * */
@EqualsAndHashCode(callSuper = true)
@Data
public class AgentToGroupDto extends BaseDto {
    List<String> agentId;
    List<String> groupId;

    public List<AgentWithGroupBaseDto> groupAgent() {
        ArrayList<AgentWithGroupBaseDto> value = Lists.newArrayList();
        if (null == agentId || agentId.isEmpty()) return value;
        if (null == groupId || groupId.isEmpty()) return value;

        for (String agent : agentId) {
            for (String group : groupId) {
                AgentWithGroupBaseDto dto = new AgentWithGroupBaseDto();
                dto.setAgentId(agent);
                dto.setGroupId(group);
                value.add(dto);
            }
        }
        return value;
    }
}
