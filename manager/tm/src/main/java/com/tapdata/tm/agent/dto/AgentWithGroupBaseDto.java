package com.tapdata.tm.agent.dto;

import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.base.dto.BaseDto;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * @author Gavin
 *
 * */
@EqualsAndHashCode(callSuper = true)
@Data
public class AgentWithGroupBaseDto extends BaseDto {
    String agentId;
    String groupId;

    public void verify() {
        if (null == groupId || "".equals(groupId.trim())){
            // 标签ID不能为空
            throw new BizException("");
        }
        if (null == agentId || "".equals(agentId.trim())){
            // Agent ID不能为空
            throw new BizException("");
        }
    }
}
