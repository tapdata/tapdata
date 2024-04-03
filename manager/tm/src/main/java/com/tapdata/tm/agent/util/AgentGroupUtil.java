package com.tapdata.tm.agent.util;

import com.tapdata.tm.agent.dto.GroupDto;
import com.tapdata.tm.agent.entity.AgentGroupEntity;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.cluster.dto.AccessNodeInfo;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class AgentGroupUtil {

    public Filter initFilter(Filter filter) {
        if (null == filter) {
            filter = new Filter();
            filter.setWhere(Where.where(AgentGroupTag.TAG_DELETE, false));
        }
        return filter;
    }

    //标签按引擎数降序
    public int sortAgentGroup(AgentGroupEntity a, AgentGroupEntity b) {
        List<String> agentIdsOfA = a.getAgentIds();
        List<String> agentIdsOfB = b.getAgentIds();
        if (null == agentIdsOfA && null == agentIdsOfB) {
            return 0;
        }
        if (null == agentIdsOfA) {
            return 1;
        }
        if (null == agentIdsOfB) {
            return -1;
        }
        return agentIdsOfB.size() - agentIdsOfA.size();
    }

    public AccessNodeInfo mappingAccessNodeInfo(AgentGroupEntity group, Map<String, AccessNodeInfo> infoMap) {
        List<String> agentIds = group.getAgentIds();
        List<AccessNodeInfo> agentInfos = null == agentIds ?
                new ArrayList<>() : agentIds.stream().map(infoMap::get)
                .collect(Collectors.toList());
        AccessNodeInfo item = new AccessNodeInfo();
        item.setProcessId(group.getGroupId());
        item.setAccessNodeName(group.getName());
        item.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
        item.setAccessNodes(agentInfos);
        return item;
    }


    public void verifyUpdateGroupInfo(GroupDto dto) {
        if (null == dto){
            // Response Body不能为空
            throw new BizException("group.response.body.failed");
        }
        String groupId = dto.getGroupId();
        if (null == groupId || "".equals(groupId.trim())){
            // Group ID不能为空
            throw new BizException("group.id.empty");
        }
        String name = dto.getName();
        if (null == name || "".equals(name.trim())){
            // New Group Name不能为空
            throw new BizException("group.new.name.empty");
        }
        if (name.length() > AgentGroupTag.MAX_AGENT_GROUP_NAME_LENGTH) {
            // New Group Name长度不能大于15
            throw new BizException("group.new.name.too.long", AgentGroupTag.MAX_AGENT_GROUP_NAME_LENGTH);
        }
    }
}
