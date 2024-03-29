package com.tapdata.tm.agent.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.service.SettingsServiceImpl;
import com.tapdata.tm.agent.dto.AgentGroupDto;
import com.tapdata.tm.agent.dto.AgentRemoveFromGroupDto;
import com.tapdata.tm.agent.dto.AgentToGroupDto;
import com.tapdata.tm.agent.dto.GroupDto;
import com.tapdata.tm.agent.dto.GroupUsedDto;
import com.tapdata.tm.agent.entity.AgentGroupEntity;
import com.tapdata.tm.agent.repository.AgentGroupRepository;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.cluster.dto.AccessNodeInfo;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.task.service.TaskServiceImpl;
import com.tapdata.tm.user.service.UserServiceImpl;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.service.WorkerServiceImpl;
import io.tapdata.utils.AppType;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @Author: Gavin
 * @Date: 2021/10/15
 * @Description:
 */
@Service
@Slf4j
public class AgentGroupService extends BaseService<GroupDto, AgentGroupEntity, ObjectId, AgentGroupRepository> {

    @Autowired
    private WorkerServiceImpl workerServiceImpl;
    @Autowired
    private DataSourceService dataSourceService;
    @Autowired
    private TaskServiceImpl taskService;
    @Autowired
    private SettingsServiceImpl settingsService;
    @Autowired
    private UserServiceImpl userService;


    public AgentGroupService(@NonNull AgentGroupRepository repository) {
        super(repository, GroupDto.class, AgentGroupEntity.class);
    }

    @Override
    protected void beforeSave(GroupDto dto, UserDetail userDetail) {

    }

    public Page<AgentGroupDto> groupAllAgent(Filter filter, Boolean containWorker, UserDetail userDetail) {
        if (null == filter) {
            filter = new Filter();
            filter.setWhere(Where.where("is_delete", false));
        }
        Page<GroupDto> groupDtoPage = find(filter, userDetail);
        List<GroupDto> items = groupDtoPage.getItems();
        if (CollectionUtils.isEmpty(items)) {
            return new Page<>(groupDtoPage.getTotal(), new ArrayList<>());
        }
        Set<String> allAgentId = items.stream()
                .map(GroupDto::getAgentIds)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        final boolean equals = Boolean.TRUE.equals(containWorker);
        List<WorkerDto> all = null;
        if (equals) {
            all = findAllAgent(allAgentId, userDetail);
        }
        Map<String, WorkerDto> map = equals ? all.stream().collect(Collectors.toMap(WorkerDto::getProcessId, w -> w)) : null;
        return new Page<>(groupDtoPage.getTotal(), items.stream()
                .map(item -> {
                    List<String> agentIds = item.getAgentIds();
                    AgentGroupDto dto = new AgentGroupDto();
                    if (equals) {
                        List<WorkerDto> collect = agentIds.stream()
                                .map(map::get)
                                .collect(Collectors.toList());
                        dto.setAgents(collect);
                    }
                    dto.setGroupId(item.getGroupId());
                    dto.setName(item.getName());
                    dto.setAgentIds(agentIds);
                    return dto;
                }).collect(Collectors.toList()));
    }

    public AgentGroupDto createGroup(GroupDto groupDto, UserDetail userDetail) {
        final String name = groupDto.getName();
        ObjectId id = new ObjectId();
        AgentGroupDto dto = new AgentGroupDto();
        dto.setGroupId(id.toHexString());
        dto.setName(name);
        Query query = verifyCountGroupByName(name, userDetail);
        long succeedCount = upsert(query, dto, userDetail);
        if (succeedCount > 0) {
            return dto;
        }
        return dto;
    }

    protected Query verifyCountGroupByName(String name, UserDetail userDetail) {
        Query query = Query.query(Criteria.where("name").is(name).and("is_delete").is(false));
        if (count(query, userDetail) > 0) {
            //Group Name重复
            throw new BizException("group.repeat");
        }
        return query;
    }


    public AgentGroupDto addAgentToGroup(AgentToGroupDto agentDto, UserDetail loginUser) {
        agentDto.verify();
        final String groupId = agentDto.getGroupId();
        final String agentId = agentDto.getAgentId();
        List<String> ats = new ArrayList<>();
        ats.add(agentId);
        List<WorkerDto> allAgent = findAllAgent(ats, loginUser);
        if (null == allAgent || allAgent.isEmpty()) {
            throw new BizException("group.agent.not.fund", agentId);
        }
        GroupDto groupDto = findGroupById(groupId, loginUser);
        List<String> agents = groupDto.getAgentIds();
        if (!(null == agents || agents.isEmpty() || !agents.contains(agentId))) {
            //引擎不能重复添加标签
            throw new BizException("group.agent.repeatedly");
        }
        List<String> agentIds = new ArrayList<>();
        agentIds.add(agentId);
        UpdateResult updateResult = update(
                Query.query(Criteria.where("groupId").is(groupId)
                        .and("is_delete").is(false)
                        .and("agentIds").nin(agentIds)),
                new Update().push("agentIds", agentId), loginUser);
        long modifiedCount = updateResult.getModifiedCount();
        if (modifiedCount <= 0) {
            //添加失败
            throw new BizException("group.agent.add.failed");
        }
        return findAgentGroupInfo(groupId, loginUser);
    }

    public AgentGroupDto removeAgentFromGroup(AgentRemoveFromGroupDto removeDto, UserDetail loginUser) {
        removeDto.verify();
        final String groupId = removeDto.getGroupId();
        final String agentId = removeDto.getAgentId();
        GroupDto groupDto = findGroupById(groupId, loginUser);
        List<String> agentIds = new ArrayList<>();
        agentIds.add(agentId);
        UpdateResult updateResult = update(Query.query(Criteria
                .where("groupId").is(groupId)
                .and("is_delete").is(false)
                .and("agentIds").in(agentIds)), new Update().pull("agentIds", agentId), loginUser);
        long modifiedCount = updateResult.getModifiedCount();
        if (modifiedCount <= 0) {
            //移除失败
            throw new BizException("group.agent.remove.failed");
        }
        return findAgentGroupInfo(groupId, loginUser);
    }


    public GroupUsedDto deleteGroup(String groupId, UserDetail loginUser) {
        GroupDto groupDto = findGroupById(groupId, loginUser);
        List<String> groupIds = new ArrayList<>();
        groupIds.add(groupId);
        //查询正在使用当前标签的数据源
        GroupUsedDto result = new GroupUsedDto();
        Query connectionQuery = Query.query(
                Criteria.where("accessNodeType").is(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP)
                .and("accessNodeProcessIdList").in(groupIds)
                .and("is_delete").is(false));

        List<DataSourceConnectionDto> beUsedConnections = dataSourceService.findAllDto(connectionQuery, loginUser);
        result.setUsedInConnection(beUsedConnections);
        //查询正在使用当前标签的任务
        List<TaskDto> serviceAllDto = taskService.findAllDto(connectionQuery, loginUser);
        result.setUsedInTask(serviceAllDto);

        result.setGroupId(groupId);
        result.setName(groupDto.getName());
        result.setAgentIds(groupDto.getAgentIds());

        if (beUsedConnections.isEmpty() && serviceAllDto.isEmpty()) {
            boolean deleted = deleteById(groupDto.getId(), loginUser);
            result.setDeleted(deleted);
            result.setDeleteMsg(deleted ? "Delete succeed" : "Delete failed, please try again");
        } else {
            result.setDeleted(false);
            result.setDeleteMsg("The current agent tag has been used by some data source connections or tasks and cannot be deleted");
        }
        return result;
    }

    public AgentGroupDto updateBaseInfo(GroupDto dto, UserDetail loginUser) {
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
        if (name.length() > 15) {
            // New Group Name长度不能大于15
            throw new BizException("group.new.name.too.long", 15);
        }
        verifyCountGroupByName(name, loginUser);
        update(Query.query(Criteria
                .where("groupId").is(groupId)
                .and("is_delete").is(false)),
                new Update().set("name", name), loginUser);
        return findAgentGroupInfo(dto.getGroupId(), loginUser);
    }

    protected GroupDto findGroupById(String groupId, UserDetail loginUser) {
        Criteria criteria = Criteria.where("groupId").is(groupId).and("is_delete").is(false);
        GroupDto groupDto = findOne(Query.query(criteria), loginUser);
        if (null == groupDto) {
            //找不到当前标签
            throw new BizException("group.not.fund", groupId);
        }
        return groupDto;
    }

    protected AgentGroupDto findAgentGroupInfo(String groupId, UserDetail loginUser) {
        Filter filter = new Filter();
        filter.setWhere(Where.where("groupId", groupId).and("is_delete", false));
        return findAgentGroupInfo(filter, loginUser);
    }

    public AgentGroupDto findAgentGroupInfo(Filter filter, UserDetail loginUser) {
        GroupDto groupDto = findOne(filter, loginUser);
        List<String> agentIds = groupDto.getAgentIds();
        List<WorkerDto> all = findAllAgent(agentIds, loginUser);
        AgentGroupDto dto = new AgentGroupDto();
        dto.setAgentIds(agentIds);
        dto.setName(groupDto.getName());
        dto.setGroupId(groupDto.getGroupId());
        dto.setAgents(all);
        return dto;
    }

    protected List<WorkerDto> findAllAgent(Collection<String> agentIds, UserDetail loginUser) {
        Criteria criteria = Criteria.where("process_id").in(agentIds)
                .and("worker_type").is("connector");
        return workerServiceImpl.findAllDto(Query.query(criteria), loginUser);
    }


    public List<AccessNodeInfo> filterGroupList(List<AccessNodeInfo> info, UserDetail loginUser) {
        if (settingsService.isCloud()) {
            return info;
        }
        if (null == info || info.isEmpty()) return info;
        Map<String, AccessNodeInfo> infoMap = info.stream().collect(Collectors.toMap(AccessNodeInfo::getProcessId, a -> a));
        List<AgentGroupEntity> entities = findAll(Query.query(Criteria.where("is_delete").is(false)), loginUser);
        List<AccessNodeInfo> groupAgentList = entities.stream().map(group -> {
            List<String> agentIds = group.getAgentIds();
            List<AccessNodeInfo> agentInfos = agentIds.stream()
                    .map(infoMap::get)
                    .collect(Collectors.toList());
            AccessNodeInfo item = new AccessNodeInfo();
            item.setProcessId(group.getGroupId());
            item.setAccessNodeName(group.getName());
            item.setAccessNodeType(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name());
            item.setAccessNodes(agentInfos);
            return item;
        }).collect(Collectors.toList());
        groupAgentList.addAll(info);
        return groupAgentList;
    }

    public List<String> getProcessNodeListWithGroup(TaskDto taskDto, UserDetail userDetail) {
        List<String> processNodeList = getProcessNodeList(taskDto.getAccessNodeType(), taskDto.getAccessNodeProcessId(), userDetail);
        if (null == processNodeList) {
            return taskDto.getAccessNodeProcessIdList();
        }
        return processNodeList;
    }

    public List<String> getProcessNodeListWithGroup(DataSourceConnectionDto connectionDto, UserDetail userDetail) {
        List<String> processNodeList = getProcessNodeList(connectionDto.getAccessNodeType(), connectionDto.getAccessNodeProcessId(), userDetail);
        if (null == processNodeList) {
            return connectionDto.getAccessNodeProcessIdList();
        }
        return processNodeList;
    }
    public List<String> getTrueProcessNodeListWithGroup(DataSourceConnectionDto connectionDto, UserDetail userDetail) {
        List<String> processNodeList = getProcessNodeList(connectionDto.getAccessNodeType(), connectionDto.getAccessNodeProcessId(), userDetail);
        if (null == processNodeList) {
            return connectionDto.getTrueAccessNodeProcessIdList();
        }
        return processNodeList;
    }

    protected List<String> getProcessNodeList(String accessNodeType, String accessNodeGroupProcessId, UserDetail userDetail) {
        if (AccessNodeTypeEnum.isGroupManually(accessNodeType) && StringUtils.isNotBlank(accessNodeGroupProcessId)) {
            AgentGroupDto info = findAgentGroupInfo(accessNodeGroupProcessId, userDetail);
            if (null == info) {
                //找不到当前标签
                throw new BizException("group.not.fund", accessNodeGroupProcessId);
            }
            if (null == info.getAgents()) {
                //无可用引擎
                throw new BizException("group.agent.not.available", info.getName());
            }
            return info.getAgentIds();
        }
        return null;
    }
}
