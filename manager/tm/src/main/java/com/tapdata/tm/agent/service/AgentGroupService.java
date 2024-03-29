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
import com.tapdata.tm.agent.util.AgentGroupTag;
import com.tapdata.tm.agent.util.AgentGroupUtil;
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
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.service.WorkerServiceImpl;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
    protected WorkerServiceImpl workerServiceImpl;
    @Autowired
    protected DataSourceService dataSourceService;
    @Autowired
    protected TaskServiceImpl taskService;
    @Autowired
    protected SettingsServiceImpl settingsService;
    @Autowired
    protected AgentGroupUtil agentGroupUtil;


    public AgentGroupService(@NonNull AgentGroupRepository repository) {
        super(repository, GroupDto.class, AgentGroupEntity.class);
    }

    @Override
    protected void beforeSave(GroupDto dto, UserDetail userDetail) {
        //do nothing
    }

    public Page<AgentGroupDto> groupAllAgent(Filter filter, Boolean containWorker, UserDetail userDetail) {
        filter = agentGroupUtil.initFilter(filter);
        Page<GroupDto> groupDtoPage = find(filter, userDetail);
        List<GroupDto> items = groupDtoPage.getItems();
        if (CollectionUtils.isEmpty(items)) {
            return new Page<>(groupDtoPage.getTotal(), Lists.newArrayList());
        }
        Set<String> allAgentId = items.stream()
                .map(GroupDto::getAgentIds)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        final boolean equals = Boolean.TRUE.equals(containWorker);
        List<WorkerDto> all = equals ? findAllAgent(allAgentId, userDetail) : null;
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
        if (succeedCount < 1) {
            //Group Name重复
            throw new BizException("group.repeat");
        }
        log.info("A agent group has be created - {}", name);
        return dto;
    }

    protected Query verifyCountGroupByName(String name, UserDetail userDetail) {
        Query query = Query.query(Criteria.where(AgentGroupTag.TAG_NAME).is(name).and(AgentGroupTag.TAG_DELETE).is(false));
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
        List<String> agentIds = Lists.newArrayList(agentId);
        List<WorkerDto> allAgent = findAllAgent(agentIds, loginUser);
        if (null == allAgent || allAgent.isEmpty()) {
            throw new BizException("group.agent.not.fund", agentId);
        }
        GroupDto groupDto = findGroupById(groupId, loginUser);
        List<String> agents = groupDto.getAgentIds();
        if (!(null == agents || agents.isEmpty() || !agents.contains(agentId))) {
            //引擎不能重复添加标签
            throw new BizException("group.agent.repeatedly");
        }
        UpdateResult updateResult = update(
                Query.query(Criteria.where(AgentGroupTag.TAG_GROUP_ID).is(groupId)
                        .and(AgentGroupTag.TAG_DELETE).is(false)
                        .and(AgentGroupTag.TAG_AGENT_IDS).nin(agentIds)),
                new Update().push(AgentGroupTag.TAG_AGENT_IDS, agentId), loginUser);
        long modifiedCount = updateResult.getModifiedCount();
        if (modifiedCount <= 0) {
            //添加失败
            throw new BizException("group.agent.add.failed");
        }
        log.info("Agent: {} has be added to group: {} ", agentId, groupDto.getName());
        return findAgentGroupInfo(groupId, loginUser);
    }

    public AgentGroupDto removeAgentFromGroup(AgentRemoveFromGroupDto removeDto, UserDetail loginUser) {
        removeDto.verify();
        final String groupId = removeDto.getGroupId();
        final String agentId = removeDto.getAgentId();
        GroupDto groupDto = findGroupById(groupId, loginUser);
        UpdateResult updateResult = update(Query.query(Criteria
                .where(AgentGroupTag.TAG_GROUP_ID).is(groupDto.getGroupId())
                .and(AgentGroupTag.TAG_DELETE).is(false)
                .and(AgentGroupTag.TAG_AGENT_IDS).in(Lists.newArrayList(agentId))), new Update().pull(AgentGroupTag.TAG_AGENT_IDS, agentId), loginUser);
        long modifiedCount = updateResult.getModifiedCount();
        if (modifiedCount <= 0) {
            //移除失败
            throw new BizException("group.agent.remove.failed");
        }
        log.info("Agent: {} has be removed from group: {} ", agentId, groupDto.getName());
        return findAgentGroupInfo(groupId, loginUser);
    }


    public GroupUsedDto deleteGroup(String groupId, UserDetail loginUser) {
        GroupDto groupDto = findGroupById(groupId, loginUser);
        //查询正在使用当前标签的数据源
        GroupUsedDto result = new GroupUsedDto();
        Query connectionQuery = Query.query(
                Criteria.where(AgentGroupTag.TAG_ACCESS_NODE_TYPE).is(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP.name())
                .and(AgentGroupTag.TAG_ACCESS_NODE_PROCESS_ID_LIST).in(Lists.newArrayList(groupId))
                .and(AgentGroupTag.TAG_DELETE).is(false));

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
            if (deleted) {
                result.setDeleteMsg("Delete [\"" + groupDto.getName() + "\"] succeed");
                log.info("Agent group has be deleted: {} ", groupDto.getName());
            } else {
                result.setDeleteMsg("Delete [\"" + groupDto.getName() + "\"] failed, please try again");
            }
        } else {
            result.setDeleted(false);
            result.setDeleteMsg("The current agent tag has been used by some data source connections or tasks and cannot be deleted");
        }
        return result;
    }

    public AgentGroupDto updateBaseInfo(GroupDto dto, UserDetail loginUser) {
        agentGroupUtil.verifyUpdateGroupInfo(dto);
        String groupId = dto.getGroupId();
        String name = dto.getName();
        verifyCountGroupByName(name, loginUser);
        update(Query.query(Criteria
                .where(AgentGroupTag.TAG_GROUP_ID).is(groupId)
                .and(AgentGroupTag.TAG_DELETE).is(false)),
                new Update().set(AgentGroupTag.TAG_NAME, name), loginUser);
        return findAgentGroupInfo(dto.getGroupId(), loginUser);
    }

    protected GroupDto findGroupById(String groupId, UserDetail loginUser) {
        Criteria criteria = Criteria.where(AgentGroupTag.TAG_GROUP_ID).is(groupId).and(AgentGroupTag.TAG_DELETE).is(false);
        GroupDto groupDto = findOne(Query.query(criteria), loginUser);
        if (null == groupDto) {
            //找不到当前标签
            throw new BizException("group.not.fund", groupId);
        }
        return groupDto;
    }

    protected AgentGroupDto findAgentGroupInfo(String groupId, UserDetail loginUser) {
        Filter filter = new Filter();
        filter.setWhere(Where.where(AgentGroupTag.TAG_GROUP_ID, groupId).and(AgentGroupTag.TAG_DELETE, false));
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
        Criteria criteria = Criteria.where(AgentGroupTag.TAG_PROCESS_ID).in(agentIds)
                .and(AgentGroupTag.TAG_WORKER_TYPE).is(AgentGroupTag.TAG_CONNECTOR);
        return workerServiceImpl.findAllDto(Query.query(criteria), loginUser);
    }


    public List<AccessNodeInfo> filterGroupList(List<AccessNodeInfo> info, UserDetail loginUser) {
        if (settingsService.isCloud()) {
            return info;
        }
        if (null == info || info.isEmpty()) return info;
        Map<String, AccessNodeInfo> infoMap = info.stream().collect(Collectors.toMap(AccessNodeInfo::getProcessId, a -> a));
        List<AgentGroupEntity> entities = findAll(Query.query(Criteria.where(AgentGroupTag.TAG_DELETE).is(false)), loginUser);
        List<AccessNodeInfo> groupAgentList = entities.stream()
                .filter(Objects::nonNull)
                .sorted(agentGroupUtil::sortAgentGroup)
                .map(group -> agentGroupUtil.mappingAccessNodeInfo(group, infoMap))
                .collect(Collectors.toList());
        groupAgentList.addAll(info);
        return groupAgentList;
    }

    public List<String> getProcessNodeListWithGroup(TaskDto taskDto, UserDetail userDetail) {
        List<String> processNodeList = getProcessNodeList(taskDto.getAccessNodeType(), taskDto.getAccessNodeProcessId(), userDetail);
        if (processNodeList.isEmpty()) {
            return taskDto.getAccessNodeProcessIdList();
        }
        taskDto.setAccessNodeProcessIdList(processNodeList);
        return processNodeList;
    }

    public List<String> getProcessNodeListWithGroup(DataSourceConnectionDto connectionDto, UserDetail userDetail) {
        List<String> processNodeList = getDataSourceConnectionProcessNodeList(connectionDto, userDetail);
        if (processNodeList.isEmpty()) {
            return connectionDto.getAccessNodeProcessIdList();
        }
        return processNodeList;
    }

    public List<String> getTrueProcessNodeListWithGroup(DataSourceConnectionDto connectionDto, UserDetail userDetail) {
        List<String> processNodeList = getDataSourceConnectionProcessNodeList(connectionDto, userDetail);
        if (processNodeList.isEmpty()) {
            return connectionDto.getTrueAccessNodeProcessIdList();
        }
        return processNodeList;
    }

    protected List<String> getDataSourceConnectionProcessNodeList(DataSourceConnectionDto connectionDto, UserDetail userDetail) {
        List<String> processNodeList = getProcessNodeList(connectionDto.getAccessNodeType(), connectionDto.getAccessNodeProcessId(), userDetail);
        connectionDto.setAccessNodeProcessIdList(processNodeList);
        return processNodeList;
    }

    protected List<String> getProcessNodeList(String accessNodeType, String accessNodeGroupProcessId, UserDetail userDetail) {
        if (log.isDebugEnabled()) {
            log.debug("Get process node list once, accessNodeType: {}, accessNodeGroupProcessId: {}", accessNodeType, accessNodeGroupProcessId);
        }
        if (AccessNodeTypeEnum.isGroupManually(accessNodeType) && StringUtils.isNotBlank(accessNodeGroupProcessId)) {
            return getProcessNodeListByGroupId(Lists.newArrayList(accessNodeGroupProcessId), userDetail);
        }
        return Lists.newArrayList();
    }

    /**
     * 根据分组ID获取全部AgentID
     * */
    public List<String> getProcessNodeListByGroupId(List<String> accessNodeGroupProcessId, String accessNodeType, UserDetail userDetail) {
        if (AccessNodeTypeEnum.isGroupManually(String.valueOf(accessNodeType))) {
            return getProcessNodeListByGroupId(accessNodeGroupProcessId, userDetail);
        }
        return accessNodeGroupProcessId;
    }

    public List<String> getProcessNodeListByGroupId(List<String> groupIds, UserDetail userDetail) {
        if (null == groupIds || groupIds.isEmpty()) {
            return Lists.newArrayList();
        }
        List<AgentGroupEntity> all = findAll(Query.query(Criteria.where(AgentGroupTag.TAG_GROUP_ID).in(groupIds).and(AgentGroupTag.TAG_DELETE).is(false)), userDetail);
        if (null == all || all.isEmpty()) {
            //找不到当前标签
            throw new BizException("group.not.fund", groupIds.toString());
        }
        Set<String> agents = new HashSet<>();
        all.stream().filter(dto -> Objects.nonNull(dto) && Objects.nonNull(dto.getAgentIds()) && ! dto.getAgentIds().isEmpty())
                .forEach(dto -> agents.addAll(dto.getAgentIds()));
        if (agents.isEmpty()) {
            //无可用引擎
            throw new BizException("group.agent.not.available", groupIds.toString());
        }
        return new ArrayList<>(agents);
    }
}
