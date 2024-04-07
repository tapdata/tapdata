package com.tapdata.tm.agent.service;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.service.SettingsServiceImpl;
import com.tapdata.tm.agent.dto.AgentGroupDto;
import com.tapdata.tm.agent.dto.AgentRemoveFromGroupDto;
import com.tapdata.tm.agent.dto.AgentToGroupDto;
import com.tapdata.tm.agent.dto.AgentWithGroupBaseDto;
import com.tapdata.tm.agent.dto.GroupDto;
import com.tapdata.tm.agent.dto.GroupUsedDto;
import com.tapdata.tm.agent.entity.AgentGroupEntity;
import com.tapdata.tm.agent.repository.AgentGroupRepository;
import com.tapdata.tm.agent.util.AgentGroupTag;
import com.tapdata.tm.agent.util.AgentGroupUtil;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
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
import java.util.HashMap;
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

    /**
     * @param filter
     * @param userDetail
     * @param containWorker 是否在返回值中包含引擎的基本信息
     * @return 按分组统计后的引擎分页结果
     */
    public Page<AgentGroupDto> groupAllAgent(Filter filter, Boolean containWorker, UserDetail userDetail) {
        filter = agentGroupUtil.initFilter(filter);
        Page<GroupDto> groupDtoPage = find(filter, userDetail);
        List<GroupDto> items = groupDtoPage.getItems();
        if (CollectionUtils.isEmpty(items)) {
            return new Page<>(groupDtoPage.getTotal(), Lists.newArrayList());
        }
        final boolean equals = Boolean.TRUE.equals(containWorker);
        List<WorkerDto> all = getAllAgentId(items, equals, userDetail);
        Map<String, WorkerDto> map = equals ? all.stream()
                .filter(w -> Objects.nonNull(w) && Objects.nonNull(w.getProcessId()))
                .collect(Collectors.toMap(WorkerDto::getProcessId, w -> w)) : new HashMap<>();
        return new Page<>(groupDtoPage.getTotal(), items.stream()
                .filter(Objects::nonNull)
                .map(item -> generateDto(item, equals, map))
                .collect(Collectors.toList()));
    }

    protected List<WorkerDto> getAllAgentId(List<GroupDto> items, boolean equals, UserDetail userDetail) {
        Set<String> allAgentId = items.stream()
                .filter(a -> Objects.nonNull(a) && CollectionUtils.isNotEmpty(a.getAgentIds()))
                .map(GroupDto::getAgentIds)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        List<WorkerDto> all = equals ? findAllAgent(allAgentId, userDetail) : null;
        if (null == all) {
            all = Lists.newArrayList();
        }
        return all;
    }

    protected AgentGroupDto generateDto(GroupDto item, boolean equals, Map<String, WorkerDto> map) {
        List<String> agentIds = item.getAgentIds();
        AgentGroupDto dto = new AgentGroupDto();
        if (equals && Objects.nonNull(agentIds)) {
            List<WorkerDto> collect = agentIds.stream()
                    .map(map::get)
                    .collect(Collectors.toList());
            dto.setAgents(collect);
        }
        dto.setGroupId(item.getGroupId());
        dto.setName(item.getName());
        dto.setAgentIds(agentIds);
        return dto;
    }

    /**
     * 分组创建
     *
     * @param groupDto 待创建的分组的信息，分组名称
     * @return 创建好的分组信息
     */
    public AgentGroupDto createGroup(GroupDto groupDto, UserDetail userDetail) {
        final String name = groupDto.getName();
        ObjectId id = new ObjectId();
        AgentGroupDto dto = new AgentGroupDto();
        dto.setGroupId(id.toHexString());
        dto.setName(name);
        Query query = verifyCountGroupByName(name, userDetail);
        upsert(query, dto, userDetail);
        log.info("A agent group has be created - {}", name);
        return dto;
    }

    /**
     * 检查分组中包含的引擎数是否大于零
     *
     * @param userDetail
     * @param name       引擎分组名称
     */
    protected Query verifyCountGroupByName(String name, UserDetail userDetail) {
        Query query = Query.query(Criteria.where(AgentGroupTag.TAG_NAME).is(name).and(AgentGroupTag.TAG_DELETE).is(false));
        if (count(query, userDetail) > 0) {
            //Group Name重复
            throw new BizException("group.repeat");
        }
        return query;
    }

    /**
     * 将引擎加入到引擎分组中
     *
     * @param agentDto 包含引擎ID和引擎分组ID
     */
    public List<AgentGroupDto> batchOperator(AgentToGroupDto agentDto, UserDetail loginUser) {
        List<String> agentId = agentDto.getAgentId();
        List<String> groupId = agentDto.getGroupId();
        if (CollectionUtils.isEmpty(agentId) && CollectionUtils.isEmpty(groupId)) {
            return batchRemoveAll(loginUser);
        }
        if (CollectionUtils.isEmpty(agentId)) {
            return batchRemoveAllGroup(groupId, loginUser);
        }
        if (CollectionUtils.isEmpty(groupId)) {
            return batchRemoveAllAgent(agentId, loginUser);
        }
        return batchUpdate(agentId, groupId, loginUser);
    }

    protected List<AgentGroupDto> batchRemoveAll(UserDetail loginUser) {
        Query query = Query.query(Criteria.where(AgentGroupTag.TAG_DELETE).is(false));
        update(query, new Update().set(AgentGroupTag.TAG_AGENT_IDS, Lists.newArrayList()), loginUser);
        return findAgentGroupInfo(query, loginUser);
    }

    protected List<AgentGroupDto> batchRemoveAllAgent(List<String> agentIds, UserDetail loginUser) {
        if (CollectionUtils.isEmpty(agentIds)) {
            return Lists.newArrayList();
        }
        Criteria and = Criteria.where(AgentGroupTag.TAG_DELETE).is(false)
                .and(AgentGroupTag.TAG_AGENT_IDS).in(agentIds);
        List<AgentGroupEntity> all = findAll(Query.query(and), loginUser);
        List<String> allGroupId = all.stream()
                .filter(a -> Objects.nonNull(a) && Objects.nonNull(a.getGroupId()))
                .map(AgentGroupEntity::getGroupId)
                .collect(Collectors.toList());
        Criteria criteria = findCriteria(allGroupId);
        criteria.and(AgentGroupTag.TAG_AGENT_IDS).in(agentIds);
        update(Query.query(criteria),
                new Update().pullAll(AgentGroupTag.TAG_AGENT_IDS, agentIds.toArray()), loginUser);
        log.info("Agent remove: remove all group when group's agent list contains {}, group: {}", agentIds, allGroupId);
        return findAgentGroupInfoMany(allGroupId, loginUser);
    }

    protected List<AgentGroupDto> batchRemoveAllGroup(List<String> groupIds, UserDetail loginUser) {
        try {
            return updateAgent(groupIds, Lists.newArrayList(), loginUser);
        } finally {
            log.info("Agent batch operator: all agent are removed from groups: {} ", groupIds);
        }
    }

    protected List<AgentGroupDto> updateAgent(List<String> groupIds, List<String> newAgentIds, UserDetail loginUser) {
        if (CollectionUtils.isEmpty(groupIds)) {
            return Lists.newArrayList();
        }
        Criteria criteria = findCriteria(groupIds);
        update(Query.query(criteria),
                new Update().set(AgentGroupTag.TAG_AGENT_IDS, newAgentIds), loginUser);
        return findAgentGroupInfoMany(groupIds, loginUser);
    }

    protected List<AgentGroupDto> batchUpdate(List<String> agentIds, List<String> groupIds, UserDetail loginUser) {
        verifyAgent(agentIds, loginUser);
        try {
            return updateAgent(groupIds, agentIds, loginUser);
        } finally {
            log.info("Agent: {} has be added to group: {} ", agentIds, groupIds);
        }
    }

    protected void verifyAgent(List<String> agentIds, UserDetail loginUser) {
        List<WorkerDto> allAgent = findAllAgent(agentIds, loginUser);
        if (CollectionUtils.isEmpty(allAgent)) {
            throw new BizException("group.agent.not.fund", agentIds);
        }
    }

    public AgentGroupDto addAgentToGroup(AgentWithGroupBaseDto agentDto, UserDetail loginUser) {
        agentDto.verify();
        final String groupId = agentDto.getGroupId();
        final String agentId = agentDto.getAgentId();
        List<String> agentIds = Lists.newArrayList(agentId);
        verifyAgent(agentIds, loginUser);
        GroupDto groupDto = findGroupById(groupId, loginUser);
        List<String> agents = groupDto.getAgentIds();
        if (CollectionUtils.isNotEmpty(agents) && agents.contains(agentId)) {
            //引擎不能重复添加标签
            if (log.isDebugEnabled()) {
                log.debug("group agent repeatedly, {}", groupDto.toString());
            }
            return findAgentGroupInfo(groupId, loginUser);
        }
        Criteria criteria = findCriteria(Lists.newArrayList(groupId));
        criteria.and(AgentGroupTag.TAG_AGENT_IDS).nin(agentIds);
        UpdateResult updateResult = update(Query.query(criteria), new Update().push(AgentGroupTag.TAG_AGENT_IDS, agentId), loginUser);
        long modifiedCount = updateResult.getModifiedCount();
        if (modifiedCount <= 0) {
            //添加失败
            throw new BizException("group.agent.add.failed");
        }
        log.info("Agent: {} has be added to group: {} ", agentId, groupDto.getName());
        return findAgentGroupInfo(groupId, loginUser);
    }

    /**
     * 将引擎从引擎分组中移除
     * @param removeDto 包含引擎ID和引擎分组ID
     * */
    public AgentGroupDto removeAgentFromGroup(AgentRemoveFromGroupDto removeDto, UserDetail loginUser) {
        removeDto.verify();
        final String groupId = removeDto.getGroupId();
        final String agentId = removeDto.getAgentId();
        GroupDto groupDto = findGroupById(groupId, loginUser);
        Criteria criteria = findCriteria(Lists.newArrayList(groupDto.getGroupId()));
        criteria.and(AgentGroupTag.TAG_AGENT_IDS).in(Lists.newArrayList(agentId));
        UpdateResult updateResult = update(Query.query(criteria), new Update().pull(AgentGroupTag.TAG_AGENT_IDS, agentId), loginUser);
        long modifiedCount = updateResult.getModifiedCount();
        if (modifiedCount <= 0) {
            //移除失败
            throw new BizException("group.agent.remove.failed");
        }
        log.info("Agent: {} has be removed from group: {} ", agentId, groupDto.getName());
        return findAgentGroupInfo(groupId, loginUser);
    }

    /**
     * 删除引擎分组
     * @param groupId 引擎分组ID
     * */
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

    /**
     * 更新引擎分组信息
     *      - 目前只有分组名称有更新的需求
     * @param dto 包含引擎分组ID和引擎分组新名称
     * */
    public AgentGroupDto updateBaseInfo(GroupDto dto, UserDetail loginUser) {
        agentGroupUtil.verifyUpdateGroupInfo(dto);
        String groupId = dto.getGroupId();
        String name = dto.getName();
        verifyCountGroupByName(name, loginUser);
        Criteria criteria = findCriteria(Lists.newArrayList(groupId));
        update(Query.query(criteria), new Update().set(AgentGroupTag.TAG_NAME, name), loginUser);
        return findAgentGroupInfo(dto.getGroupId(), loginUser);
    }

    /**
     * 根据引擎分组ID查询引擎分组信息
     * @param groupId 分组ID
     * */
    protected GroupDto findGroupById(String groupId, UserDetail loginUser) {
        Criteria criteria = findCriteria(Lists.newArrayList(groupId));
        GroupDto groupDto = findOne(Query.query(criteria), loginUser);
        if (null == groupDto) {
            //找不到当前标签
            throw new BizException(AgentGroupTag.GROUP_NOT_FUND, groupId);
        }
        return groupDto;
    }

    protected List<AgentGroupDto> findAgentGroupInfoMany(List<String> groupIds, UserDetail loginUser) {
        Criteria criteria = findCriteria(groupIds);
        Query query = Query.query(criteria);
        return findAgentGroupInfo(query, loginUser);
    }

    /**
     * 根据引擎分组ID查询引擎分组信息
     * @param groupId 分组ID
     * */
    protected AgentGroupDto findAgentGroupInfo(String groupId, UserDetail loginUser) {
        List<AgentGroupDto> agentGroupInfoMany = findAgentGroupInfoMany(Lists.newArrayList(groupId), loginUser);
        if (CollectionUtils.isEmpty(agentGroupInfoMany)) {
            return null;
        }
        return agentGroupInfoMany.get(0);
    }

    public List<AgentGroupDto> findAgentGroupInfo(List<GroupDto> groupDto, UserDetail loginUser) {
        Set<String> agents = new HashSet<>();
        for (GroupDto dto : groupDto) {
            List<String> agentIds = dto.getAgentIds();
            if (CollectionUtils.isNotEmpty(agentIds)) {
                agents.addAll(agentIds);
            }
        }
        List<WorkerDto> all = null;
        if (!agents.isEmpty()) {
            all = findAllAgent(agents, loginUser);
        }
        List<AgentGroupDto> info = Lists.newArrayList();
        for (GroupDto group : groupDto) {
            collectAgentGroupDto(group, all, info);
        }
        return info;
    }

    protected void collectAgentGroupDto(GroupDto group, List<WorkerDto> all, List<AgentGroupDto> info) {
        AgentGroupDto dto = new AgentGroupDto();
        List<String> agentIds = group.getAgentIds();
        if (CollectionUtils.isNotEmpty(agentIds) && CollectionUtils.isNotEmpty(all)) {
            dto.setAgents(all.stream().filter(w -> {
                if (null == w || null == w.getProcessId()) return false;
                return agentIds.contains(w.getProcessId());
            }).collect(Collectors.toList()));
        }
        dto.setAgentIds(agentIds);
        dto.setName(group.getName());
        dto.setGroupId(group.getGroupId());
        info.add(dto);
    }

    /**
     * 查询引擎分组信息，包含分组中引擎的信息
     * */
    public List<AgentGroupDto> findAgentGroupInfo(Query query, UserDetail loginUser) {
        List<GroupDto> groupDto = findAllDto(query, loginUser);
        return findAgentGroupInfo(groupDto, loginUser);
    }

    public List<AgentGroupDto> findAgentGroupInfo(Filter filter, UserDetail loginUser) {
        filter = agentGroupUtil.initFilter(filter);
        GroupDto groupDto = findOne(filter, loginUser);
        return findAgentGroupInfo(Lists.newArrayList(groupDto), loginUser);
    }

    /**
     * 更具引擎ID列表查询引擎列表
     * */
    protected List<WorkerDto> findAllAgent(Collection<String> agentIds, UserDetail loginUser) {
        if (CollectionUtils.isEmpty(agentIds)) {
            return Lists.newArrayList();
        }
        Criteria criteria = Criteria.where(AgentGroupTag.TAG_PROCESS_ID).in(agentIds)
                .and(AgentGroupTag.TAG_WORKER_TYPE).is(AgentGroupTag.TAG_CONNECTOR);
        return workerServiceImpl.findAllDto(Query.query(criteria), loginUser);
    }

    /**
     * 在返回的引擎列表中增加引擎分组列表，其中引擎分组中包含对应的引擎列表
     * @param info 原来的引擎列表
     * */
    public List<AccessNodeInfo> filterGroupList(List<AccessNodeInfo> info, UserDetail loginUser) {
        if (settingsService.isCloud()) {
            return info;
        }
        if (CollectionUtils.isEmpty(info)) return info;
        Map<String, AccessNodeInfo> infoMap = info.stream()
                .filter(a -> Objects.nonNull(a) && Objects.nonNull(a.getProcessId()))
                .collect(Collectors.toMap(AccessNodeInfo::getProcessId, a -> a));
        Criteria criteria = findCriteria(null);
        List<AgentGroupEntity> entities = findAll(Query.query(criteria), loginUser);
        List<AccessNodeInfo> groupAgentList = entities.stream()
                .filter(a -> Objects.nonNull(a) && CollectionUtils.isNotEmpty(a.getAgentIds()))
                .sorted(agentGroupUtil::sortAgentGroup)
                .map(group -> agentGroupUtil.mappingAccessNodeInfo(group, infoMap))
                .collect(Collectors.toList());
        groupAgentList.addAll(info);
        return groupAgentList;
    }

    /**
     * 获取TaskDto中的引擎Ids,
     *  - 如果AccessNodeType==MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP，则先查询使用当前分组的引擎列表后返回、
     *  - 如果不是，则直接TaskDto.getAccessNodeProcessIdList()返回
     * */
    public List<String> getProcessNodeListWithGroup(TaskDto taskDto, UserDetail userDetail) {
        List<String> processNodeList = getProcessNodeList(taskDto.getAccessNodeType(), taskDto.getAccessNodeProcessId(), userDetail);
        if (processNodeList.isEmpty()) {
            return taskDto.getAccessNodeProcessIdList();
        }
        taskDto.setAccessNodeProcessIdList(processNodeList);
        return processNodeList;
    }

    /**
     * 获取 DataSourceConnectionDto 中的引擎Ids,
     *  - 如果AccessNodeType==MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP，则先查询使用当前分组的引擎列表后返回、
     *  - 如果不是，则直接 DataSourceConnectionDto.getAccessNodeProcessIdList() 返回
     * */
    public List<String> getProcessNodeListWithGroup(DataSourceConnectionDto connectionDto, UserDetail userDetail) {
        List<String> processNodeList = getDataSourceConnectionProcessNodeList(connectionDto, userDetail);
        if (processNodeList.isEmpty()) {
            return connectionDto.getAccessNodeProcessIdList();
        }
        return processNodeList;
    }

    /**
     * 获取 DataSourceConnectionDto 中的引擎Ids,
     *  - 如果AccessNodeType==MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP，则先查询使用当前分组的引擎列表后返回、
     *  - 如果不是，则直接 DataSourceConnectionDto.getTrueAccessNodeProcessIdList() 返回
     * */
    public List<String> getTrueProcessNodeListWithGroup(DataSourceConnectionDto connectionDto, UserDetail userDetail) {
        List<String> processNodeList = getDataSourceConnectionProcessNodeList(connectionDto, userDetail);
        if (processNodeList.isEmpty()) {
            return connectionDto.getTrueAccessNodeProcessIdList();
        }
        return processNodeList;
    }

    /**
     * 获取 DataSourceConnectionDto 中的引擎Ids,
     *  - 如果AccessNodeType==MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP，则先查询使用当前分组的引擎列表后返回、
     *  - 如果不是，则直接 DataSourceConnectionDto.getTrueAccessNodeProcessIdList() 返回
     * */
    protected List<String> getDataSourceConnectionProcessNodeList(DataSourceConnectionDto connectionDto, UserDetail userDetail) {
        List<String> processNodeList = getProcessNodeList(connectionDto.getAccessNodeType(), connectionDto.getAccessNodeProcessId(), userDetail);
        connectionDto.setAccessNodeProcessIdList(processNodeList);
        return processNodeList;
    }

    /**
     * 根据 accessNodeType 和 accessNodeGroupProcessId 获取引擎Ids,
     *  - 如果AccessNodeType==MANUALLY_SPECIFIED_BY_THE_USER_AGENT_GROUP，则先查询使用当前分组的引擎列表后返回
     *  - 如果不是，则直接返回空列表
     * */
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
        if (AccessNodeTypeEnum.isGroupManually(accessNodeType)) {
            return getProcessNodeListByGroupId(accessNodeGroupProcessId, userDetail);
        }
        return accessNodeGroupProcessId;
    }

    /**
     * 根据分组ID列表查询引擎ID列表
     * */
    public List<String> getProcessNodeListByGroupId(List<String> groupIds, UserDetail userDetail) {
        if (CollectionUtils.isEmpty(groupIds)) {
            return Lists.newArrayList();
        }
        Criteria criteria = findCriteria(groupIds);
        Query query = Query.query(criteria);
        List<AgentGroupEntity> all = findAll(query, userDetail);
        if (CollectionUtils.isEmpty(all)) {
            //找不到当前标签
            throw new BizException(AgentGroupTag.GROUP_NOT_FUND, groupIds.toString());
        }
        Set<String> agents = new HashSet<>();
        all.stream().filter(dto -> Objects.nonNull(dto) && CollectionUtils.isNotEmpty(dto.getAgentIds()))
                .forEach(dto -> agents.addAll(dto.getAgentIds()));
        if (agents.isEmpty()) {
            //无可用引擎
            throw new BizException("group.agent.not.available", groupIds.toString());
        }
        return new ArrayList<>(agents);
    }

    protected Criteria findCriteria(List<String> groupIds) {
        Criteria criteria = Criteria.where(AgentGroupTag.TAG_DELETE).is(false);
        if (null == groupIds || groupIds.isEmpty()) {
            return criteria;
        }
        if (groupIds.size() == 1) {
            criteria.and(AgentGroupTag.TAG_GROUP_ID).in(groupIds.get(0));
        } else {
            criteria.and(AgentGroupTag.TAG_GROUP_ID).in(groupIds);
        }
        return criteria;
    }
}
