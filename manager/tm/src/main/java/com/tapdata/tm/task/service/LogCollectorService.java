package com.tapdata.tm.task.service;

import com.mongodb.ConnectionString;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.Settings.constant.SettingsEnum;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.DataNode;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.SubTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.task.bean.*;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeanUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author: Zed
 * @Date: 2022/2/15
 * @Description:
 */
@Service
@Slf4j
public class LogCollectorService {

    private final TaskService taskService;

    private final SubTaskService subTaskService;
    private final DataSourceService dataSourceService;
    private final WorkerService workerService;

    private final SettingsService settingsService;

    public LogCollectorService(TaskService taskService, SubTaskService subTaskService, DataSourceService dataSourceService,
                               WorkerService workerService, SettingsService settingsService) {
        this.taskService = taskService;
        this.subTaskService = subTaskService;
        this.dataSourceService = dataSourceService;
        this.workerService = workerService;
        this.settingsService = settingsService;
    }


    private List<String> syncTimePoints = Lists.newArrayList("current", "localTZ", "connTZ");;

    public Page<LogCollectorVo> find(String name, UserDetail user, int skip, int limit, List<String> sort) {

        Criteria criteria = Criteria.where("is_deleted").is(false).and("syncType").is("logCollector");
        if (StringUtils.isNotBlank(name)) {
            criteria.and("name").regex(name);
        }

        Query query = new Query(criteria);
        query.skip(skip);
        query.limit(limit);
        MongoUtils.applySort(query, sort);
        query.fields().include("status", "name", "createTime", "dag", "statuses");

        List<TaskDto> allDto = taskService.findAllDto(query, user);
        long count = taskService.count(new Query(criteria), user);
        List<LogCollectorVo> logCollectorVos = convertTask(allDto);
        Page<LogCollectorVo> logCollectorVoPage = new Page<>();
        logCollectorVoPage.setItems(logCollectorVos);
        logCollectorVoPage.setTotal(count);
        return logCollectorVoPage;


    }

    /**
     * 通过挖掘任务id查询被使用到的挖掘的同步任务的子任务
     * 1.首先查询是否存在挖掘任务
     * @param taskDto
     * @return
     */
    public List<SubTaskDto> findSyncTaskById(TaskDto taskDto, UserDetail user) {

        if (taskDto == null) {
            return null;
        }

        DAG dag = taskDto.getDag();
        List<Node> sources = dag.getSources();

        if (CollectionUtils.isEmpty(sources)) {
            return null;
        }

        Node node = sources.get(0);

        LogCollectorNode node1 = (LogCollectorNode) node;
        List<String> connectionIds = node1.getConnectionIds();



        //查询所有开启日志挖掘并且启动的使用的数据源包含该数据源的任务
        Criteria taskCriteria = Criteria.where("is_deleted").is(false).and("shareCdcEnable").is(true).and("syncType").ne("logCollector").
                and("dag.nodes").elemMatch(Criteria.where("catalog").is("data").and("connectionId").in(connectionIds));


        Query taskQuery = new Query(taskCriteria);
        taskQuery.fields().include("_id", "syncType");
        List<TaskDto> allDtos = taskService.findAllDto(taskQuery, user);
        if (CollectionUtils.isEmpty(allDtos)) {
            return null;
        }

        Map<ObjectId, TaskDto> dtoMap = allDtos.stream().collect(Collectors.toMap(TaskDto::getId, t -> t));

        //再通过主任务去查询子任务
        List<ObjectId> parentIds = allDtos.stream().map(TaskDto::getId).collect(Collectors.toList());
        Criteria subTaskCriteria = Criteria.where("parentId").in(parentIds).and("dag.nodes").elemMatch(Criteria.where("catalog").is("data").and("connectionId").in(connectionIds));
        Query subTaskQuery = new Query(subTaskCriteria);
        List<SubTaskDto> subTaskDtos = subTaskService.findAllDto(subTaskQuery, user);

        List<SubTaskDto> callSubtasks = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(subTaskDtos)) {
            for (SubTaskDto subTaskDto : subTaskDtos) {
                List<Node> nodes = subTaskDto.getDag().getNodes();
                List<Node> targets = subTaskDto.getDag().getTargets();
                nodes.removeAll(targets);
                for (Node node2 : nodes) {
                    if (node2 instanceof DataParentNode) {
                        String connectionId = ((DataParentNode<?>) node2).getConnectionId();
                        if (connectionIds.contains(connectionId)) {
                            callSubtasks.add(subTaskDto);
                        }
                    }
                }

            }
        }

        if (CollectionUtils.isNotEmpty(callSubtasks)) {
            for (SubTaskDto subTaskDto : callSubtasks) {
                TaskDto taskDto1 = dtoMap.get(subTaskDto.getParentId());
                subTaskDto.setParentSyncType(taskDto1.getSyncType());
            }
        }

        //考虑下要不要把task设置到subTaskDto里面
        return callSubtasks;
    }

    /**
     * 通过任务id查询当前任务用到的共享挖掘任务
     * @param taskId
     * @return
     */
    public List<LogCollectorVo> findByTaskId(String taskId, UserDetail user) {
        ObjectId objectId = MongoUtils.toObjectId(taskId);
        //查询任务是否存在，任务不能是一个挖掘任务
        Criteria taskCriteria = Criteria.where("_id").is(objectId).and("is_deleted").is(false).and("shareCdcEnable").is(true).and("sync_type").ne("logCollector");
        Query taskQuery = new Query(taskCriteria);
        TaskDto taskDto = taskService.findOne(taskQuery, user);
        if (taskDto == null) {
            return null;
        }

        DAG dag = taskDto.getDag();
        //获取任务的所有的除了尾结点的数据节点，根据数据源id查询所有的挖掘任务

        return getLogCollectorVos(dag, user);

    }


    /**
     * 通过子任务id查询当前任务用到的共享挖掘任务
     * @param subTaskId
     * @return
     */
    public List<LogCollectorVo> findBySubTaskId(String subTaskId, UserDetail user) {
        ObjectId objectId = MongoUtils.toObjectId(subTaskId);
        //查询任务是否存在，任务不能是一个挖掘任务
        Criteria subTaskCriteria = Criteria.where("_id").is(objectId);
        Query subTaskQuery = new Query(subTaskCriteria);
        subTaskQuery.fields().include("dag", "parentId");
        SubTaskDto subTaskDto = subTaskService.findOne(subTaskQuery, user);
        if (subTaskDto == null) {
            return new ArrayList<>();
        }

        Query query = new Query(Criteria.where("_id").is(subTaskDto.getParentId()).and("shareCdcEnable").is(true).and("sync_type").ne("logCollector"));
        query.fields().include("_id");
        long count = taskService.count(query, user);

        if (count < 1) {
            return new ArrayList<>();
        }

        DAG dag = subTaskDto.getDag();
        return getLogCollectorVos(dag, user);

    }

    @NotNull
    private List<LogCollectorVo> getLogCollectorVos(DAG dag, UserDetail user) {
        //获取任务的所有的除了尾结点的数据节点，根据数据源id查询所有的挖掘任务

        List<Node> allNodes = dag.getNodes();
        List<Node> targets = dag.getTargets();
        Set<String> sinkIds = targets.stream().map(Node::getId).collect(Collectors.toSet());

        List<String> connectionIds = allNodes.stream().
                filter(Node::isDataNode)
                .filter(n -> !sinkIds.contains(n.getId()))
                .map(n -> ((DataParentNode) n).getConnectionId()).collect(Collectors.toList());

        Pair<Criteria, List<TaskDto>> taskByConnectionIds = findTaskByConnectionIds(null, connectionIds, user, null, null, null);
        return convertTask(taskByConnectionIds.getValue());
    }

    private Pair<Criteria, List<TaskDto>> findTaskByConnectionIds(String name, List<String> connectionIds, UserDetail user, Integer skip, Integer limit, List<String> sort) {
        Criteria criteria1 = Criteria.where("is_deleted").is(false).and("syncType").is("logCollector").and("dag.nodes").elemMatch(Criteria.where("type").is("logCollector").and("connectionIds").elemMatch(Criteria.where("$in").is(connectionIds)));

        if (StringUtils.isNotBlank(name)) {
            criteria1.and("name").regex(name);
        }

        Query query = new Query(criteria1);
        query.fields().include("status", "name", "createTime", "dag");
        if (limit != null) {
            query.limit(limit);
        }
        if (skip != null) {
            query.skip(skip);
        }
        MongoUtils.applySort(query, sort);
        List<TaskDto> taskDtos = taskService.findAllDto(query, user);

        return ImmutablePair.of(criteria1, taskDtos);
    }

    /**
     * 根据数据源的名称模糊查询共享挖掘的任务
     * @param connectionName
     * @return
     */
    public Page<LogCollectorVo> findByConnectionName(String name, String connectionName, UserDetail user, int skip, int limit, List<String> sort) {
        //通过数据源的名称模糊查询一个数据源列表
        Query nameQuery = new Query(Criteria.where("name").regex(connectionName));
        nameQuery.fields().include("_id");
        List<DataSourceConnectionDto> connectionDtos = dataSourceService.findAllDto(nameQuery, user);
        List<String> connectionIds = connectionDtos.stream().map(d -> d.getId().toHexString()).collect(Collectors.toList());
        Pair<Criteria, List<TaskDto>> pair = findTaskByConnectionIds(name, connectionIds, user, skip, limit, sort);
        Query query = new Query(pair.getKey());
        long count = taskService.count(query, user);

        Page<LogCollectorVo> page = new Page<>();
        page.setTotal(count);
        page.setItems(convertTask(pair.getValue()));

        return page;
    }


    /**
     * 校验系统是否具备开启挖掘任务条件
     * @return
     */
    public boolean checkCondition(UserDetail user) {
        //查看系统设置中的共享增量方式中，是否存在完整的配置
        String mode = SettingsEnum.SHARE_CDC_PERSISTENCE_MODE.getValue();
        if ("MongoDB".equals(mode)) {
            String mongoUrl = SettingsEnum.SHARE_CDC_PERSISTENCE_MONGODB_COLLECTION.getValue();
            if (StringUtils.isBlank(mongoUrl)) {
                return false;
            }
        }

        List<Worker> availableAgents = workerService.findAvailableAgent(user);
        if (CollectionUtils.isNotEmpty(availableAgents)) {
            if (availableAgents.size() > 1 && !"MongoDB".equals(mode)) {
                //只有mongodb的模式是支持多个可以agent的，其他的都是不支持的
                return false;
            }
        }


        return true;
    }

    public void update(LogCollectorEditVo logCollectorEditVo, UserDetail user) {


        ObjectId objectId = MongoUtils.toObjectId(logCollectorEditVo.getId());
        TaskDto taskDto = taskService.checkExistById(objectId, user);
        Update update = new Update();

        if (StringUtils.isNotBlank(logCollectorEditVo.getName())) {
            taskService.checkTaskName(logCollectorEditVo.getName(), user, objectId);
            update.set("name", logCollectorEditVo.getName());
        }

        if (StringUtils.isNotBlank(logCollectorEditVo.getSyncTimePoint()) && syncTimePoints.contains(logCollectorEditVo.getSyncTimePoint())) {
            update.set("syncPoints.pointType", logCollectorEditVo.getSyncTimePoint());
        }

        if (StringUtils.isNotBlank(logCollectorEditVo.getSyncTineZone())) {
            update.set("syncPoints.timeZone", logCollectorEditVo.getSyncTineZone());
        }

        if (StringUtils.isNotBlank(logCollectorEditVo.getSyncTime())) {
            update.set("syncPoints.dateTime", logCollectorEditVo.getSyncTime());
        }

        taskService.updateById(objectId, update, user);

        List<Node> sources = taskDto.getDag().getSources();
        Node node = sources.get(0);

        List<SubTaskDto> subTaskDtos = subTaskService.findByTaskId(taskDto.getId(), "_id");
        if (CollectionUtils.isEmpty(subTaskDtos)) {
            return;
        }
        SubTaskDto subTaskDto = subTaskDtos.get(0);

        if (logCollectorEditVo.getStorageTime() != null) {
            Document document = new Document();
            document.put("storageTime", logCollectorEditVo.getStorageTime());
            Document _body = new Document();
            _body.put("$set", document);
            subTaskService.updateNode(subTaskDto.getId(), node.getId(), _body, user);
        }
    }


    public LogCollectorDetailVo findDetail(String id, UserDetail user) {
        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(id), user);
        if (taskDto == null) {
            return null;
        }
        DAG dag = taskDto.getDag();

        if (dag == null) {
            return null;
        }

        List<Node> sources = dag.getSources();
        if (CollectionUtils.isEmpty(sources)) {
            return null;
        }

        Node node = sources.get(0);

        if (node == null) {
            return null;
        }
        if (node instanceof LogCollectorNode ) {

            List<SubTaskDto> subTaskDtos = findSyncTaskById(taskDto, user);

            LogCollectorVo logCollectorVo = convert(taskDto);
            LogCollectorDetailVo logCollectorDetailVo = new LogCollectorDetailVo();
            BeanUtils.copyProperties(logCollectorVo, logCollectorDetailVo);
            if (CollectionUtils.isNotEmpty(subTaskDtos)) {
                List<SyncTaskVo> syncTaskVos = convertSubTask(subTaskDtos, ((LogCollectorNode) node).getConnectionIds());
                logCollectorDetailVo.setTaskList(syncTaskVos);
            }

            List<SubTaskDto> logSubTasks = subTaskService.findByTaskId(taskDto.getId(), "attrs", "dag");
            if (CollectionUtils.isNotEmpty(logSubTasks)) {
                SubTaskDto subTaskDto = logSubTasks.get(0);
                logCollectorDetailVo.setSubTaskId(subTaskDto.getId().toHexString());
                DAG dag1 = subTaskDto.getDag();
                if (dag1 != null) {
                    List<Edge> edges = dag1.getEdges();
                    Edge edge = edges.get(0);
                    Date eventTime = getAttrsValues(edge.getSource(), edge.getTarget(), "eventTime", subTaskDto.getAttrs());
                    logCollectorDetailVo.setLogTime(eventTime);
                }
            }
            return logCollectorDetailVo;
        } else {
            return null;
        }
    }


    private LogCollectorVo convert(TaskDto taskDto) {
        LogCollectorVo logCollectorVo = new LogCollectorVo();
        logCollectorVo.setName(taskDto.getName());
        logCollectorVo.setId(taskDto.getId().toString());
        logCollectorVo.setCreateTime(taskDto.getCreateAt());
        logCollectorVo.setStatus(taskDto.getStatus());
        logCollectorVo.setStatuses(taskDto.getStatuses());
        List<TaskDto.SyncPoint> syncPoints = taskDto.getSyncPoints();
        if (CollectionUtils.isNotEmpty(syncPoints)) {
            TaskDto.SyncPoint syncPoint = syncPoints.get(0);
            logCollectorVo.setSyncTimePoint(syncPoint.getPointType());
            logCollectorVo.setSyncTime(new Date(syncPoint.getDateTime()));
            logCollectorVo.setSyncTimeZone(syncPoint.getTimeZone());
        }

        if (taskDto.getDag() != null) {
            List<Node> sources = taskDto.getDag().getSources();

            if (CollectionUtils.isNotEmpty(sources)) {
                Node node = sources.get(0);
                if (node instanceof LogCollectorNode) {
                    LogCollectorNode logCollectorNode = (LogCollectorNode) sources.get(0);
                    if (logCollectorNode != null) {
                        List<ObjectId> ids = logCollectorNode.getConnectionIds().stream().map(MongoUtils::toObjectId).collect(Collectors.toList());
                        Criteria criteria = Criteria.where("_id").in(ids);
                        Query query = new Query(criteria);
                        query.fields().include("name");
                        List<DataSourceConnectionDto> datasources = dataSourceService.findAll(query);
                        List<Pair<String, String>> datasourcePairs = datasources.stream().map(d -> ImmutablePair.of(d.getId().toHexString(), d.getName())).collect(Collectors.toList());
                        logCollectorVo.setConnections(datasourcePairs);
                        logCollectorVo.setTableName(logCollectorNode.getTableNames());
                        logCollectorVo.setStorageTime(logCollectorNode.getStorageTime());
                    }
                }
            }
        }

        return logCollectorVo;
    }


    private SyncTaskVo convert(SubTaskDto subTaskDto, List<String> connectionIds) {
        SyncTaskVo logCollectorVo = new SyncTaskVo();
        logCollectorVo.setId(subTaskDto.getId().toString());
        logCollectorVo.setName(subTaskDto.getName());
        logCollectorVo.setStatus(subTaskDto.getStatus());
        logCollectorVo.setSyncTimestamp(new Date());
        logCollectorVo.setSourceTimestamp(new Date());
        logCollectorVo.setParentId(subTaskDto.getParentId().toHexString());
        logCollectorVo.setSyncType(subTaskDto.getParentSyncType());

        DAG dag = subTaskDto.getDag();
        try {
            if (dag != null) {
                List<Node> nodes = dag.getNodes();
                if (CollectionUtils.isNotEmpty(nodes)) {
                    for (Node node : nodes) {
                        if (node instanceof DataNode) {
                            String connectionId = ((DataNode) node).getConnectionId();
                            if (connectionIds.contains(connectionId)) {
                                List<Node> target = dag.getTarget(node.getId());
                                if (CollectionUtils.isNotEmpty(target)) {
                                    Node targetNode = target.get(0);
                                    Date eventTime = getAttrsValues(node.getId(), targetNode.getId(), "eventTime", subTaskDto.getAttrs());
                                    Date sourceTime = getAttrsValues(node.getId(), targetNode.getId(), "sourceTime", subTaskDto.getAttrs());
                                    logCollectorVo.setSyncTimestamp(eventTime);
                                    logCollectorVo.setSourceTimestamp(sourceTime);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("set log collector date time error");
        }

        return logCollectorVo;
    }



    private List<SyncTaskVo> convertSubTask(List<SubTaskDto> subTaskDtos, List<String> connectionIds) {
        List<SyncTaskVo> syncTaskVos = new ArrayList<>();
        for (SubTaskDto subTaskDto : subTaskDtos) {
            syncTaskVos.add(convert(subTaskDto, connectionIds));
        }
        return syncTaskVos;
    }


    private List<LogCollectorVo> convertTask(List<TaskDto> TaskDtos) {
        List<LogCollectorVo> logCollectorVos = new ArrayList<>();
        for (TaskDto taskDto : TaskDtos) {
            logCollectorVos.add(convert(taskDto));
        }
        return logCollectorVos;
    }

    public LogSystemConfigDto findSystemConfig(UserDetail loginUser) {
        String value0 = SettingsEnum.SHARE_CDC_PERSISTENCE_MODE.getValue();
        String value1 = SettingsEnum.SHARE_CDC_PERSISTENCE_MEMORY_SIZE.getValue();
        String value2 = SettingsEnum.SHARE_CDC_PERSISTENCE_MONGODB_URI_DB.getValue();
        String value3 = SettingsEnum.SHARE_CDC_PERSISTENCE_MONGODB_COLLECTION.getValue();
        String value4 = SettingsEnum.SHARE_CDC_PERSISTENCE_ROCKSDB_PATH.getValue();
        String value5 = SettingsEnum.SHARE_CDC_TTL_DAY.getValue();

        if (StringUtils.isNotBlank(value2)) {
            ConnectionString connectionString = null;
            try {
                connectionString = new ConnectionString(value2);
            } catch (Exception e) {
                log.error("Parse connection string failed ({})", value2, e);
            }

            if (connectionString != null ) {
                char[] password = connectionString.getPassword();

                if (password != null) {
                    String password1 = new String(password);
                    value2 = value2.replace(password1, "******");
                }

            }
        }

        LogSystemConfigDto logSystemConfigDto = new LogSystemConfigDto(value0, value1, value2, value3, value4, value5);
        return logSystemConfigDto;

    }

    public void updateSystemConfig(LogSystemConfigDto logSystemConfigDto, UserDetail user) {

        Boolean canUpdate = checkUpdateConfig(user);

        if (!canUpdate) {
            throw new BizException("LogCollectConfigUpdateError");
        }

        String persistenceMode = logSystemConfigDto.getPersistenceMode();
        if (persistenceMode != null) {
            settingsService.update(SettingsEnum.SHARE_CDC_PERSISTENCE_MODE, persistenceMode);
        }

        String persistenceMemory_size = logSystemConfigDto.getPersistenceMemory_size();
        if (persistenceMemory_size != null) {
            settingsService.update(SettingsEnum.SHARE_CDC_PERSISTENCE_MEMORY_SIZE, persistenceMemory_size);
            if (persistenceMode == null) {
                persistenceMode = "Mem";
                settingsService.update(SettingsEnum.SHARE_CDC_PERSISTENCE_MODE, persistenceMode);
            }
        }

        String persistenceMongodb_collection = logSystemConfigDto.getPersistenceMongodb_collection();
        if (persistenceMongodb_collection != null) {
            settingsService.update(SettingsEnum.SHARE_CDC_PERSISTENCE_MONGODB_COLLECTION, persistenceMongodb_collection);
            if (persistenceMode == null) {
                persistenceMode = "MongoDB";
                settingsService.update(SettingsEnum.SHARE_CDC_PERSISTENCE_MODE, persistenceMode);
            }
        }

        String persistenceMongodb_uri_db = logSystemConfigDto.getPersistenceMongodb_uri_db();
        if (persistenceMongodb_uri_db != null) {
            if (!persistenceMongodb_uri_db.contains("******")) {
                settingsService.update(SettingsEnum.SHARE_CDC_PERSISTENCE_MONGODB_URI_DB, persistenceMongodb_uri_db);
                if (persistenceMode == null) {
                    persistenceMode = "MongoDB";
                    settingsService.update(SettingsEnum.SHARE_CDC_PERSISTENCE_MODE, persistenceMode);
                }
            }
        }

        String persistenceRocksdb_path = logSystemConfigDto.getPersistenceRocksdb_path();
        if (persistenceRocksdb_path != null) {
            settingsService.update(SettingsEnum.SHARE_CDC_PERSISTENCE_ROCKSDB_PATH, persistenceRocksdb_path);
            if (persistenceMode == null) {
                persistenceMode = "RocksDB";
                settingsService.update(SettingsEnum.SHARE_CDC_PERSISTENCE_MODE, persistenceMode);
            }
        }

        String share_cdc_ttl_day = logSystemConfigDto.getShare_cdc_ttl_day();
        if (share_cdc_ttl_day != null) {
            settingsService.update(SettingsEnum.SHARE_CDC_TTL_DAY, share_cdc_ttl_day);
        }


    }

    /**
     *  这里有一个大坑， 存在一个任务被删除了，但是子任务没有被删除，只是停止状态。
     * @return
     */
    public Boolean checkUpdateConfig(UserDetail user) {
        //查询所有的开启挖掘的任务跟，挖掘任务，是否都停止并且重置
        Criteria criteria = Criteria.where("shareCdcEnable").is(true).and("is_deleted").is(false).and("statuses").elemMatch(Criteria.where("status").ne(SubTaskDto.STATUS_EDIT));
        Query query = new Query(criteria);
        query.fields().include("shareCdcEnable", "is_deleted", "statuses");
        TaskDto taskDto = taskService.findOne(query);
        if (taskDto != null) {
            return false;
        }

        Criteria criteria1 = Criteria.where("is_deleted").is(false).and("dag.nodes").elemMatch(Criteria.where("type").is("logCollector"))
                .and("statuses").elemMatch(Criteria.where("status").ne(SubTaskDto.STATUS_EDIT));
        Query query1 = new Query(criteria1);
        query1.fields().include("shareCdcEnable", "is_deleted", "statuses");
        TaskDto taskDto1 = taskService.findOne(query1);
        return taskDto1 == null;
    }

    public Page<Map<String, String>> findTableNames(String taskId, int skip, int limit, UserDetail user) {
        Page<Map<String, String>> mapPage = new Page<>();
        mapPage.setTotal(0);
        mapPage.setItems(new ArrayList<>());
        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId), user);
        List<Node> sources = taskDto.getDag().getSources();
        if (sources != null) {
            Node node = sources.get(0);
            if (node instanceof LogCollectorNode) {
                List<String> tableNames = ((LogCollectorNode) node).getTableNames();
                mapPage.setTotal(tableNames.size());
                if (skip < tableNames.size()) {
                    int endIndex = skip + limit;
                    if (skip + limit > tableNames.size()) {
                        endIndex = tableNames.size();
                    }
                    List<String> subList = tableNames.subList(skip, endIndex);
                    List<Map<String, String>> tablename = subList.stream().map(s -> {
                        Map<String, String> table = new HashMap<>();
                        table.put("tablename", s);
                        return table;
                    }).collect(Collectors.toList());
                    mapPage.setItems(tablename);
                }
            }
        }

        return mapPage;
    }

    private Date getAttrsValues(String sourceId, String targetId, String type, Map<String, Object> attrs) {
        try {
            if (attrs == null) {
                return new Date();
            }
            Object syncProgress = attrs.get("syncProgress");
            if (syncProgress == null) {
                return new Date();
            }

            Map syncProgressMap = (Map) syncProgress;
            List<String> key = Lists.newArrayList(sourceId, targetId);

            Map valueMap = (Map) syncProgressMap.get(JsonUtil.toJsonUseJackson(key));
            if (valueMap == null) {
                return new Date();
            }

            Object o = valueMap.get(type);
            if (o == null) {
                return new Date();
            }

            return new Date((Long) o);

        } catch (Exception e) {
            return new Date();
        }
    }

    public Page<Map<String, String>> findCallTableNames(String taskId, String callSubId, int skip, int limit, UserDetail user) {
        Page<Map<String, String>> mapPage = new Page<>();
        mapPage.setTotal(0);
        mapPage.setItems(new ArrayList<>());
        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId), user);
        List<Node> sources = taskDto.getDag().getSources();
        List<String> tableNames = new ArrayList<>();
        List<String> connectionIds = new ArrayList<>();
        if (sources != null) {
            Node node = sources.get(0);
            if (node instanceof LogCollectorNode) {
                connectionIds = ((LogCollectorNode) node).getConnectionIds();
                tableNames = ((LogCollectorNode) node).getTableNames();
            }
        }

        Field field = new Field();
        field.put("dag", true);
        SubTaskDto subTaskDto = subTaskService.findById(new ObjectId(callSubId), field);
        if (subTaskDto != null) {
            List<String> nodeTableNames = new ArrayList<>();
            DAG dag = subTaskDto.getDag();
            List<Node> targets = dag.getTargets();
            List<Node> nodes = dag.getNodes();
            nodes.removeAll(targets);
            for (Node node : nodes) {
                if (node instanceof TableNode) {
                    if (connectionIds.contains(((TableNode) node).getConnectionId())) {
                        nodeTableNames.add(((TableNode) node).getTableName());
                    }

                } else if (node instanceof DatabaseNode) {
                    if (connectionIds.contains(((DatabaseNode) node).getConnectionId())) {
                        nodeTableNames = ((DatabaseNode) node).getSourceNodeTableNames();
                    }
                }
            }

            tableNames = nodeTableNames.stream().filter(tableNames::contains).collect(Collectors.toList());
            mapPage.setTotal(tableNames.size());
            if (skip < tableNames.size()) {
                int endIndex = skip + limit;
                if (skip + limit > tableNames.size()) {
                    endIndex = tableNames.size();
                }
                List<String> subList = tableNames.subList(skip, endIndex);
                List<Map<String, String>> tablename = subList.stream().map(s -> {
                    Map<String, String> table = new HashMap<>();
                    table.put("tablename", s);
                    return table;
                }).collect(Collectors.toList());
                mapPage.setItems(tablename);
            }
        }

        return mapPage;
    }
}
