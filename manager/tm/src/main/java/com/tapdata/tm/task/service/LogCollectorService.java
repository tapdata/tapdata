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
import com.tapdata.tm.commons.dag.logCollector.HazelCastImdgNode;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.DataNode;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.Status;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.bean.*;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.UUIDUtil;
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

    private final DataSourceService dataSourceService;
    private final WorkerService workerService;

    private final SettingsService settingsService;

    private DataSourceDefinitionService dataSourceDefinitionService;
    private MetadataInstancesService metadataInstancesService;

    public LogCollectorService(TaskService taskService, DataSourceService dataSourceService,
                               WorkerService workerService, SettingsService settingsService) {
        this.taskService = taskService;
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
    public List<TaskDto> findSyncTaskById(TaskDto taskDto, UserDetail user) {

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


        //考虑下要不要把task设置到subTaskDto里面
        return allDtos;
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
     * @param taskId
     * @return
     */
    public List<LogCollectorVo> findBySubTaskId(String taskId, UserDetail user) {
        ObjectId objectId = MongoUtils.toObjectId(taskId);
        //查询任务是否存在，任务不能是一个挖掘任务
        Criteria subTaskCriteria = Criteria.where("_id").is(objectId).and("shareCdcEnable").is(true).and("sync_type").ne("logCollector");
        Query subTaskQuery = new Query(subTaskCriteria);
        subTaskQuery.fields().include("dag");
        TaskDto taskDto = taskService.findOne(subTaskQuery, user);
        if (taskDto == null) {
            return new ArrayList<>();
        }

        return getLogCollectorVos(taskDto.getDag(), user);

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


        if (logCollectorEditVo.getStorageTime() != null) {
            Document document = new Document();
            document.put("storageTime", logCollectorEditVo.getStorageTime());
            Document _body = new Document();
            _body.put("$set", document);
            taskService.updateNode(taskDto.getId(), node.getId(), _body, user);
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

            List<TaskDto> taskDtos = findSyncTaskById(taskDto, user);

            LogCollectorVo logCollectorVo = convert(taskDto);
            LogCollectorDetailVo logCollectorDetailVo = new LogCollectorDetailVo();
            BeanUtils.copyProperties(logCollectorVo, logCollectorDetailVo);
            if (CollectionUtils.isNotEmpty(taskDtos)) {
                List<SyncTaskVo> syncTaskVos = convertSubTask(taskDtos, ((LogCollectorNode) node).getConnectionIds());
                logCollectorDetailVo.setTaskList(syncTaskVos);
            }

            TaskDto taskDto1 = taskDtos.get(0);
            logCollectorDetailVo.setTaskId(taskDto1.getId().toHexString());
            DAG dag1 = taskDto1.getDag();
            if (dag1 != null) {
                List<Edge> edges = dag1.getEdges();
                Edge edge = edges.get(0);
                Date eventTime = getAttrsValues(edge.getSource(), edge.getTarget(), "eventTime", taskDto1.getAttrs());
                logCollectorDetailVo.setLogTime(eventTime);
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


    private SyncTaskVo convert(TaskDto taskDto, List<String> connectionIds) {
        SyncTaskVo logCollectorVo = new SyncTaskVo();
        logCollectorVo.setId(taskDto.getId().toString());
        logCollectorVo.setName(taskDto.getName());
        logCollectorVo.setStatus(taskDto.getStatus());
        logCollectorVo.setSyncTimestamp(new Date());
        logCollectorVo.setSourceTimestamp(new Date());
        logCollectorVo.setSyncType(taskDto.getParentSyncType());

        DAG dag = taskDto.getDag();
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
                                    Date eventTime = getAttrsValues(node.getId(), targetNode.getId(), "eventTime", taskDto.getAttrs());
                                    Date sourceTime = getAttrsValues(node.getId(), targetNode.getId(), "sourceTime", taskDto.getAttrs());
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



    private List<SyncTaskVo> convertSubTask(List<TaskDto> taskDtos, List<String> connectionIds) {
        List<SyncTaskVo> syncTaskVos = new ArrayList<>();
        for (TaskDto taskDto : taskDtos) {
            syncTaskVos.add(convert(taskDto, connectionIds));
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
//        if (StringUtils.isNotBlank(connectionName)) {
//            connectionName = connectionName.trim();
//            Criteria nameCriteria = Criteria.where("name").is(connectionName);
//            Query query = new Query(nameCriteria);
//            query.fields().include("config", "database_type");
//            DataSourceConnectionDto connectionDto = dataSourceService.findOne(query, user);
//            Map<String, Object> config = connectionDto.getConfig();
//            if (connectionDto.getDatabase_type().toLowerCase(Locale.ROOT).contains("mongo") && config.get("uri") != null) {
//                persistenceMongodb_uri_db = (String) config.get("uri");
//            }
//        }
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
        Criteria criteria = Criteria.where("shareCdcEnable").is(true).and("is_deleted").is(false).and("statuses").elemMatch(Criteria.where("status").ne(TaskDto.STATUS_EDIT));
        Query query = new Query(criteria);
        query.fields().include("shareCdcEnable", "is_deleted", "statuses");
        TaskDto taskDto = taskService.findOne(query);
        if (taskDto != null) {
            return false;
        }

        Criteria criteria1 = Criteria.where("is_deleted").is(false).and("dag.nodes").elemMatch(Criteria.where("type").is("logCollector"))
                .and("statuses").elemMatch(Criteria.where("status").ne(TaskDto.STATUS_EDIT));
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
        TaskDto subTaskDto = taskService.findById(new ObjectId(callSubId), field);
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

    /**
     * 日志挖掘
     *
     * @param user
     * @param oldTaskDto
     */
    public void logCollector(UserDetail user, TaskDto oldTaskDto) {

        if (!oldTaskDto.getShareCdcEnable()) {
            //任务没有开启共享挖掘
            return;
        }

        //获取DAG所有的源节点并分组
        DAG dag = oldTaskDto.getDag();

        if (!TaskDto.SYNC_TYPE_MIGRATE.equals(oldTaskDto.getSyncType()) && !TaskDto.SYNC_TYPE_SYNC.equals(oldTaskDto.getSyncType())) {
            //日志挖掘类型的任务，不需要进行日志挖掘
            return;
        }

        //只有全量+增量或者增量的时候可以使用挖掘任务
        if (ParentTaskDto.TYPE_INITIAL_SYNC.equals(oldTaskDto.getType())) {
            //只是全量任务不用开启共享日志挖掘
            return;
        }

        List<Node> allNodes = dag.getNodes();
        List<Node> targets = dag.getTargets();
        Set<String> sinkIds = targets.stream().map(Node::getId).collect(Collectors.toSet());

        Map<ObjectId, List<Node>> group = allNodes.stream().
                filter(Node::isDataNode)
                .filter(n -> !sinkIds.contains(n.getId()))
                .collect(Collectors.groupingBy(s -> MongoUtils.toObjectId(((DataParentNode<?>) s).getConnectionId())));
        //查询获取所有源的数据源连接
        Criteria criteria = Criteria.where("_id").in(group.keySet());
        Query query = new Query(criteria);
        query.fields().include("_id", "shareCdcEnable", "shareCdcTTL", "uniqueName", "database_type", "name");
        List<DataSourceConnectionDto> dataSourceDtos = dataSourceService.findAllDto(query, user);

        //根据数据源连接
        Set<String> sourceUniqSet = new HashSet<>();
        List<DataSourceConnectionDto> _dataSourceDtos = new ArrayList<>();
        List<DataSourceConnectionDto> createLogCollects = new ArrayList<>();
        for (DataSourceConnectionDto dataSourceDto : dataSourceDtos) {
            if (dataSourceDto.getShareCdcEnable() == null || !dataSourceDto.getShareCdcEnable()) {
                continue;
            }

            String uniqueName = dataSourceDto.getUniqueName();

            if (sourceUniqSet.contains(uniqueName)) {
                _dataSourceDtos.add(dataSourceDto);
            }

            if (StringUtils.isBlank(uniqueName) || !sourceUniqSet.contains(uniqueName)) {
                createLogCollects.add(dataSourceDto);
                sourceUniqSet.add(uniqueName);
            }
        }

        _dataSourceDtos.addAll(createLogCollects);
        dataSourceDtos = _dataSourceDtos;

        Map<String, List<DataSourceConnectionDto>> datasourceMap = dataSourceDtos.stream().collect(Collectors.groupingBy(d -> StringUtils.isBlank(d.getUniqueName()) ? d.getId().toHexString() : d.getUniqueName()));


        //不同类型数据源的id缓存
        Map<String, List<DataSourceConnectionDto>> dataSourceCacheByType = new HashMap<>();

        //数据源id对应创建的挖掘任务id
        Map<String, String> newLogCollectorMap = new HashMap<>();

        datasourceMap.forEach((k, v) -> {


            //获取需要日志挖掘的表名
            List<String> tableNames = new ArrayList<>();
            for (DataSourceConnectionDto d : v) {
                List<Node> nodes = group.get(d.getId());
                for (Node node : nodes) {
                    if (node instanceof TableNode) {

                        tableNames.add(((TableNode) node).getTableName());

                    } else if (node instanceof DatabaseNode) {
                        tableNames = ((DatabaseNode) node).getSourceNodeTableNames();
                    }
                }
            }

            //查询是否存在相同的日志挖掘任务，存在，并且表也存在，则不处理
            //根据unique name查询，或者根据id查询
            DataSourceConnectionDto dataSource = v.get(0);
            List<String> ids = new ArrayList<>();

            //如果没有uniqname,则唯一键采用的id，所以不会存在相似的数据源
            if (StringUtils.isBlank(dataSource.getUniqueName())) {
                ids.add(dataSource.getId().toHexString());

            } else {

                List<DataSourceConnectionDto> cache = dataSourceCacheByType.get(dataSource.getDatabase_type());
                if (CollectionUtils.isEmpty(cache)) {
                    Criteria criteria1 = Criteria.where("database_type").is(dataSource.getDatabase_type());
                    Query query1 = new Query(criteria1);
                    query1.fields().include("_id", "uniqueName");
                    cache = dataSourceService.findAllDto(query1, user);
                    dataSourceCacheByType.put(dataSource.getDatabase_type(), cache);

                }


                ids = cache.stream().filter(c -> dataSource.getUniqueName().equals(c.getUniqueName())).map(d -> d.getId().toHexString()).collect(Collectors.toList());
            }


            Criteria criteria1 = Criteria.where("is_deleted").is(false).and("dag.nodes").elemMatch(Criteria.where("type").is("logCollector").and("connectionIds").elemMatch(Criteria.where("$in").is(ids)));
            Query query1 = new Query(criteria1);
            query1.fields().include("dag", "statuses");
            List<String> connectionIds = v.stream().map(d -> d.getId().toHexString()).collect(Collectors.toList());
            TaskDto oldLogCollectorTask = taskService.findOne(query1, user);
            if (oldLogCollectorTask != null) {
                List<Node> sources1 = oldLogCollectorTask.getDag().getSources();
                LogCollectorNode logCollectorNode = (LogCollectorNode) sources1.get(0);
                List<String> oldTableNames = logCollectorNode.getTableNames();
                List<TaskDto> id1s = new ArrayList<>();//findByTaskId(oldLogCollectorTask.getId(), user, "_id");
                TaskDto TaskDto = id1s.get(0);
                for (String id : ids) {
                    newLogCollectorMap.put(id, TaskDto.getId().toHexString());
                }

                List<String> oldConnectionIds = logCollectorNode.getConnectionIds();

                boolean updateConnectionId = false;
                for (String connectionId : connectionIds) {
                    if (!oldConnectionIds.contains(connectionId)) {
                        oldConnectionIds.add(connectionId);
                        updateConnectionId = true;
                    }
                }

                if (CollectionUtils.isNotEmpty(oldTableNames) && oldTableNames.containsAll(tableNames)) {
                    //检查状态，如果状态不是启动的，需要启动起来
                    List<Status> statuses = oldLogCollectorTask.getStatuses();
                    if (updateConnectionId) {
                        taskService.confirmById(oldLogCollectorTask, user, true);
                    }
                    if (CollectionUtils.isNotEmpty(statuses)) {
                        Status subStatus = statuses.get(0);
                        if (TaskDto.STATUS_RUNNING.equals(subStatus.getStatus())) {
                            return;
                        }
                    }


                    taskService.start(oldLogCollectorTask.getId(), user);
                    return;
                }

                tableNames.addAll(oldTableNames);
                tableNames = tableNames.stream().distinct().collect(Collectors.toList());
                logCollectorNode.setTableNames(tableNames);
                taskService.confirmById(oldLogCollectorTask, user, true);
                updateLogCollectorMap(oldTaskDto.getId(), newLogCollectorMap, user);
                //这个stop是异步的， 需要重启，重启的逻辑是通过定时任务跑的
                taskService.stop(oldLogCollectorTask.getId(), user, false, true);
                return;
            }


            LogCollectorNode logCollectorNode = new LogCollectorNode();
            logCollectorNode.setId(UUIDUtil.getUUID());
            logCollectorNode.setConnectionIds(connectionIds);
            logCollectorNode.setDatabaseType(v.get(0).getDatabase_type());
            logCollectorNode.setName(UUIDUtil.getUUID());


            logCollectorNode.setTableNames(tableNames);
            logCollectorNode.setSelectType(LogCollectorNode.SELECT_TYPE_RESERVATION);

            HazelCastImdgNode hazelCastImdgNode = new HazelCastImdgNode();
            hazelCastImdgNode.setId(UUIDUtil.getUUID());
            hazelCastImdgNode.setName(hazelCastImdgNode.getId());

            List<Node> nodes = Lists.newArrayList(logCollectorNode, hazelCastImdgNode);

            Edge edge = new Edge(logCollectorNode.getId(), hazelCastImdgNode.getId());
            List<Edge> edges = Lists.newArrayList(edge);
            Dag dag1 = new Dag(edges, nodes);
            DAG build = DAG.build(dag1);
            TaskDto taskDto = new TaskDto();
            taskDto.setName("来自" + dataSource.getName() + "的共享挖掘任务");
            taskDto.setDag(build);
            taskDto.setType("cdc");
            taskDto.setSyncType("logCollector");
            taskDto = taskService.create(taskDto, user);
            taskDto = taskService.confirmById(taskDto, user, true);

            //保存新增挖掘任务id到子任务中
            for (String id : ids) {
                Map<String, Object> attrs = taskDto.getAttrs();
                if (attrs != null) {
                    newLogCollectorMap.put(id, (String) attrs.get(TaskService.LOG_COLLECTOR_SAVE_ID));
                }
            }

            taskService.start(taskDto.getId(), user);
        });

        updateLogCollectorMap(oldTaskDto.getId(), newLogCollectorMap, user);
    }

//    public void startConnHeartbeat(UserDetail user, TaskDto oldTaskDto) {
//
//        if (!TaskDto.SYNC_TYPE_MIGRATE.equals(oldTaskDto.getSyncType()) && !TaskDto.SYNC_TYPE_SYNC.equals(oldTaskDto.getSyncType())) {
//            //日志挖掘类型的任务，不需要进行日志挖掘
//            return;
//        }
//
//        //只有全量+增量或者增量的时候可以使用挖掘任务
//        if (ParentTaskDto.TYPE_INITIAL_SYNC.equals(oldTaskDto.getType())) {
//            //只是全量任务不用开启共享日志挖掘
//            return;
//        }
//
//        List<DataSourceConnectionDto> dataSourceDtos = getConnectionByDag(user, oldTaskDto.getDag());
//
//        //不同类型数据源的id缓存
//        Map<String, List<DataSourceConnectionDto>> dataSourceCacheByType = new HashMap<>();
//
//
//        DataSourceConnectionDto heartbeatConnection = null;
//        String heartbeatTable = "_tapdata_heartbeat_table";
//        for (DataSourceConnectionDto dataSource : dataSourceDtos) {
//            List<String> connectionIds = getConnectionIds(user, dataSourceCacheByType, dataSource);
//            TaskDto oldConnHeartbeatTask = getHeartbeatTaskDto(connectionIds, user);
//            if (oldConnHeartbeatTask != null) {
//                HashSet<String> heartbeatTasks = oldConnHeartbeatTask.getHeartbeatTasks();
//                List<TaskDto> TaskDtos = taskService.findByTaskId(oldTaskDto.getId(), user, "_id");
//                List<String> collect = TaskDtos.stream().map(s -> s.getId().toHexString()).collect(Collectors.toList());
//                heartbeatTasks.addAll(collect);
//                taskService.update(new Query(Criteria.where("_id").is(oldConnHeartbeatTask.getId())), Update.update("heartbeatTasks", heartbeatTasks), user);
//                if (TaskDto.STATUS_RUNNING.equals(oldConnHeartbeatTask.getStatus())) {
//                    taskService.start(oldConnHeartbeatTask.getId(), user);
//                }
//                return;
//            }
//
//
//            //循环中只需要获取一次dummy源跟打点模型表
//            if (heartbeatConnection == null) {
//                String databaseType = "dummy";
//                String mode = "ConnHeartbeat";
//
//                Query query3 = new Query(Criteria.where("pdkId").is(databaseType));
//                query3.fields().include("pdkHash", "type");
//                DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.findOne(query3);
//                //获取打点的Dummy数据源
//                Query query2 = new Query(Criteria.where("database_type").is(databaseType)
//                        .and("config.mode").is(mode)
//                );
//                heartbeatConnection = dataSourceService.findOne(query2, user);
//
//                boolean addDummy = false;
//                if (heartbeatConnection == null) {
//                    heartbeatConnection = new DataSourceConnectionDto();
//                    heartbeatConnection.setName("tapdata_heartbeat_dummy_connection");
//                    LinkedHashMap<String, Object> config = new LinkedHashMap<>();
//                    config.put("mode", mode);
//
//                    heartbeatConnection.setConfig(config);
//                    heartbeatConnection.setConnection_type("source");
//                    heartbeatConnection.setPdkType("pdk");
//                    heartbeatConnection.setRetry(0);
//                    heartbeatConnection.setStatus("testing");
//                    heartbeatConnection.setShareCdcEnable(false);
//                    heartbeatConnection.setDatabase_type(definitionDto.getType());
//                    heartbeatConnection.setPdkHash(definitionDto.getPdkHash());
//                    heartbeatConnection = dataSourceService.add(heartbeatConnection, user);
//                    addDummy = true;
//
//                }
//
//                String qualifiedName = MetaDataBuilderUtils.generateQualifiedName("table", heartbeatConnection, "heartbeatTable");
//                MetadataInstancesDto metadata = metadataInstancesService.findByQualifiedNameNotDelete(qualifiedName, user, "_id");
//                if (metadata == null) {
//                    if (!addDummy) {
//                        //新增数据源的时候 我自动加载模型
//                        dataSourceService.sendTestConnection(heartbeatConnection, true, true, user);
//                    }
//
//                    for (int i = 0; i < 8; i++) {
//                        if (metadataInstancesService.findByQualifiedNameNotDelete(qualifiedName, user, "_id") == null) {
//                            try {
//                                Thread.sleep(500 * i);
//                            } catch (InterruptedException e) {
//                                throw new BizException("SystemError");
//                            }
//                        }
//
//                    }
//                }
//            }
//
//
//            TableNode sourceNode = new TableNode();
//            sourceNode.setId(UUID.randomUUID().toString());
//            sourceNode.setTableName(heartbeatTable);
//            sourceNode.setConnectionId(heartbeatConnection.getId().toHexString());
//            sourceNode.setDatabaseType(heartbeatConnection.getDatabase_type());
//            sourceNode.setName(heartbeatTable);
//            TableNode targetNode = new TableNode();
//            targetNode.setId(UUID.randomUUID().toString());
//            sourceNode.setTableName(heartbeatTable);
//            sourceNode.setConnectionId(dataSource.getId().toHexString());
//            sourceNode.setDatabaseType(dataSource.getDatabase_type());
//            sourceNode.setName(heartbeatTable);
//
//            List<Node> nodes = Lists.newArrayList(sourceNode, targetNode);
//
//            Edge edge = new Edge(sourceNode.getId(), targetNode.getId());
//            List<Edge> edges = Lists.newArrayList(edge);
//            Dag dag1 = new Dag(edges, nodes);
//            DAG build = DAG.build(dag1);
//            TaskDto taskDto = new TaskDto();
//            taskDto.setName("来自" + dataSource.getName() + "的打点任务");
//            taskDto.setDag(build);
//            taskDto.setType("cdc");
//            taskDto.setSyncType(TaskDto.SYNC_TYPE_CONN_HEARTBEAT);
//            List<TaskDto> TaskDtos = findByTaskId(oldTaskDto.getId(), user, "_id");
//            HashSet<String> heartbeatTasks = TaskDtos.stream().map(s -> s.getId().toHexString()).collect(Collectors.toCollection(HashSet::new));
//            taskDto.setHeartbeatTasks(heartbeatTasks);
//            taskDto = taskService.create(taskDto, user);
//            taskDto = taskService.confirmById(taskDto, user, true);
//
//            taskService.start(taskDto.getId(), user);
//        }
//
//    }


    private List<String> getConnectionIds(UserDetail user, Map<String, List<DataSourceConnectionDto>> dataSourceCacheByType, DataSourceConnectionDto dataSource) {
        List<String> ids = new ArrayList<>();

        //如果没有uniqname,则唯一键采用的id，所以不会存在相似的数据源
        if (StringUtils.isBlank(dataSource.getUniqueName())) {
            ids.add(dataSource.getId().toHexString());

        } else {

            List<DataSourceConnectionDto> cache = dataSourceCacheByType.get(dataSource.getDatabase_type());
            if (CollectionUtils.isEmpty(cache)) {
                Criteria criteria1 = Criteria.where("database_type").is(dataSource.getDatabase_type());
                Query query1 = new Query(criteria1);
                query1.fields().include("_id", "uniqueName");
                cache = dataSourceService.findAllDto(query1, user);
                dataSourceCacheByType.put(dataSource.getDatabase_type(), cache);

            }


            ids = cache.stream().filter(c -> dataSource.getUniqueName().equals(c.getUniqueName())).map(d -> d.getId().toHexString()).collect(Collectors.toList());
        }
        return ids;
    }


    private TaskDto getHeartbeatTaskDto(List<String> ids, UserDetail user) {



        Criteria criteria1 = Criteria.where("is_deleted").is(false).and("syncType").is(TaskDto.SYNC_TYPE_CONN_HEARTBEAT).and("dag.nodes").elemMatch(Criteria.where("connectionId").in(ids));
        Query query1 = new Query(criteria1);
        query1.fields().include("dag", "status", "heartbeatTasks");
        TaskDto oldConnHeartbeatTask = taskService.findOne(query1, user);
        return oldConnHeartbeatTask;
    }


//    public void endConnHeartbeat(UserDetail user, TaskDto TaskDto) {
//        TaskDto parentTask = TaskDto.getParentTask();
//        if (!TaskDto.SYNC_TYPE_MIGRATE.equals(parentTask.getSyncType()) && !TaskDto.SYNC_TYPE_SYNC.equals(parentTask.getSyncType())) {
//            //日志挖掘类型跟打点类型的任务，不需要进行日志挖掘
//            return;
//        }
//
//        //只有全量+增量或者增量的时候可以使用挖掘，打点任务
//        if (ParentTaskDto.TYPE_INITIAL_SYNC.equals(parentTask.getType())) {
//            //只是全量任务不用开启
//            return;
//        }
//
//        List<DataSourceConnectionDto> dataSourceDtos = getConnectionByDag(user, TaskDto.getDag());
//
//
//        //不同类型数据源的id缓存
//        Map<String, List<DataSourceConnectionDto>> dataSourceCacheByType = new HashMap<>();
//
//        for (DataSourceConnectionDto dataSource : dataSourceDtos) {
//            List<String> connectionIds = getConnectionIds(user, dataSourceCacheByType, dataSource);
//            //获取是否存在这些connectionIds作为源的任务。
//
//            TaskDto oldConnHeartbeatTask = getHeartbeatTaskDto(connectionIds, user);
//            HashSet<String> heartbeatTasks = oldConnHeartbeatTask.getHeartbeatTasks();
//            heartbeatTasks.remove(TaskDto.getId().toHexString());
//            taskService.update(new Query(Criteria.where("_id").is(oldConnHeartbeatTask.getId())), Update.update("heartbeatTasks", heartbeatTasks), user);
//            if (heartbeatTasks.size() == 0) {
//                taskService.stop(oldConnHeartbeatTask.getId(), user, false);
//            }
//        }
//    }

    private List<DataSourceConnectionDto> getConnectionByDag(UserDetail user, DAG dag) {
        List<Node> sources = dag.getSources();

        Set<String> connectionIds = sources.stream().map(n -> ((DataParentNode) n).getConnectionId()).collect(Collectors.toSet());
        //查询获取所有源的数据源连接
        Criteria criteria = Criteria.where("_id").in(connectionIds);
        Query query = new Query(criteria);
        query.fields().include("_id", "uniqueName", "database_type", "name");
        List<DataSourceConnectionDto> dataSourceDtos = dataSourceService.findAllDto(query, user);
        return dataSourceDtos;
    }

    //由于调用这个方法的都是异步方法，不存在返回时长问题，所以这里直接使用sleep等待
    private void delayCheckSubTaskStatus(ObjectId taskId, String subStatus, UserDetail user) {
        Criteria criteria = Criteria.where("parentId").is(taskId);
        Query query = new Query(criteria);
        query.fields().include("status");
        level1:
        //如果16秒钟还没有等到想要的结果，就不再继续等待了
        for (int i = 0; i < 100; i++) {
            int ms = (1 << i) * 1000;
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                log.warn("thread sleep interrupt exception, e = {}", e.getMessage());
                return;
            }

            List<TaskDto> allDto = taskService.findAllDto(query, user);
            for (TaskDto TaskDto : allDto) {
                if (!subStatus.equals(TaskDto.getStatus())) {
                    break;
                }
                break level1;
            }
        }
    }


    //将启动的挖掘任务id更新到任务中去
    private void updateLogCollectorMap(ObjectId taskId, Map<String, String> newLogCollectorMap, UserDetail user) {
        List<TaskDto> TaskDtos = taskService.findByTaskId(taskId, "dag", "_id");
        if (CollectionUtils.isEmpty(TaskDtos)) {
            return;
        }

        if (newLogCollectorMap == null || newLogCollectorMap.isEmpty()) {

            return;
        }

        for (TaskDto TaskDto : TaskDtos) {
            DAG dag = TaskDto.getDag();
            List<Node> sources = dag.getSources();
            Map<String, String> shareCdcTaskId = TaskDto.getShareCdcTaskId();
            if (shareCdcTaskId == null) {
                shareCdcTaskId = new HashMap<>();
                TaskDto.setShareCdcTaskId(shareCdcTaskId);
            }

            for (Node source : sources) {
                if (source instanceof DataParentNode) {
                    String id = ((DataParentNode<?>) source).getConnectionId();
                    if (newLogCollectorMap.get(id) != null) {
                        shareCdcTaskId.put(id, newLogCollectorMap.get(id));
                    }
                }
            }

            Update update = new Update();
            update.set("shareCdcTaskId", shareCdcTaskId);
            taskService.updateById(TaskDto.getId(), update, user);
        }


    }
}
