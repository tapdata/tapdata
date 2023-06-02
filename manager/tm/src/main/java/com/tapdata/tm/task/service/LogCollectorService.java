package com.tapdata.tm.task.service;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Maps;
import com.mongodb.ConnectionString;
import com.tapdata.tm.Settings.constant.SettingsEnum;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.HazelCastImdgNode;
import com.tapdata.tm.commons.dag.logCollector.LogCollecotrConnConfig;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.DataNode;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.commons.task.dto.ParentTaskDto;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.util.ConnHeartbeatUtils;
import com.tapdata.tm.commons.util.CreateTypeEnum;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.externalStorage.service.ExternalStorageService;
import com.tapdata.tm.externalStorage.vo.ExternalStorageVo;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.shareCdcTableMetrics.ShareCdcTableMetricsDto;
import com.tapdata.tm.shareCdcTableMetrics.service.ShareCdcTableMetricsService;
import com.tapdata.tm.task.bean.*;
import com.tapdata.tm.task.param.TableLogCollectorParam;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.FunctionUtils;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.UUIDUtil;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.ProjectionOperation;
import org.springframework.data.mongodb.core.aggregation.UnwindOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;

import javax.annotation.Nullable;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    @Autowired
    private DataSourceDefinitionService dataSourceDefinitionService;
    @Autowired
    private MetadataInstancesService metadataInstancesService;
    @Autowired
    private MonitoringLogsService monitoringLogsService;
    @Autowired
    private ExternalStorageService externalStorageService;
    @Autowired
    private ShareCdcTableMetricsService shareCdcTableMetricsService;
    @Autowired
    private UserService userService;

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
        query.fields().include("status", "name", "createTime", "dag", "statuses", "attrs", "syncPoints");

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
        taskQuery.fields().include("_id", "syncType", "name", "status", "attrs");
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
        query.fields().include("status", "name", "createTime", "dag", "attrs");
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

        if (CollectionUtils.isNotEmpty(logCollectorEditVo.getSyncPoints())) {
            update.set("syncPoints", logCollectorEditVo.getSyncPoints());
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
                TaskDto taskDto1 = taskDtos.get(0);
                logCollectorDetailVo.setTaskId(taskDto1.getId().toHexString());
                DAG dag1 = taskDto1.getDag();
                if (dag1 != null) {
                    List<Edge> edges = dag1.getEdges();
                    Edge edge = edges.get(0);
                    Date eventTime = getAttrsValues(edge.getSource(), edge.getTarget(), "eventTime", taskDto1.getAttrs());
                    logCollectorDetailVo.setLogTime(eventTime);
                }
            }

            findAndFillExternalStorageIntoDetail(user, node, logCollectorDetailVo);

            return logCollectorDetailVo;
        } else {
            return null;
        }
    }

    private void findAndFillExternalStorageIntoDetail(UserDetail user, Node node, LogCollectorDetailVo logCollectorDetailVo) {
        String externalStorageId = node.getExternalStorageId();
        if (StringUtils.isBlank(externalStorageId)) {
            List<String> connectionIds = ((LogCollectorNode) node).getConnectionIds();
            String connectionId = connectionIds.get(0);
            if (StringUtils.isNotBlank(connectionId)) {
                Field field = new Field();
                field.put("shareCDCExternalStorageId", true);
                DataSourceConnectionDto connectionDto = dataSourceService.findById(new ObjectId(connectionId), field);
                if (null != connectionDto) {
                    externalStorageId = connectionDto.getShareCDCExternalStorageId();
                }
            }
        }
        ExternalStorageDto externalStorageDto;
        if (StringUtils.isBlank(externalStorageId)) {
            externalStorageDto = externalStorageService.findOne(Query.query(Criteria.where("defaultStorage").is(true)));
        } else {
            externalStorageDto = externalStorageService.findById(new ObjectId(externalStorageId), user);
        }
        if (null != externalStorageDto) {
            ExternalStorageVo externalStorageVo = new ExternalStorageVo();
            BeanUtils.copyProperties(externalStorageDto, externalStorageVo);
            logCollectorDetailVo.setExternalStorage(externalStorageVo);
        }
    }


    private LogCollectorVo convert(TaskDto taskDto) {
        LogCollectorVo logCollectorVo = new LogCollectorVo();
        logCollectorVo.setName(taskDto.getName());
        logCollectorVo.setId(taskDto.getId().toString());
        logCollectorVo.setCreateTime(taskDto.getCreateAt());
        logCollectorVo.setStatus(taskDto.getStatus());
        logCollectorVo.setStatuses(taskDto.getStatuses());
        logCollectorVo.setDag(taskDto.getDag());
        logCollectorVo.setSyncPoints(taskDto.getSyncPoints());
//        List<TaskDto.SyncPoint> syncPoints = taskDto.getSyncPoints();
//        if (CollectionUtils.isNotEmpty(syncPoints)) {
//            TaskDto.SyncPoint syncPoint = syncPoints.get(0);
//            logCollectorVo.setSyncTimePoint(syncPoint.getPointType());
//            logCollectorVo.setSyncTime(new Date(syncPoint.getDateTime()));
//            logCollectorVo.setSyncTimeZone(syncPoint.getTimeZone());
//        }



        if (taskDto.getDag() != null) {
            List<Node> sources = taskDto.getDag().getSources();
            List<Node> targets = taskDto.getDag().getTargets();

            if (CollectionUtils.isNotEmpty(sources) && CollectionUtils.isNotEmpty(targets)) {
                Node node = sources.get(0);
                Node targetNode = targets.get(0);
                Date eventTime = getAttrsValues(node.getId(), targetNode.getId(), "eventTime", taskDto.getAttrs());
                Date sourceTime = getAttrsValues(node.getId(), targetNode.getId(), "sourceTime", taskDto.getAttrs());
                logCollectorVo.setLogTime(eventTime);
                if (null != eventTime && null != sourceTime) {
                    long delayTime = sourceTime.getTime() - eventTime.getTime();
                    logCollectorVo.setDelayTime(delayTime > 0 ? delayTime : 0);
                } else {
                    logCollectorVo.setDelayTime(-1L);
                }

                if (node instanceof LogCollectorNode) {
                    LogCollectorNode logCollectorNode = (LogCollectorNode) node;
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

        return logCollectorVo;
    }


    private SyncTaskVo convert(TaskDto taskDto, List<String> connectionIds) {
        SyncTaskVo logCollectorVo = new SyncTaskVo();
        logCollectorVo.setId(taskDto.getId().toString());
        logCollectorVo.setName(taskDto.getName());
        logCollectorVo.setStatus(taskDto.getStatus());
        logCollectorVo.setSyncTimestamp(new Date());
        logCollectorVo.setSourceTimestamp(new Date());
        logCollectorVo.setSyncType(taskDto.getSyncType());

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
                log.error("Parse connection string failed ({}), {}", value2, e.getMessage());
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
            throw new BizException("LogCollect.ConfigUpdateError");
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
            try {
                ConnectionString connectionString = new ConnectionString(persistenceMongodb_uri_db);
                if (persistenceMongodb_uri_db.contains("******")) {
                    String old = SettingsEnum.SHARE_CDC_PERSISTENCE_MONGODB_URI_DB.getValue();
                    if (StringUtils.isBlank(old)) {
                        throw new BizException("LogCollect.UriInvalid");
                    }

                    if (!persistenceMongodb_uri_db.equals(old)) {
                        ConnectionString connectionStringOld = new ConnectionString(old);
                        if (connectionStringOld.getHosts().equals(connectionString.getHosts())) {
                            if (connectionStringOld.getPassword() == null) {
                                throw new BizException("LogCollect.UriInvalid");
                            }
                            persistenceMongodb_uri_db = persistenceMongodb_uri_db.replace("******", new String(connectionStringOld.getPassword()));
                        } else {
                            throw new BizException("LogCollect.UriInvalid");
                        }
                    }
                }

                settingsService.update(SettingsEnum.SHARE_CDC_PERSISTENCE_MONGODB_URI_DB, persistenceMongodb_uri_db);
                if (persistenceMode == null) {
                    persistenceMode = "MongoDB";
                    settingsService.update(SettingsEnum.SHARE_CDC_PERSISTENCE_MODE, persistenceMode);
                }
            } catch (Exception e) {
                throw new BizException("LogCollect.UriInvalid");
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
        Criteria criteria = Criteria.where("shareCdcEnable").is(true).and("is_deleted").ne(true).and("status").nin(TaskDto.STATUS_EDIT, TaskDto.STATUS_WAIT_START);
        Query query = new Query(criteria);
        query.fields().include("shareCdcEnable", "is_deleted", "status");
        TaskDto taskDto = taskService.findOne(query);
        if (taskDto != null) {
            return false;
        }

        Criteria criteria1 = Criteria.where("is_deleted").ne(true).and("dag.nodes").elemMatch(Criteria.where("type").is("logCollector"))
                .and("status").nin(TaskDto.STATUS_EDIT, TaskDto.STATUS_WAIT_START);
        Query query1 = new Query(criteria1);
        query1.fields().include("shareCdcEnable", "is_deleted", "status");
        TaskDto taskDto1 = taskService.findOne(query1);
        return taskDto1 == null;
    }


    /**
     *  这里有一个大坑， 存在一个任务被删除了，但是子任务没有被删除，只是停止状态。
     * @return
     */
    public Boolean checkUpdateConfig(String connectionId, UserDetail user) {
        //查询挖掘任务，是否都停止并且重置
        Criteria criteria1 = Criteria.where("is_deleted").ne(true).and("dag.nodes").elemMatch(Criteria.where("type").is("logCollector"))
                .and("status").nin(TaskDto.STATUS_EDIT, TaskDto.STATUS_WAIT_START).and("dag.nodes.connectionIds").is(connectionId);
        Query query1 = new Query(criteria1);
        query1.fields().include("shareCdcEnable", "is_deleted", "status");
        TaskDto taskDto1 = taskService.findOne(query1, user);
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

    @Nullable
    private Date getAttrsValues(String sourceId, String targetId, String type, Map<String, Object> attrs) {
        try {
            if (attrs == null) {
                return null;
            }
            Object syncProgress = attrs.get("syncProgress");
            if (syncProgress == null) {
                return null;
            }

            Map syncProgressMap = (Map) syncProgress;
            List<String> key = Lists.newArrayList(sourceId, targetId);

            String valueMapString = (String) syncProgressMap.get(JsonUtil.toJsonUseJackson(key));
            LinkedHashMap valueMap = JsonUtil.parseJson(valueMapString, LinkedHashMap.class);
            if (valueMap == null) {
                return null;
            }

            Object o = valueMap.get(type);
            if (o == null) {
                return null;
            }

            return new Date(((Double) o).longValue());

        } catch (Exception e) {
            return null;
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
        query.fields().include("_id", "shareCdcEnable", "shareCdcTTL", "uniqueName", "multiConnectionInstanceId", "database_type", "name", "pdkHash","shareCDCExternalStorageId");
        List<DataSourceConnectionDto> dataSourceDtos = dataSourceService.findAllDto(query, user);

        //根据数据源连接
        Set<String> sourceUniqSet = new HashSet<>();
        List<DataSourceConnectionDto> _dataSourceDtos = new ArrayList<>();
        List<DataSourceConnectionDto> createLogCollects = new ArrayList<>();
        for (DataSourceConnectionDto dataSourceDto : dataSourceDtos) {
            if (dataSourceDto.getShareCdcEnable() == null || !dataSourceDto.getShareCdcEnable()) {
                continue;
            }

            String uniqueName = dataSourceDto.getMultiConnectionInstanceId();

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

        Map<String, List<DataSourceConnectionDto>> datasourceMap = dataSourceDtos.stream().collect(Collectors.groupingBy(d -> StringUtils.isBlank(d.getMultiConnectionInstanceId()) ? d.getId().toHexString() : d.getMultiConnectionInstanceId()));

        //不同类型数据源的id缓存
        Map<String, List<DataSourceConnectionDto>> dataSourceCacheByType = new HashMap<>();

        //数据源id对应创建的挖掘任务id
        Map<String, String> newLogCollectorMap = new HashMap<>();

        datasourceMap.forEach((k, v) -> {
            //获取需要日志挖掘的表名
            //Set<String> tableSet = new HashSet<>();

            Map<String, List<String>> tableMaps = new HashMap<>();
            Set<String> finalTableNames = new HashSet<>();

            for (DataSourceConnectionDto d : v) {
                Set<String> tableSet = new HashSet<>();
                List<Node> nodes = group.get(d.getId());
                for (Node node : nodes) {
                    if (node instanceof TableNode) {
                        tableSet.add(((TableNode) node).getTableName());
                    } else if (node instanceof DatabaseNode) {
                        tableSet.addAll(((DatabaseNode) node).getSourceNodeTableNames());
                    }
                }
                List<String> tableNames = new ArrayList<>(tableSet);
                finalTableNames.addAll(tableSet);
                tableMaps.put(d.getId().toHexString(), tableNames);

            }


            //查询是否存在相同的日志挖掘任务，存在，并且表也存在，则不处理
            //根据unique name查询，或者根据id查询
            DataSourceConnectionDto dataSource = v.get(0);
            List<String> ids = new ArrayList<>();

            //如果没有uniqname,则唯一键采用的id，所以不会存在相似的数据源
            if (StringUtils.isBlank(dataSource.getMultiConnectionInstanceId())) {
                ids.add(dataSource.getId().toHexString());
            } else {
                List<DataSourceConnectionDto> cache = dataSourceCacheByType.get(dataSource.getDatabase_type());
                if (CollectionUtils.isEmpty(cache)) {
                    Criteria criteria1 = Criteria.where("database_type").is(dataSource.getDatabase_type());
                    Query query1 = new Query(criteria1);
                    query1.fields().include("_id", "uniqueName", "multiConnectionInstanceId");
                    cache = dataSourceService.findAllDto(query1, user);
                    dataSourceCacheByType.put(dataSource.getDatabase_type(), cache);

                }
                ids = cache.stream().filter(c -> dataSource.getMultiConnectionInstanceId().equals(c.getMultiConnectionInstanceId())).map(d -> d.getId().toHexString()).collect(Collectors.toList());
            }

            Criteria criteria1 = Criteria.where("is_deleted").is(false).and("dag.nodes").elemMatch(Criteria.where("type").is("logCollector").and("connectionIds").elemMatch(Criteria.where("$in").is(ids)));
            Query query1 = new Query(criteria1);
            query1.fields().include("dag", "status", "name", "currentEventTimestamp");
            List<String> connectionIds = v.stream().map(d -> d.getId().toHexString()).collect(Collectors.toList());
            TaskDto oldLogCollectorTask = taskService.findOne(query1, user);

            if (oldLogCollectorTask != null) {
                List<Node> sources1 = oldLogCollectorTask.getDag().getSources();
                LogCollectorNode logCollectorNode = (LogCollectorNode) sources1.get(0);
                boolean oldShareCdcNode = isOldShareCdcNode(logCollectorNode);
                if (oldShareCdcNode) {
                    oldShareCdcProcess(user, oldTaskDto, newLogCollectorMap, finalTableNames, ids, connectionIds, oldLogCollectorTask, logCollectorNode);
                    return;
                }

                newShareCdcProcess(user, oldTaskDto, newLogCollectorMap, tableMaps, finalTableNames, ids, connectionIds, oldLogCollectorTask, logCollectorNode);
                return;
            }

            LogCollectorNode logCollectorNode = new LogCollectorNode();
            if (StringUtils.isNotBlank(dataSource.getMultiConnectionInstanceId())) {
                Map<String, LogCollecotrConnConfig> logCollectorConnConfigs = new HashMap<>();
                for (String connectionId : connectionIds) {
                    LogCollecotrConnConfig logCollecotrConnConfig = new LogCollecotrConnConfig(connectionId, tableMaps.get(connectionId));
                    logCollectorConnConfigs.put(connectionId, logCollecotrConnConfig);
                }
                logCollectorNode.setLogCollectorConnConfigs(logCollectorConnConfigs);
            }


            logCollectorNode.setId(UUIDUtil.getUUID());
            logCollectorNode.setConnectionIds(connectionIds);
            logCollectorNode.setDatabaseType(v.get(0).getDatabase_type());
            logCollectorNode.setName(v.get(0).getName());
            logCollectorNode.setSelectType(LogCollectorNode.SELECT_TYPE_RESERVATION);
            if (logCollectorNode.getLogCollectorConnConfigs() == null || logCollectorNode.getLogCollectorConnConfigs().size() != 0) {
                logCollectorNode.setTableNames(new ArrayList<>(finalTableNames));
            }
            Map<String, Object> attr = Maps.newHashMap();
            attr.put("pdkHash", dataSource.getPdkHash());
            logCollectorNode.setAttrs(attr);

            HazelCastImdgNode hazelCastImdgNode = new HazelCastImdgNode();
            hazelCastImdgNode.setId(UUIDUtil.getUUID());
            AtomicReference<String> targetName = new AtomicReference<>("Shared Mining Target");
            Optional.ofNullable(externalStorageService.findById(MongoUtils.toObjectId(dataSource.getShareCDCExternalStorageId()))).ifPresent(externalStorageDto -> {
                hazelCastImdgNode.setExternaltype(externalStorageDto.getType());
                hazelCastImdgNode.setExternalStorageId(externalStorageDto.getId().toHexString());
                targetName.set(externalStorageDto.getName());
            });
            hazelCastImdgNode.setName(targetName.get());

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
                newLogCollectorMap.put(id, taskDto.getId().toHexString());
            }

            taskService.start(taskDto.getId(), user);

            TaskDto finalTaskDto = taskDto;
            FunctionUtils.ignoreAnyError(() -> {
                String template = "relate share cdc task, create new task: {0}, table name: {1}, current status {2}.";
                String msg = MessageFormat.format(template, finalTaskDto.getName(), JSON.toJSONString(finalTableNames), finalTaskDto.getStatus());
                monitoringLogsService.startTaskErrorLog(oldTaskDto, user, msg, Level.INFO);
            });
        });

        updateLogCollectorMap(oldTaskDto.getId(), newLogCollectorMap, user);
    }

    private void newShareCdcProcess(UserDetail user, TaskDto oldTaskDto, Map<String, String> newLogCollectorMap, Map<String, List<String>> tableMaps, Set<String> finalTableNames, List<String> ids, List<String> connectionIds, TaskDto oldLogCollectorTask, LogCollectorNode logCollectorNode) {
        boolean updateConfig = false;
        Map<String, LogCollecotrConnConfig> logCollectorConnConfigs = logCollectorNode.getLogCollectorConnConfigs();
        for (String id : ids) {
            newLogCollectorMap.put(id, oldLogCollectorTask.getId().toHexString());
        }
        for (String connectionId : connectionIds) {
            LogCollecotrConnConfig logCollecotrConnConfig = logCollectorConnConfigs.get(connectionId);
            if (logCollecotrConnConfig == null) {
                logCollecotrConnConfig = new LogCollecotrConnConfig(connectionId, tableMaps.get(connectionId));
                logCollectorConnConfigs.put(connectionId, logCollecotrConnConfig);
                updateConfig = true;
            } else {
                List<String> tableNames = tableMaps.get(connectionId);
                List<String> oldConfigTableNames = logCollecotrConnConfig.getTableNames();
                tableNames.addAll(oldConfigTableNames);
                tableNames = tableNames.stream().distinct().collect(Collectors.toList());
								if (CollectionUtils.isNotEmpty(logCollecotrConnConfig.getExclusionTables())) {
									logCollecotrConnConfig.getExclusionTables().removeIf(tableNames::contains);
								}
                if (tableNames.size() != oldConfigTableNames.size()) {
                    updateConfig =  true;
                    logCollecotrConnConfig.setTableNames(tableNames);
                }
            }
        }
        if (!updateConfig) {
            //检查状态，如果状态不是启动的，需要启动起来
            String status = oldLogCollectorTask.getStatus();

            if (TaskDto.STATUS_RUNNING.equals(status)) {
                FunctionUtils.ignoreAnyError(() -> {
                    String template = "relate share cdc task: {0}, table name: {1}, current status {2}, currentEventTimestamp is {3}.";
                    String msg = MessageFormat.format(template, oldLogCollectorTask.getName(), JSON.toJSONString(finalTableNames), oldLogCollectorTask.getStatus(), oldLogCollectorTask.getCurrentEventTimestamp());
                    monitoringLogsService.startTaskErrorLog(oldTaskDto, user, msg, Level.INFO);
                });
                return;
            }

            FunctionUtils.ignoreAnyError(() -> {
                String template = "relate share cdc task: {0}, table name: {1}, current status {2}, currentEventTimestamp is {3}, will start this task.";
                String msg = MessageFormat.format(template, oldLogCollectorTask.getName(), JSON.toJSONString(finalTableNames), oldLogCollectorTask.getStatus(), oldLogCollectorTask.getCurrentEventTimestamp());
                monitoringLogsService.startTaskErrorLog(oldTaskDto, user, msg, Level.INFO);
            });
            taskService.start(oldLogCollectorTask.getId(), user);
            return;
        }

        logCollectorNode.setLogCollectorConnConfigs(logCollectorConnConfigs);
        if (logCollectorNode.getLogCollectorConnConfigs() == null || logCollectorNode.getLogCollectorConnConfigs().size() != 0) {
            logCollectorNode.setTableNames(new ArrayList<>(finalTableNames));
        }
        taskService.updateById(oldLogCollectorTask, user);
        updateLogCollectorMap(oldTaskDto.getId(), newLogCollectorMap, user);

        FunctionUtils.ignoreAnyError(() -> {
            String template = "relate share cdc task: {0}, table name: {1}, current status {2}, currentEventTimestamp is {3}.";
            String msg = MessageFormat.format(template, oldLogCollectorTask.getName(), JSON.toJSONString(finalTableNames), oldLogCollectorTask.getStatus(), oldLogCollectorTask.getCurrentEventTimestamp());
            monitoringLogsService.startTaskErrorLog(oldTaskDto, user, msg, Level.INFO);
        });

        //这个stop是异步的， 需要重启，重启的逻辑是通过定时任务跑的
        pause(oldLogCollectorTask, user);
        taskService.pause(oldLogCollectorTask.getId(), user, false, true);
    }

    private void pause(TaskDto oldLogCollectorTask, UserDetail user) {
        pause(oldLogCollectorTask, true, user);
    }

    private void pause(TaskDto oldLogCollectorTask, boolean restart, UserDetail user) {
        if (TaskDto.STATUS_RUNNING.equals(oldLogCollectorTask.getStatus())) {
            taskService.pause(oldLogCollectorTask.getId(), user, false, restart);
            return;
        }

        taskService.start(oldLogCollectorTask.getId(), user);

    }

    private void oldShareCdcProcess(UserDetail user, TaskDto oldTaskDto, Map<String, String> newLogCollectorMap, Set<String> finalTableNames, List<String> ids, List<String> connectionIds, TaskDto oldLogCollectorTask, LogCollectorNode logCollectorNode) {
        List<String> oldTableNames = logCollectorNode.getTableNames();
        for (String id : ids) {
            newLogCollectorMap.put(id, oldLogCollectorTask.getId().toHexString());
        }

        List<String> oldConnectionIds = logCollectorNode.getConnectionIds();

        boolean updateConnectionId = false;
        for (String connectionId : connectionIds) {
            if (!oldConnectionIds.contains(connectionId)) {
                oldConnectionIds.add(connectionId);
                updateConnectionId = true;
            }
        }


        if (CollectionUtils.isNotEmpty(oldTableNames) && new HashSet<>(oldTableNames).containsAll(finalTableNames)) {
            //检查状态，如果状态不是启动的，需要启动起来
            String status = oldLogCollectorTask.getStatus();
            if (updateConnectionId) {
                taskService.confirmById(oldLogCollectorTask, user, true);
            }

            if (TaskDto.STATUS_RUNNING.equals(status)) {
                FunctionUtils.ignoreAnyError(() -> {
                    String template = "relate share cdc task: {0}, table name: {1}, current status {2}, currentEventTimestamp is {3}.";
                    String msg = MessageFormat.format(template, oldLogCollectorTask.getName(), JSON.toJSONString(finalTableNames), oldLogCollectorTask.getStatus(), oldLogCollectorTask.getCurrentEventTimestamp());
                    monitoringLogsService.startTaskErrorLog(oldTaskDto, user, msg, Level.INFO);
                });
                return;
            }

            FunctionUtils.ignoreAnyError(() -> {
                String template = "relate share cdc task: {0}, table name: {1}, current status {2}, currentEventTimestamp is {3}, will start this task.";
                String msg = MessageFormat.format(template, oldLogCollectorTask.getName(), JSON.toJSONString(finalTableNames), oldLogCollectorTask.getStatus(), oldLogCollectorTask.getCurrentEventTimestamp());
                monitoringLogsService.startTaskErrorLog(oldTaskDto, user, msg, Level.INFO);
            });
            taskService.start(oldLogCollectorTask.getId(), user);
            return;
        }
        if (CollectionUtils.isNotEmpty(oldTableNames)) {
            finalTableNames.addAll(oldTableNames);
        }
        List<String> collect = finalTableNames.stream().distinct().collect(Collectors.toList());
				List<String> exclusionTables = logCollectorNode.getExclusionTables();
				if (CollectionUtils.isNotEmpty(exclusionTables)) {
					exclusionTables.removeIf(collect::contains);
				}
				logCollectorNode.setTableNames(collect);
        taskService.updateById(oldLogCollectorTask, user);
        updateLogCollectorMap(oldTaskDto.getId(), newLogCollectorMap, user);

        FunctionUtils.ignoreAnyError(() -> {
            String template = "relate share cdc task: {0}, table name: {1}, current status {2}, currentEventTimestamp is {3}.";
            String msg = MessageFormat.format(template, oldLogCollectorTask.getName(), JSON.toJSONString(finalTableNames), oldLogCollectorTask.getStatus(), oldLogCollectorTask.getCurrentEventTimestamp());
            monitoringLogsService.startTaskErrorLog(oldTaskDto, user, msg, Level.INFO);
        });

        //这个stop是异步的， 需要重启，重启的逻辑是通过定时任务跑的
        pause(oldLogCollectorTask, user);
    }

    /**
     * 根据同步任务启动连接心跳打点任务
     * <pre>
     * 启动条件：
     *   - 同步任务启动时检查，并启动连接对应打点任务
     *   - 同步任务类型为：迁移、同步
     *   - 同步任务包含增量同步（全量+增量、增量）
     *   - 同步任务源连接非 dummy 节点，并支持增量同步
     * 打点任务逻辑：
     *   - 增量任务
     *   - 一个打点任务对应一个连接的心跳数据生成
     *   - 源为 dummy 节点，mode=ConnHeartbeat
     *   - 目标为任务源节点
     * </pre>
     *
     * @param user       操作用户信息
     * @param taskDto 启动任务
     */
    public void startConnHeartbeat(UserDetail user, TaskDto taskDto) {
        if (!ConnHeartbeatUtils.checkTask(taskDto.getType(), taskDto.getSyncType())) return;
        log.info("start connection heartbeat: {}({})", taskDto.getId(), taskDto.getName());

        String subTaskId;
        DataSourceConnectionDto heartbeatConnection = null;
        List<DataSourceConnectionDto> dataSourceDtos;
        Set<String> joinConnectionIdSet = new HashSet<>();
        Map<String, List<DataSourceConnectionDto>> dataSourceCacheByType = new HashMap<>(); //不同类型数据源的id缓存

        subTaskId = taskDto.getId().toHexString();
        dataSourceDtos = getConnectionByDag(user, taskDto.getDag());
        for (DataSourceConnectionDto dataSource : dataSourceDtos) {
            String dataSourceId = dataSource.getId().toHexString();
            if (joinConnectionIdSet.contains(dataSourceId) || !ConnHeartbeatUtils.checkConnection(dataSource))
                continue;

            //如果连接已经有心跳任务，将连接任务编号添加到 heartbeatTasks，并尝试启动任务
            List<String> connectionIds = getConnectionIds(user, dataSourceCacheByType, dataSource);
            TaskDto oldConnHeartbeatTask = queryTask(user, connectionIds);
            if (oldConnHeartbeatTask != null) {
                HashSet<String> heartbeatTasks = Optional.ofNullable(oldConnHeartbeatTask.getHeartbeatTasks()).orElseGet(HashSet::new);
                heartbeatTasks.add(subTaskId);
                taskService.update(new Query(Criteria.where("_id").is(oldConnHeartbeatTask.getId())), Update.update(ConnHeartbeatUtils.TASK_RELATION_FIELD, heartbeatTasks), user);
                if (!TaskDto.STATUS_RUNNING.equals(oldConnHeartbeatTask.getStatus())) {
                    taskService.start(oldConnHeartbeatTask.getId(), user);
                }
                joinConnectionIdSet.add(dataSourceId);
                continue;
            }

            //循环中只需要获取一次dummy源跟打点模型表
            if (heartbeatConnection == null) {
                boolean addDummy = false;
                //获取打点的Dummy数据源
                Query query2 = new Query(Criteria.where("database_type").is(ConnHeartbeatUtils.PDK_NAME)
                        .and("createType").is(CreateTypeEnum.System)
                );
                heartbeatConnection = dataSourceService.findOne(query2, user);
                if (heartbeatConnection == null) {
                    Query query3 = new Query(Criteria.where("pdkId").is(ConnHeartbeatUtils.PDK_ID));
                    query3.fields().include("pdkHash", "type");
                    DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.findOne(query3);
                    if (null == definitionDto) {
                        log.warn("Not found heartbeat connector: {}", ConnHeartbeatUtils.PDK_NAME);
                        return;
                    }
                    heartbeatConnection = ConnHeartbeatUtils.generateConnections(dataSourceId, definitionDto);
                    heartbeatConnection = dataSourceService.add(heartbeatConnection, user);
                    dataSourceService.sendTestConnection(heartbeatConnection, true, true, user); //添加后没加载模型，手动加载一次
                    addDummy = true;
                }

                String qualifiedName = MetaDataBuilderUtils.generateQualifiedName("table", heartbeatConnection, ConnHeartbeatUtils.TABLE_NAME);
                MetadataInstancesDto metadata = metadataInstancesService.findByQualifiedNameNotDelete(qualifiedName, user, "_id");
                if (metadata == null) {
                    if (!addDummy) {
                        //新增数据源的时候 我自动加载模型
                        dataSourceService.sendTestConnection(heartbeatConnection, true, true, user);
                    }

                    for (int i = 0; i < 8; i++) {
                        if (metadataInstancesService.findByQualifiedNameNotDelete(qualifiedName, user, "_id") == null) {
                            try {
                                Thread.sleep(500 * i);
                            } catch (InterruptedException e) {
                                throw new BizException("SystemError", "Wait heartbeat task transformed schema timeout");
                            }
                        }

                    }
                }
            }

            TaskDto taskDto1 = ConnHeartbeatUtils.generateTask(subTaskId
                    , dataSourceId, dataSource.getName(), dataSource.getDatabase_type(), dataSource.getPdkHash()
                    , heartbeatConnection.getId().toHexString(), heartbeatConnection.getDatabase_type(), heartbeatConnection.getPdkHash());
            taskDto1 = taskService.create(taskDto1, user);
            taskDto1 = taskService.confirmById(taskDto1, user, true);
            taskService.start(taskDto1.getId(), user);
            joinConnectionIdSet.add(dataSourceId);
        }
    }




    private List<String> getConnectionIds(UserDetail user, Map<String, List<DataSourceConnectionDto>> dataSourceCacheByType, DataSourceConnectionDto dataSource) {
        List<String> ids = new ArrayList<>();

        //如果没有uniqname,则唯一键采用的id，所以不会存在相似的数据源
        if (StringUtils.isBlank(dataSource.getMultiConnectionInstanceId())) {
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


            ids = cache.stream().filter(c -> dataSource.getMultiConnectionInstanceId().equals(c.getMultiConnectionInstanceId())).map(d -> d.getId().toHexString()).collect(Collectors.toList());
        }
        return ids;
    }

    public void endConnHeartbeat(UserDetail user, TaskDto taskDto) {
        if (null == taskDto) {
            log.warn("end connections heartbeat not found parent task: {}({})", taskDto.getId(), taskDto.getName());
            return;
        }
        log.info("stop connection heartbeat: {}({})", taskDto.getId(), taskDto.getName());

        if (!ConnHeartbeatUtils.checkTask(taskDto.getType(), taskDto.getSyncType())) return;

        //不同类型数据源的id缓存
        Map<String, List<DataSourceConnectionDto>> dataSourceCacheByType = new HashMap<>();
        List<DataSourceConnectionDto> dataSourceDtos = getConnectionByDag(user, taskDto.getDag());
        for (DataSourceConnectionDto dataSource : dataSourceDtos) {
            //获取是否存在这些connectionIds作为源的任务。
            List<String> connectionIds = getConnectionIds(user, dataSourceCacheByType, dataSource);
            TaskDto oldConnHeartbeatTask = queryTask(user, connectionIds);
            if (null == oldConnHeartbeatTask) {
                log.warn("not found heartbeat task, connId: {}", dataSource.getId().toHexString());
                continue;
            }
            HashSet<String> heartbeatTasks = oldConnHeartbeatTask.getHeartbeatTasks();
            if (heartbeatTasks.remove(taskDto.getId().toHexString())) {
                taskService.update(new Query(Criteria.where("_id").is(oldConnHeartbeatTask.getId())), Update.update(ConnHeartbeatUtils.TASK_RELATION_FIELD, heartbeatTasks), user);
            }
            if (heartbeatTasks.size() == 0) {
                taskService.pause(oldConnHeartbeatTask.getId(), user, false);
            }
        }
    }

    private TaskDto queryTask(UserDetail user, List<String> ids) {
        Criteria criteria1 = Criteria.where("is_deleted").is(false)
                .and("syncType").is(TaskDto.SYNC_TYPE_CONN_HEARTBEAT)
                .and("dag.nodes").elemMatch(Criteria.where("connectionId").in(ids));
        Query query1 = new Query(criteria1);
        query1.fields().include("dag", "status", ConnHeartbeatUtils.TASK_RELATION_FIELD);
        TaskDto oldConnHeartbeatTask = taskService.findOne(query1, user);
        return oldConnHeartbeatTask;
    }

    private List<DataSourceConnectionDto> getConnectionByDag(UserDetail user, DAG dag) {
        Set<String> connectionIds = new HashSet<>();
        for (Node n :  dag.getSources()) {
            if (n instanceof DataParentNode) {
                Optional.ofNullable(((DataParentNode<?>) n).getConnectionId()).ifPresent(connectionIds::add);
            } else if (n instanceof LogCollectorNode) {
                Optional.ofNullable(((LogCollectorNode) n).getConnectionIds()).ifPresent(connectionIds::addAll);
            }
        }

        //查询获取所有源的数据源连接
        Criteria criteria = Criteria.where("_id").in(connectionIds);
        Query query = new Query(criteria);
        query.fields().include("_id", "uniqueName", "database_type", "name", "capabilities", "heartbeatEnable", "pdkHash");
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
        TaskDto taskDto = taskService.findByTaskId(taskId, "dag", "_id");
        if (taskDto == null) {
            return;
        }

        if (newLogCollectorMap == null || newLogCollectorMap.isEmpty()) {
            return;
        }

        DAG dag = taskDto.getDag();
        List<Node> sources = dag.getSources();
        Map<String, String> shareCdcTaskId = taskDto.getShareCdcTaskId();
        if (shareCdcTaskId == null) {
            shareCdcTaskId = new HashMap<>();
            taskDto.setShareCdcTaskId(shareCdcTaskId);
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
        taskService.updateById(taskDto.getId(), update, user);
    }


    private boolean isOldShareCdcNode(LogCollectorNode logCollectorNode) {
        Map<String, LogCollecotrConnConfig> logCollectorConnConfigs = logCollectorNode.getLogCollectorConnConfigs();
        return logCollectorConnConfigs == null || logCollectorConnConfigs.size() == 0;
    }
//    private boolean convertLogCollectorNode(LogCollectorNode logCollectorNode) {
//
//
//        Map<String, LogCollecotrConnConfig> logCollectorConnConfigs = logCollectorNode.getLogCollectorConnConfigs();
//        if (logCollectorConnConfigs != null && logCollectorConnConfigs.size() != 0) {
//            return false;
//        }
//
//        List<String> connectionIds = logCollectorNode.getConnectionIds();
//        List<String> tableNames = logCollectorNode.getTableNames();
//        logCollectorConnConfigs = new HashMap<>();
//        if (CollectionUtils.isNotEmpty(connectionIds)) {
//            logCollectorNode.setLogCollectorConnConfigs(logCollectorConnConfigs);
//            return true;
//        }
//
//        for (String connectionId : connectionIds) {
//            logCollectorConnConfigs.put(connectionId, new LogCollecotrConnConfig(connectionId, tableNames));
//        }
//
//        logCollectorNode.setLogCollectorConnConfigs(logCollectorConnConfigs);
//
//        logCollectorNode.setConnectionIds(null);
//        logCollectorNode.setTableNames(null);
//        return true;
//    }

    public void cancelMerge(String taskId, String connectionId, UserDetail user) {
        TaskDto shareCdcTask = taskService.findById(MongoUtils.toObjectId(taskId), user);
        DAG dag = shareCdcTask.getDag();
        List<Node> sources = dag.getSources();
        LogCollectorNode logCollectorNode = (LogCollectorNode)sources.get(0);
        Map<String, LogCollecotrConnConfig> logCollectorConnConfigs = logCollectorNode.getLogCollectorConnConfigs();
        if (logCollectorConnConfigs != null && logCollectorConnConfigs.size() != 0) {
            //new version shareCdc task
            LogCollecotrConnConfig logCollecotrConnConfig = logCollectorConnConfigs.get(connectionId);
        }
    }

    public Page<ShareCdcTableInfo> tableInfos(String taskId, String connectionId, String keyword, Integer page, Integer size, UserDetail user) {
        TaskDto shareCdcTask = taskService.findById(MongoUtils.toObjectId(taskId), user);
        DAG dag = shareCdcTask.getDag();
        List<Node> sources = dag.getSources();
        LogCollectorNode logCollectorNode = (LogCollectorNode)sources.get(0);
        Map<String, LogCollecotrConnConfig> logCollectorConnConfigs = logCollectorNode.getLogCollectorConnConfigs();
        List<String> tableNames;
        Map<String, String> tableNameConnectionIdMap = new HashMap<>();
        if (logCollectorConnConfigs == null || logCollectorConnConfigs.size() == 0) {
            //old version shareCdc task
            tableNames = logCollectorNode.getTableNames();
            tableNames.forEach(tableName -> tableNameConnectionIdMap.put(tableName, logCollectorNode.getConnectionIds().get(0)));
        } else {
            //new version shareCdc task
            if (StringUtils.isNotEmpty(connectionId)) {
                LogCollecotrConnConfig logCollecotrConnConfig = logCollectorConnConfigs.get(connectionId);
                tableNames = logCollecotrConnConfig.getTableNames();
                tableNames.forEach(tableName -> tableNameConnectionIdMap.put(tableName, connectionId));
            } else {
                tableNames = logCollectorConnConfigs.values().stream().flatMap(logCollecotrConnConfig -> logCollecotrConnConfig.getTableNames().stream()).collect(Collectors.toList());
                logCollectorConnConfigs.forEach((connId, logCollecotrConnConfig) ->
                        logCollecotrConnConfig.getTableNames().forEach(tableName -> tableNameConnectionIdMap.put(tableName, connId)));
            }

        }
        return getShareCdcTableInfoPage(tableNameConnectionIdMap, page, size, user, keyword, logCollectorNode.getId(), taskId);
    }


    public Page<ShareCdcTableInfo> excludeTableInfos(String taskId, String connectionId, String keyword, Integer page, Integer size, UserDetail user) {
        TaskDto shareCdcTask = taskService.findById(MongoUtils.toObjectId(taskId), user);
        DAG dag = shareCdcTask.getDag();
        List<Node> sources = dag.getSources();
        LogCollectorNode logCollectorNode = (LogCollectorNode) sources.get(0);
        Map<String, LogCollecotrConnConfig> logCollectorConnConfigs = logCollectorNode.getLogCollectorConnConfigs();
        List<String> tableNames;
        Map<String, String> tableNameConnectionIdMap = new HashMap<>();
        if (logCollectorConnConfigs == null || logCollectorConnConfigs.size() == 0) {
            //old version shareCdc task
            tableNames = logCollectorNode.getExclusionTables();
            tableNames.forEach(tableName -> tableNameConnectionIdMap.put(tableName, logCollectorNode.getConnectionIds().get(0)));
        } else {
            //new version shareCdc task
            if (StringUtils.isNotEmpty(connectionId)) {
                LogCollecotrConnConfig logCollecotrConnConfig = logCollectorConnConfigs.get(connectionId);
                tableNames = logCollecotrConnConfig.getExclusionTables();
                tableNames.forEach(tableName -> tableNameConnectionIdMap.put(tableName, connectionId));
            } else {
                tableNames = logCollectorConnConfigs.values().stream().flatMap(logCollecotrConnConfig -> logCollecotrConnConfig.getExclusionTables().stream()).collect(Collectors.toList());
                logCollectorConnConfigs.forEach((connId, logCollecotrConnConfig) ->
                        logCollecotrConnConfig.getExclusionTables().forEach(tableName -> tableNameConnectionIdMap.put(tableName, connId)));
            }

        }
        return getShareCdcTableInfoPage(tableNameConnectionIdMap, page, size, user, keyword, logCollectorNode.getId(), taskId);
    }

    @NotNull
    private Page<ShareCdcTableInfo> getShareCdcTableInfoPage(Map<String, String> tableNameConnectionIdMap, Integer page, Integer size, UserDetail user, String keyword, String nodeId, String taskId) {
        int limit = (page - 1) * size;
        List<String> tableNames = new ArrayList<>(tableNameConnectionIdMap.keySet());
        if (StringUtils.isNotEmpty(keyword)) {
            tableNames = tableNames.stream().filter(tableName -> StringUtils.containsAnyIgnoreCase(tableName, keyword)).collect(Collectors.toList());
        }
        int tableCount = tableNames.size();

        Field field = new Field();
        field.put("_id", true);
        field.put("name", true);
        List<ShareCdcTableInfo> shareCdcTableInfos = new ArrayList<>();
        for (int i = limit; i< size; i++) {
            if (tableCount <= i) {
                break;
            }
            String tableName = tableNames.get(i);
            String connectionId = tableNameConnectionIdMap.get(tableName);
            DataSourceConnectionDto connectionDto = dataSourceService.findById(MongoUtils.toObjectId(connectionId), field, user);
            String connectionName = connectionDto.getName();
            ShareCdcTableInfo shareCdcTableInfo = new ShareCdcTableInfo();
            shareCdcTableInfo.setName(tableName);
            shareCdcTableInfo.setConnectionName(connectionName);
            shareCdcTableInfo.setConnectionId(connectionId);
            setShareTableInfo(shareCdcTableInfo, user, taskId, nodeId);

            shareCdcTableInfos.add(shareCdcTableInfo);

        }

        Page<ShareCdcTableInfo> shareCdcTableInfoPage = new Page<>();
        shareCdcTableInfoPage.setTotal(tableCount);
        shareCdcTableInfoPage.setItems(shareCdcTableInfos);
        return shareCdcTableInfoPage;
    }


    private void setShareTableInfo(ShareCdcTableInfo shareCdcTableInfo, UserDetail user, String taskId, String nodeId) {
        Criteria criteria = Criteria.where("taskId").is(taskId)
                .and("tableName").is(shareCdcTableInfo.getName())
                .and("nodeId").is(nodeId)
                .and("connectionId").is(shareCdcTableInfo.getConnectionId());
        Query query = new Query(criteria);
        query.with(Sort.by("createTime").descending());
        ShareCdcTableMetricsDto metricsDto = shareCdcTableMetricsService.findOne(query, user);
				if (metricsDto != null) {
					shareCdcTableInfo.setFirstLogTime(metricsDto.getFirstEventTime());
					shareCdcTableInfo.setLastLogTime(metricsDto.getCurrentEventTime());
					shareCdcTableInfo.setJoinTime(metricsDto.getStartCdcTime());
					shareCdcTableInfo.setTodayCount(metricsDto.getCount());
					shareCdcTableInfo.setAllCount(metricsDto.getAllCount());
				}
		}

    public void configTables(String taskId, List<TableLogCollectorParam> params, String type, UserDetail user) {
			TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(taskId), user);
			DAG dag = taskDto.getDag();
			List<Node> sources = dag.getSources();
			LogCollectorNode logCollectorNode = (LogCollectorNode)sources.get(0);
      boolean deleteTask = false;
      log.info("Update logCollector configuration: {} {}", type, params);
			if (logCollectorNode.getLogCollectorConnConfigs() == null) {
          if (type.equals("exclusion")) {
              exclusionTables(logCollectorNode, params);
          } else if (type.equals("add")) {
              addTables(logCollectorNode, params);
          } else {
              throw new IllegalArgumentException("type param is illegal");
          }
          if (CollectionUtils.isEmpty(logCollectorNode.getTableNames())) {
              deleteTask = true;
          }
			} else {
          Map<String, LogCollecotrConnConfig> logCollectorConnConfigMap = logCollectorNode.getLogCollectorConnConfigs();
          if (type.equals("exclusion")) {
              logCollectorConnConfigMap = exclusionTables(logCollectorConnConfigMap, params);
          } else if (type.equals("add")) {
              logCollectorConnConfigMap = addTables(logCollectorConnConfigMap, params);
          } else {
              throw new IllegalArgumentException("type param is illegal");
          }
          logCollectorNode.setLogCollectorConnConfigs(logCollectorConnConfigMap);
          List<String> tableNames = logCollectorConnConfigMap.values().stream()
                  .map(LogCollecotrConnConfig::getTableNames).flatMap(Collection::stream).collect(Collectors.toList());
          if (CollectionUtils.isEmpty(tableNames)) {
              deleteTask = true;
          }
      }

			taskService.update(Query.query(Criteria.where("_id").is(taskDto.getId())), taskDto);
      pause(taskDto, !deleteTask, user);
      if (deleteTask) {
          log.info("No tables need to collect logs, the task [{}-{}] will be deleted. ", taskDto.getId(), taskDto.getName());
          taskService.remove(taskDto.getId(), user);
      }
	}

    public List<ShareCdcConnectionInfo> getConnectionIds(String taskId, UserDetail user) {
        TaskDto shareCdcTask = taskService.findById(MongoUtils.toObjectId(taskId), user);
        DAG dag = shareCdcTask.getDag();
        List<Node> sources = dag.getSources();
        LogCollectorNode logCollectorNode = (LogCollectorNode)sources.get(0);
        Map<String, LogCollecotrConnConfig> logCollectorConnConfigs = logCollectorNode.getLogCollectorConnConfigs();
        Set<String> connectionIds;
        if (logCollectorConnConfigs == null) {
            connectionIds = new HashSet<>(logCollectorNode.getConnectionIds());
        } else {
            connectionIds = logCollectorConnConfigs.keySet();
        }
        List<DataSourceConnectionDto> alls = dataSourceService.findAllByIds(new ArrayList<>(connectionIds));
        if (alls != null) {
            return alls.stream().map(c -> new ShareCdcConnectionInfo(c.getId().toHexString(), c.getName())).collect(Collectors.toList());
        }

        return new ArrayList<>();
    }

    public void clear() {
        //1、查找所有正在运行的共享挖掘任务
        Criteria criteria = Criteria.where("is_deleted").ne(true)
                .and("syncType").is(TaskDto.SYNC_TYPE_LOG_COLLECTOR)
                .and("status").is(TaskDto.STATUS_RUNNING);
        Query query = new Query(criteria);
        query.fields().include("_id", "dag", "status", "user_id");
        List<TaskDto> taskDtos = taskService.findAll(query);
        if (CollectionUtils.isEmpty(taskDtos)) {
            return;
        }

        for (TaskDto logCollectorTaskDto : taskDtos) {
					try {
						DAG dag = logCollectorTaskDto.getDag();
						List<Node> sources = dag.getSources();
						LogCollectorNode logCollectorNode = (LogCollectorNode) sources.get(0);
						Map<String, LogCollecotrConnConfig> logCollectorConnConfigs = logCollectorNode.getLogCollectorConnConfigs();
						Map<String, Set<String>> tableMap = new HashMap<>();
						if (logCollectorConnConfigs == null) {
								tableMap.put(logCollectorNode.getConnectionIds().get(0), new HashSet<>(logCollectorNode.getTableNames()));
						} else {
								tableMap = logCollectorConnConfigs.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new HashSet<>(e.getValue().getTableNames())));
						}
						if (MapUtils.isEmpty(tableMap)) {
								continue;
						}

						MatchOperation taskMatchOperation = Aggregation.match(Criteria.where("is_deleted").ne(true)
										.and("syncType").ne(TaskDto.SYNC_TYPE_LOG_COLLECTOR));
						List<Criteria> orCriteriaList = new ArrayList<>();
						tableMap.forEach((connectionId, tableNames) -> {
								orCriteriaList.add(
												new Criteria("dag.nodes")
																.elemMatch(
																				new Criteria("connectionId")
																								.is(connectionId)
																								.orOperator(
																												new Criteria("tableName").in(tableNames),
																												new Criteria("tableNames").in(tableNames)
																								)
																)
								);
						});
						Criteria finalCriteria = new Criteria().orOperator(orCriteriaList);
						MatchOperation matchOperation = Aggregation.match(finalCriteria);

						UnwindOperation unwindOperation = Aggregation.unwind("$dag.nodes");

						List<Criteria> nodeOrCriteriaList = new ArrayList<>();
						tableMap.forEach((connectionId, tableNames) -> nodeOrCriteriaList.add(new Criteria("dag.nodes.connectionId").is(connectionId)
										.orOperator(new Criteria("dag.nodes.tableName").in(tableNames), new Criteria("dag.nodes.tableNames").in(tableNames))));
						Criteria nodeFinalCriteria = new Criteria().orOperator(nodeOrCriteriaList);
						MatchOperation nodeMatchOperation = Aggregation.match(nodeFinalCriteria);

						ProjectionOperation project = Aggregation.project()
										.andExclude("_id")
										.and("dag.nodes.connectionId").as("connectionId")
										.and("dag.nodes.tableName").as("tableName")
										.and("dag.nodes.tableNames").as("tableNames");


						Aggregation aggregation = Aggregation.newAggregation(taskMatchOperation, matchOperation, unwindOperation, nodeMatchOperation, project);
						List<Map> mappedResults = taskService.aggregate(aggregation, Map.class).getMappedResults();

						Map<String, Set<String>> useMap = new HashMap<>();
						if (CollectionUtils.isNotEmpty(mappedResults)) {
								for (Map<String, Object> mappedResult : mappedResults) {
										String connectionId = (String) mappedResult.get("connectionId");
										Set<String> tableSet = useMap.computeIfAbsent(connectionId, c -> new HashSet<>());
										String tableName = (String) mappedResult.get("tableName");
										if (tableName != null) {
												tableSet.add(tableName);
										}
										Collection<String> tableNames = (Collection<String>) mappedResult.get("tableNames");
										if (tableNames != null) {
												tableSet.addAll(tableNames);
										}
								}
						}

						//tableMap中的tableNames在map中不存在的，需要删除
						tableMap.forEach((connectionId, tableNames) -> {
								Set<String> usingTableNames = useMap.get(connectionId);
								if (CollectionUtils.isEmpty(usingTableNames)) {
										return;
								}
								usingTableNames.forEach(tableNames::remove);
						});
						if (MapUtils.isEmpty(tableMap) || CollectionUtils.isEmpty(tableMap.values().stream().flatMap(Collection::stream).collect(Collectors.toList()))) {
								continue;
						}
						//需要排除
						List<TableLogCollectorParam> params = tableMap.entrySet().stream()
										.map(e -> new TableLogCollectorParam(e.getKey(), e.getValue())).collect(Collectors.toList());
						log.info("The logCollector table is not being used, will be canceled: {}", params);
						UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(logCollectorTaskDto.getUserId()));
						configTables(logCollectorTaskDto.getId().toHexString(), params, "exclusion", userDetail);
					} catch (Exception e) {
						log.error("Failed to clear logCollector task: {} {}", logCollectorTaskDto.getId(), logCollectorTaskDto.getName(), e);
					}
				}
    }

		public void removeTask() {
			Criteria taskStatusCriteria = Criteria.where("is_deleted").ne(true)
							.and("syncType").is(TaskDto.SYNC_TYPE_LOG_COLLECTOR)
							.and("status").ne(TaskDto.STATUS_RUNNING);

			Criteria noTablesCriteria = Criteria.where("dag.nodes").elemMatch(new Criteria().andOperator(
							Criteria.where("type").is("logCollector"),
							new Criteria().orOperator(
											new Criteria().andOperator(new Criteria().orOperator(
															Criteria.where("logCollectorConnConfigs").exists(false), Criteria.where("logCollectorConnConfigs").is(new BsonDocument())),
															new Criteria().orOperator(Criteria.where("tableNames").exists(false), Criteria.where("tableNames").size(0))),
											Criteria.where("logCollectorConnConfigs").exists(true).not().elemMatch(Criteria.where("tableNames").exists(true).ne(Collections.emptyList()))
			)));

			Query query = Query.query(new Criteria().andOperator(taskStatusCriteria, noTablesCriteria));
			query.fields().include("_id", "name","user_id");
			List<TaskDto> taskDtos = taskService.findAll(query);
			if (CollectionUtils.isNotEmpty(taskDtos)) {
				taskDtos.forEach(taskDto -> {
					try {
						UserDetail userDetail = userService.loadUserById(MongoUtils.toObjectId(taskDto.getUserId()));
						taskService.remove(taskDto.getId(), userDetail);
						log.info("removed logCollector task: {} {}", taskDto.getId().toHexString(), taskDto.getName());
					} catch (Exception e) {
						log.error("Failed to remove logCollector task: {} {}", taskDto.getId().toHexString(), taskDto.getName(), e);
					}
				});
			}
		}

	private void addTables(LogCollectorNode logCollectorNode, List<TableLogCollectorParam> params) {
		for (TableLogCollectorParam param : params) {
			if (!logCollectorNode.getConnectionIds().contains(param.getConnectionId())) {
				throw new IllegalArgumentException("not support add table for connectionId: " + param.getConnectionId() + "tables: " + param.getTableNames());
			}
			List<String> tableNames = logCollectorNode.getTableNames();
			if (tableNames == null) {
				tableNames = new ArrayList<>();
			}
			tableNames.addAll(param.getTableNames());
			logCollectorNode.setTableNames(new ArrayList<>(new HashSet<>(tableNames)));
			List<String> exclusionTables = logCollectorNode.getExclusionTables();
			if (exclusionTables != null) {
				exclusionTables.removeIf(tableNames::contains);
			}
		}
	}


    private Map<String, LogCollecotrConnConfig> addTables(Map<String, LogCollecotrConnConfig> logCollectorConnConfigMap, List<TableLogCollectorParam> params) {
		Map<String, LogCollecotrConnConfig> paramMap = params.stream()
						.collect(Collectors.toMap(TableLogCollectorParam::getConnectionId, e -> new LogCollecotrConnConfig(e.getConnectionId(), new ArrayList<>(e.getTableNames())), (o, n) -> {
							o.getTableNames().addAll(n.getTableNames());
							return o;
						}));
		logCollectorConnConfigMap = Stream.of(paramMap, logCollectorConnConfigMap).flatMap(map -> map.entrySet().stream())
						.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (o, n) -> {
							if (n.getTableNames() != null) {
									if (o.getTableNames() == null) {
											o.setTableNames(n.getTableNames());
									} else {
											o.getTableNames().addAll(n.getTableNames());
									}
							}
							if (n.getExclusionTables() != null) {
									if (o.getExclusionTables() == null) {
											o.setExclusionTables(n.getExclusionTables());
									} else {
											o.getExclusionTables().addAll(n.getExclusionTables());
									}
							}
							return o;
						}));
		logCollectorConnConfigMap.values().forEach(v -> {
            if (v.getExclusionTables() !=null) {
                v.getExclusionTables().removeIf(v.getTableNames()::contains);
            }
        });
		return logCollectorConnConfigMap;
	}

	private void exclusionTables(LogCollectorNode logCollectorNode, List<TableLogCollectorParam> params) {
		for (TableLogCollectorParam param : params) {
			if (!logCollectorNode.getConnectionIds().contains(param.getConnectionId())) {
				throw new IllegalArgumentException("not support exclusion table for connectionId: " + param.getConnectionId() + "tables: " + param.getTableNames());
			}
			List<String> exclusionTables = logCollectorNode.getExclusionTables();
			if (exclusionTables == null) {
				exclusionTables = new ArrayList<>();
			}
			exclusionTables.addAll(param.getTableNames());
			logCollectorNode.setExclusionTables(new ArrayList<>(new HashSet<>(exclusionTables)));
			List<String> tableNames = logCollectorNode.getTableNames();
			if (tableNames == null) {
				tableNames = new ArrayList<>();
				logCollectorNode.setTableNames(tableNames);
			}
			tableNames.removeIf(exclusionTables::contains);
		}
	}

	private Map<String, LogCollecotrConnConfig> exclusionTables(Map<String, LogCollecotrConnConfig> logCollectorConnConfigMap, List<TableLogCollectorParam> params) {
		Map<String, LogCollecotrConnConfig> paramMap = params.stream()
						.collect(Collectors.toMap(TableLogCollectorParam::getConnectionId,
										e -> new LogCollecotrConnConfig(e.getConnectionId(), new ArrayList<>(), new ArrayList<>(e.getTableNames())),
										(o, n) -> {
											o.getExclusionTables().addAll(n.getExclusionTables());
											return o;
						}));
		logCollectorConnConfigMap = Stream.of(paramMap, logCollectorConnConfigMap).flatMap(map -> map.entrySet().stream())
						.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (o, n) -> {
							if (n.getExclusionTables() != null) {
									if (o.getExclusionTables() == null) {
											o.setExclusionTables(n.getExclusionTables());
									} else {
											o.getExclusionTables().addAll(n.getExclusionTables());
									}
							}
							if (n.getTableNames() != null) {
								if (o.getTableNames() == null) {
									o.setTableNames(n.getTableNames());
								} else {
									o.getTableNames().addAll(n.getTableNames());
								}
							}
							return o;
						}));
        logCollectorConnConfigMap.values().forEach(v -> {
            if (null != v.getTableNames() && null != v.getExclusionTables()) {
                v.getTableNames().removeIf(v.getExclusionTables()::contains);
            }
        });

		return logCollectorConnConfigMap;
	}

	public static void main(String[] args) {
		Map<String, LogCollecotrConnConfig> logCollectorConnConfigMap = new HashMap<String, LogCollecotrConnConfig>() {{
			put("connectionId_1", new LogCollecotrConnConfig("connectionId_1",
							new ArrayList<String>() {{
								add("table_1");
								add("table_2");
								add("table_3");
							}},
							new ArrayList<String>() {{
								add("table_3");
								add("table_4");
								add("table_5");
								add("table_6");
							}}
			));
		}};
		List<TableLogCollectorParam> addParams = new ArrayList<TableLogCollectorParam>() {{
			add(new TableLogCollectorParam("connectionId_1", new HashSet<String>() {{
				add("table_4");
				add("table_5");
			}}));
		}};

		List<TableLogCollectorParam> exclusionParams = new ArrayList<TableLogCollectorParam>() {{
			add(new TableLogCollectorParam("connectionId_1", new HashSet<String>() {{
				add("table_1");
				add("table_2");
			}}));
		}};

//		Map<String, LogCollecotrConnConfig> logCollecotrConnConfigMap = new LogCollectorService().addTables(logCollectorConnConfigMap, addParams);
//		System.out.println(logCollecotrConnConfigMap);


	}
}
