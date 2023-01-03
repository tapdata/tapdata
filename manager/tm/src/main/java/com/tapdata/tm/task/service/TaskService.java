package com.tapdata.tm.task.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.map.MapUtil;
import cn.hutool.extra.cglib.CglibUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.autoinspect.constants.AutoInspectConstants;
import com.tapdata.tm.autoinspect.entity.AutoInspectProgress;
import com.tapdata.tm.autoinspect.service.TaskAutoInspectResultsService;
import com.tapdata.tm.autoinspect.utils.AutoInspectUtil;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.handler.ExceptionHandler;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.dag.*;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.*;
import com.tapdata.tm.commons.dag.process.*;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.dag.vo.Operation;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.task.dto.*;
import com.tapdata.tm.commons.task.dto.migrate.MigrateTableDto;
import com.tapdata.tm.commons.task.dto.progress.TaskSnapshotProgress;
import com.tapdata.tm.commons.util.CapitalizedEnum;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflowinsight.dto.DataFlowInsightStatisticsDto;
import com.tapdata.tm.disruptor.constants.DisruptorTopicEnum;
import com.tapdata.tm.disruptor.service.DisruptorService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.file.service.FileService;
import com.tapdata.tm.inspect.constant.InspectResultEnum;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.metadatainstance.service.MetaDataHistoryService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.param.IdParam;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.schedule.service.ScheduleService;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.bean.*;
import com.tapdata.tm.task.constant.SyncType;
import com.tapdata.tm.task.constant.TaskEnum;
import com.tapdata.tm.task.constant.TaskOpStatusEnum;
import com.tapdata.tm.task.constant.TaskStatusEnum;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.entity.TaskRecord;
import com.tapdata.tm.task.param.LogSettingParam;
import com.tapdata.tm.task.param.SaveShareCacheParam;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.task.vo.ShareCacheDetailVo;
import com.tapdata.tm.task.vo.ShareCacheVo;
import com.tapdata.tm.task.vo.TaskDetailVo;
import com.tapdata.tm.task.vo.TaskStatsDto;
import com.tapdata.tm.transform.service.MetadataTransformerItemService;
import com.tapdata.tm.transform.service.MetadataTransformerService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.utils.*;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.ws.enums.MessageType;
import io.tapdata.common.sample.request.Sample;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.group;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.match;

/**
 * @Author:
 * @Date: 2021/11/03
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TaskService extends BaseService<TaskDto, TaskEntity, ObjectId, TaskRepository> {
    private MessageService messageService;
    private SnapshotEdgeProgressService snapshotEdgeProgressService;
    private InspectService inspectService;
    private TaskRunHistoryService taskRunHistoryService;
    private TransformSchemaAsyncService transformSchemaAsyncService;
    private TransformSchemaService transformSchemaService;
    private DataSourceService dataSourceService;
    private MetadataTransformerService transformerService;
    private MetadataInstancesService metadataInstancesService;
    private MetadataTransformerItemService metadataTransformerItemService;
    private MetaDataHistoryService historyService;
    private WorkerService workerService;
    private FileService fileService1;
    private MongoTemplate mongoTemplate;
    private static ThreadPoolExecutor completableFutureThreadPool;
    private UserLogService userLogService;
    private MessageQueueService messageQueueService;
    private UserService userService;
    private DisruptorService disruptorService;
    private MonitoringLogsService monitoringLogsService;
    private TaskAutoInspectResultsService taskAutoInspectResultsService;
    private TaskSaveService taskSaveService;
    private SettingsService settingsService;
    private MeasurementServiceV2 measurementServiceV2;

    private LogCollectorService logCollectorService;

    private static final DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyyMMdd");

    private TaskResetLogService taskResetLogService;

    private TaskCollectionObjService taskCollectionObjService;

    private ExceptionHandler exceptionHandler;

    private StateMachineService stateMachineService;
    private TaskScheduleService taskScheduleService;

    private ScheduleService scheduleService;

    private MetadataDefinitionService metadataDefinitionService;

    public final static String LOG_COLLECTOR_SAVE_ID = "log_collector_save_id";

    public TaskService(@NonNull TaskRepository repository) {
        super(repository, TaskDto.class, TaskEntity.class);
    }

    /**
     * 添加任务， 这里需要拆分子任务，入库需要保证原子性
     *
     * @param taskDto
     * @param user
     * @return
     */
    //@Transactional
    public TaskDto create(TaskDto taskDto, UserDetail user) {
        //新增任务校验
        taskDto.setStatus(TaskDto.STATUS_EDIT);
        log.debug("The save task is complete and the task will be processed, task name = {}", taskDto.getName());
        DAG dag = taskDto.getDag();

        if (dag != null && CollectionUtils.isNotEmpty(dag.getNodes())) {
            List<Node> nodes = dag.getNodes();
            for (Node node : nodes) {
                if (node instanceof DatabaseNode) {
                    taskDto.setSyncType("migrate");
                    break;
                }
            }
        }

        checkTaskName(taskDto.getName(), user, taskDto.getId());

        boolean rename = false;
        if (taskDto.getId() != null) {
            Criteria criteria = Criteria.where("_id").is(taskDto.getId().toHexString());
            Query query = new Query(criteria);
            query.fields().include("name");
            TaskDto old = findOne(query, user);
            String name = old.getName();
            if (StringUtils.isNotBlank(name) && StringUtils.isNotBlank(taskDto.getName()) && !name.equals(taskDto.getName())) {
                rename = true;
            }
        }

        String editVersion = buildEditVersion(taskDto);
        taskDto.setEditVersion(editVersion);

        //模型推演
        //setDefault(taskDto);
        taskDto = save(taskDto, user);
        if (dag != null) {
            dag.setTaskId(taskDto.getId());
            //为了防止上传的json中字段值为null, 导致默认值不生效，二次补上默认值
            if (TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
                transformSchemaAsyncService.transformSchema(dag, user, taskDto.getId());
            } else {
                transformSchemaService.transformSchema(dag, user, taskDto.getId());
                //暂时先这样子更新，为了保存join节点中的推演中产生的primarykeys数据
                long count = dag.getNodes().stream()
                        .filter(n -> NodeEnum.join_processor.name().equals(n.getType()))
                        .count();

                if (count != 0) {
                    Update update = new Update();
                    update.set("dag", taskDto.getDag());
                    updateById(taskDto.getId(), update, user);
                }
            }
        }

        //新增任务成功，新增校验任务
        //inspectService.saveInspect(taskDto, user);
        return taskDto;
    }

    private boolean getBoolValue(Boolean v, boolean defaultValue) {
        if (v == null) {
            return defaultValue;
        }
        return v;
    }

    private int getIntValue(Integer v, int defaultValue) {
        if (v == null) {
            return defaultValue;
        }
        return v;
    }

    private String getStringValue(String v, String defaultValue) {
        if (v == null) {
            return defaultValue;
        }
        return v;
    }

    private void setDefault(TaskDto taskDto) {
        //增量滞后判断时间设置
        taskDto.setIncreHysteresis(getBoolValue(taskDto.getIncreHysteresis(), false));

        //增量数据处理模式，支持批量false  跟逐行true
        taskDto.setIncreOperationMode(getBoolValue(taskDto.getIncreOperationMode(), false));
        // 增量同步并发写入 默认关闭
        taskDto.setIncreSyncConcurrency(getBoolValue(taskDto.getIncreSyncConcurrency(), false));
        // 处理器线程数
        taskDto.setProcessorThreadNum(getIntValue(taskDto.getProcessorThreadNum(), 8));

        // 增量读取条数
        taskDto.setIncreaseReadSize(getIntValue(taskDto.getIncreaseReadSize(), 1));
        // 全量一批读取条数
        taskDto.setReadBatchSize(getIntValue(taskDto.getReadBatchSize(), 500));

        // 自动创建索引
        taskDto.setIsAutoCreateIndex(getBoolValue(taskDto.getIsAutoCreateIndex(), true));

        // 过滤设置
        taskDto.setIsFilter(getBoolValue(taskDto.getIsFilter(), false));

        // 定时调度任务
        taskDto.setIsSchedule(getBoolValue(taskDto.getIsSchedule(), false));

        // 遇到错误时停止
        taskDto.setIsStopOnError(getBoolValue(taskDto.getIsStopOnError(), true));

        // 共享挖掘
        taskDto.setShareCdcEnable(getBoolValue(taskDto.getShareCdcEnable(), false));

        // 类型 [{label: '全量+增量', value: 'initial_sync+cdc'}, {label: '全量', value: 'initial_sync'}, {label: '增量', value: 'cdc'} ]
        taskDto.setType(getStringValue(taskDto.getType(), "initial_sync+cdc"));

        // 目标写入线程数
        taskDto.setWriteThreadSize(getIntValue(taskDto.getWriteThreadSize(), 8));
        taskDto.setSyncType(getStringValue(taskDto.getSyncType(), "sync"));
        taskDto.setDeduplicWriteMode(getStringValue(taskDto.getDeduplicWriteMode(), "intelligent"));

        // 删除标记
        taskDto.set_deleted(false);
        taskDto.setStatuses(new ArrayList<>());
    }

    private void handleMessage(TaskDto taskDto, Map<String, List<Message>> validateMessage) {
        log.debug("handle error message, task id = {}", taskDto.getId());
        if (validateMessage.size() != 0) {
            List<Message> messages = new ArrayList<>();
            for (List<Message> value : validateMessage.values()) {
                messages.addAll(value);
            }
            taskDto.setMessage(messages);
        }
    }

    protected void beforeSave(TaskDto task, UserDetail user) {
        setDefault(task);

        DAG dag = task.getDag();
        if (dag == null) {
            return;
        }
        List<Node> nodes = dag.getNodes();
        if (CollectionUtils.isEmpty(nodes)) {
            return;
        }

        for (Node node : nodes) {
            node.setSchema(null);
            node.setOutputSchema(null);

            //设置主从合并节点的isarray属性，引擎需要用到
            if (node instanceof MergeTableNode) {
                List<MergeTableProperties> mergeProperties = ((MergeTableNode) node).getMergeProperties();
                if (CollectionUtils.isNotEmpty(mergeProperties)) {
                    for (MergeTableProperties mergeProperty : mergeProperties) {
                        MergeTableProperties.autoFillingArray(mergeProperty, false);
                    }
                }
            } else if (node instanceof CacheNode) {
                task.setType(ParentTaskDto.TYPE_CDC);
                TaskDto.SyncPoint syncPoint = new TaskDto.SyncPoint();
                syncPoint.setPointType("current");
                List<Node> sources = dag.getSources();
                if (CollectionUtils.isNotEmpty(sources)) {
                    Node node1 = sources.get(0);
                    TableNode tableNode = (TableNode) node1;
                    syncPoint.setConnectionId(tableNode.getConnectionId());
                }

                task.setSyncPoints(Lists.of(syncPoint));
            }
        }
    }


    /**
     * 根据id修改任务。
     *
     * @param taskDto 任务
     * @param user    用户
     * @return TaskDto
     */
    //@Transactional
    public TaskDto updateById(TaskDto taskDto, UserDetail user) {
        checkTaskInspectFlag(taskDto);
        //根据id校验当前需要更新到任务是否存在
        TaskDto oldTaskDto = null;

        if (taskDto.getId() != null) {
            oldTaskDto = findById(taskDto.getId(), user);
            taskDto.setSyncType(oldTaskDto.getSyncType());

            if (StringUtils.isBlank(taskDto.getAccessNodeType())) {
                taskDto.setAccessNodeType(oldTaskDto.getAccessNodeType());
                taskDto.setAccessNodeProcessId(oldTaskDto.getAccessNodeProcessId());
                taskDto.setAccessNodeProcessIdList(oldTaskDto.getAccessNodeProcessIdList());
            }

            log.debug("old task = {}", oldTaskDto);
        }

        if (oldTaskDto == null) {
            log.debug("task not found, need create new task, task id = {}", taskDto.getId());
            return create(taskDto, user);
        }

        if (oldTaskDto.getEditVersion().equals(taskDto.getEditVersion())) {
            //throw new BizException("Task.OldVersion");
        }

        //改名不能重复
        if (StringUtils.isNotBlank(taskDto.getName()) && !taskDto.getName().equals(oldTaskDto.getName())) {
            checkTaskName(taskDto.getName(), user, taskDto.getId());
        }

        //校验dag
        DAG dag = taskDto.getDag();
        int dagHash = 0;
        if (dag != null) {
            if (TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
                if (CollectionUtils.isNotEmpty(dag.getSourceNode())) {
                    // supplement migrate_field_rename_processor fieldMapping data
                    supplementMigrateFieldMapping(taskDto, user);

                    taskSaveService.syncTaskSetting(taskDto, user);

                    transformSchemaAsyncService.transformSchema(dag, user, taskDto.getId());
                }
            } else {
                transformSchemaService.transformSchema(dag, user, taskDto.getId());
            }
        }
        log.debug("check task dag complete, task id =- {}", taskDto.getId());

        String editVersion = buildEditVersion(taskDto);
        taskDto.setEditVersion(editVersion);

        //更新任务
        log.debug("update task, task dto = {}", taskDto);
        //推演的时候改的，这里必须清空掉。清空只是不会被修改。
        taskDto.setTransformed(null);
        taskDto.setTransformUuid(null);
        taskDto.setTransformDagHash(dagHash);

        taskDto.setWriteBatchSize(null);
        taskDto.setWriteBatchWaitMs(null);

        return save(taskDto, user);

    }

    private void supplementMigrateFieldMapping(TaskDto taskDto, UserDetail userDetail) {
        DAG dag = taskDto.getDag();
        dag.getNodes().forEach(node -> {
            if (node instanceof MigrateFieldRenameProcessorNode) {
                MigrateFieldRenameProcessorNode fieldNode = (MigrateFieldRenameProcessorNode) node;
                LinkedList<TableFieldInfo> fieldsMapping = fieldNode.getFieldsMapping();

                if (CollectionUtils.isNotEmpty(fieldsMapping)) {
                    List<String> tableNames = fieldsMapping.stream()
                            .map(TableFieldInfo::getOriginTableName)
                            .collect(Collectors.toList());
                    DatabaseNode sourceNode = dag.getSourceNode().getFirst();

                    List<MetadataInstancesDto> metaList = metadataInstancesService.findBySourceIdAndTableNameList(sourceNode.getConnectionId(),
                            tableNames, userDetail, taskDto.getId().toHexString());
                    Map<String, List<com.tapdata.tm.commons.schema.Field>> fieldMap = metaList.stream()
                            .collect(Collectors.toMap(MetadataInstancesDto::getQualifiedName, MetadataInstancesDto::getFields));
                    fieldsMapping.forEach(table -> {
                        Operation operation = table.getOperation();
                        LinkedList<FieldInfo> fields = table.getFields();

                        List<String> fieldNames = Lists.newArrayList();
                        if (CollectionUtils.isNotEmpty(fields)) {
                            fieldNames = fields.stream().map(FieldInfo::getSourceFieldName).collect(Collectors.toList());
                        }

                        List<String> hiddenFields = table.getFields().stream().filter(t -> !t.getIsShow())
                                .map(FieldInfo::getSourceFieldName)
                                .collect(Collectors.toList());

                        List<com.tapdata.tm.commons.schema.Field> tableFields = fieldMap.get(table.getQualifiedName());
                        if (CollectionUtils.isNotEmpty(tableFields)) {
                            for (com.tapdata.tm.commons.schema.Field field : tableFields) {
                                String targetFieldName = field.getFieldName();
                                if (!fieldNames.contains(targetFieldName)) {
                                    if (CollectionUtils.isNotEmpty(hiddenFields) && hiddenFields.contains(targetFieldName)) {
                                        continue;
                                    }

                                    if (StringUtils.isNotBlank(operation.getPrefix())) {
                                        targetFieldName = operation.getPrefix().concat(targetFieldName);
                                    }
                                    if (StringUtils.isNotBlank(operation.getSuffix())) {
                                        targetFieldName = targetFieldName.concat(operation.getSuffix());
                                    }
                                    if (StringUtils.isNotBlank(operation.getCapitalized())) {
                                        if (CapitalizedEnum.fromValue(operation.getCapitalized()) == CapitalizedEnum.UPPER) {
                                            targetFieldName = StringUtils.upperCase(targetFieldName);
                                        } else {
                                            targetFieldName = StringUtils.lowerCase(targetFieldName);
                                        }
                                    }
                                    FieldInfo fieldInfo = new FieldInfo(field.getFieldName(), targetFieldName, true, "system");
                                    fields.add(fieldInfo);
                                }
                            }
                        }
                    });
                }
            }
        });
    }


    public TaskDto updateShareCacheTask(String id, SaveShareCacheParam saveShareCacheParam, UserDetail user) {
        TaskDto taskDto = findById(MongoUtils.toObjectId(id));
        parseCacheToTaskDto(saveShareCacheParam, taskDto);

        updateById(taskDto, user);
        start(taskDto.getId(), user);
        return taskDto;

    }


    private void checkTaskName(String newName, UserDetail user) {
        checkTaskName(newName, user, null);
    }

    public void checkTaskName(String newName, UserDetail user, ObjectId id) {
        if (checkTaskNameNotError(newName, user, id)) {
            throw new BizException("Task.RepeatName");
        }
    }

    public boolean checkTaskNameNotError(String newName, UserDetail user, ObjectId id) {

        Criteria criteria = Criteria.where("name").is(newName).and("is_deleted").ne(true);
        if (id != null) {
            criteria.and("_id").ne(id);
        }
        Query query = new Query(criteria);
        long count = count(query, user);
        return count > 0;
    }


    /**
     * 确认保存并启动。
     *
     * @param taskDto 任务
     * @param user    用户
     * @return
     */
    public TaskDto confirmStart(TaskDto taskDto, UserDetail user, boolean confirm) {
        checkDagAgentConflict(taskDto, true);
        taskDto = confirmById(taskDto, user, confirm);
        start(taskDto, user);
        return findById(taskDto.getId(), user);
    }

    /**
     * 编辑和新增都是调用的这个方法
     *
     * @param taskDto 任务
     * @param user    用户
     * @return
     */
    public TaskDto confirmById(TaskDto taskDto, UserDetail user, boolean confirm) {
        if (Objects.nonNull(taskDto.getId())) {
            TaskDto temp = findById(taskDto.getId());
            if (Objects.nonNull(temp) && StringUtils.isBlank(taskDto.getAccessNodeType())) {
                taskDto.setAccessNodeType(temp.getAccessNodeType());
                taskDto.setAccessNodeProcessId(temp.getAccessNodeProcessId());
                taskDto.setAccessNodeProcessIdList(temp.getAccessNodeProcessIdList());
            }
        }
        // check task inspect flag
        checkTaskInspectFlag(taskDto);

        checkDagAgentConflict(taskDto, true);

        checkDDLConflict(taskDto);

        //saveInspect(existedTask, taskDto, user);

        return confirmById(taskDto, user, confirm, false);
    }

    private void checkDDLConflict(TaskDto taskDto) {
        LinkedList<DatabaseNode> sourceNode = taskDto.getDag().getSourceNode();
        if (CollectionUtils.isNotEmpty(sourceNode)) {
            return;
        }
        boolean enableDDL = sourceNode.stream().anyMatch(DataParentNode::getEnableDDL);
        if (!enableDDL) {
            return;
        }

        FunctionUtils.isTureOrFalse(TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())).trueOrFalseHandle(
                () -> {
                    boolean anyMatch = taskDto.getDag().getNodes().stream().anyMatch(n -> n instanceof MigrateJsProcessorNode);
                    FunctionUtils.isTure(anyMatch).throwMessage("Task.DDL.Conflict.Migrate");
                },
                () -> {
                    boolean anyMatch = taskDto.getDag().getNodes().stream().anyMatch(n -> n instanceof JsProcessorNode);
                    FunctionUtils.isTure(anyMatch).throwMessage("Task.DDL.Conflict.Sync");
                }
        );
    }

    public void checkTaskInspectFlag (TaskDto taskDto) {
//        if (taskDto.isAutoInspect() && !taskDto.isCanOpenInspect()) {
//            throw new BizException("Task.CanNotSupportInspect");
//        }
    }

    private void saveInspect(TaskDto existedTask, TaskDto taskDto, UserDetail userDetail) {
        try {
            ObjectId taskId = taskDto.getId();
            if (isInspectPropertyChanged(existedTask, taskDto)) {
                inspectService.deleteByTaskId(taskId.toString());
                inspectService.saveInspect(taskDto, userDetail);
            }
        } catch (Exception e) {
            log.error("新建校验任务出错 {}", e.getMessage());
        }
    }


    //编辑一个复制，要根据属性判断是否删除原来的inspect
    //源数据库或者目标数据库变动，要删除
    //源表或者目标秒变动，要删除
    private Boolean isInspectPropertyChanged(TaskDto existedTask, TaskDto newTask) {
        Boolean changed = false;

        DatabaseNode existedSourceDataNode = (DatabaseNode) getSourceNode(existedTask);
        DatabaseNode existedTargetDataNode = (DatabaseNode) getTargetNode(existedTask);


        DatabaseNode newSourceDataNode = (DatabaseNode) getSourceNode(newTask);
        DatabaseNode newTargetDataNode = (DatabaseNode) getTargetNode(newTask);

        if (!existedSourceDataNode.getName().equals(newSourceDataNode.getName()) ||
                !existedTargetDataNode.getName().equals(newTargetDataNode.getName())
        ) {
            changed = true;
        } else {
            List<SyncObjects> newSyncObjects = newTargetDataNode.getSyncObjects();
            List<SyncObjects> existedSyncObjects = existedTargetDataNode.getSyncObjects();
            Optional<SyncObjects> newTableSyncObject = newSyncObjects.stream().filter(e -> "table".equals(e.getType())).findFirst();
            Optional<SyncObjects> existedTableSyncObject = existedSyncObjects.stream().filter(e -> "table".equals(e.getType())).findFirst();

            if (existedTableSyncObject.isPresent() && newTableSyncObject.isPresent()) {
                List<String> existedSourceTableNames = existedTableSyncObject.get().getObjectNames();
                List<String> newSourceTableNames = newTableSyncObject.get().getObjectNames();
                if (!existedSourceTableNames.equals(newSourceTableNames)) {
                    changed = true;
                }
            }
        /*    SyncObjects existedTopicSyncObject = existedSyncObjects.stream().filter(e -> "topic".equals(e.getType())).findFirst().get();
            SyncObjects existedQueueSyncObject = existedSyncObjects.stream().filter(e -> "queue".equals(e.getType())).findFirst().get();
            if (!existedTopicSyncObject.getObjectNames().equals(newTopicSyncObject.getObjectNames()) ||
                    !existedQueueSyncObject.getObjectNames().equals(newQueueSyncObject.getObjectNames())) {
                changed = true;
            }*/
        }

        return changed;
    }

    /**
     * @see DataFlowEvent#CONFIRM
     * @param taskDto
     * @param user
     * @param confirm
     * @param importTask
     * @return
     */
    public TaskDto confirmById(TaskDto taskDto, UserDetail user, boolean confirm, boolean importTask) {

        DAG dag = taskDto.getDag();

        if (!taskDto.getShareCache()) {
            if (!importTask) {
                Map<String, List<Message>> validateMessage = dag.validate();
                if (!validateMessage.isEmpty()) {
                    throw new BizException("Task.ListWarnMessage", validateMessage);
                }
            }
        }

        updateById(taskDto, user);

        StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.CONFIRM, user);
        if (stateMachineResult.isOk()){
            updateTaskRecordStatus(taskDto, taskDto.getStatus(), user);
        }

        return taskDto;
    }



    public void checkDagAgentConflict(TaskDto taskDto, boolean showListMsg) {
        if (taskDto.getShareCache()) {
            return;
        }

        DAG dag = taskDto.getDag();
        List<String> connectionIdList = new ArrayList<>();
        dag.getNodes().forEach(node -> {
            if (node instanceof DataParentNode) {
                connectionIdList.add(((DataParentNode<?>) node).getConnectionId());
            }
        });
        List<String> taskProcessIdList = taskDto.getAccessNodeProcessIdList();
        List<DataSourceConnectionDto> dataSourceConnectionList = dataSourceService.findInfoByConnectionIdList(connectionIdList);
        Map<String, List<Message>> validateMessage = Maps.newHashMap();
        if (CollectionUtils.isNotEmpty(dataSourceConnectionList)) {
            Map<String, DataSourceConnectionDto> collect = dataSourceConnectionList.stream().collect(Collectors.toMap(s -> s.getId().toHexString(), a -> a, (k1, k2) -> k1));
            String code = "Task.AgentConflict";
            Message message = new Message(code, MessageUtil.getMessage(code), null, null);
            dag.getNodes().forEach(node -> {
                if (node instanceof DataParentNode) {
                    DataParentNode<?> dataParentNode = (DataParentNode<?>) node;
                    DataSourceConnectionDto connectionDto = collect.get(dataParentNode.getConnectionId());
                    Assert.notNull(connectionDto, "task connectionDto is null id:" + dataParentNode.getConnectionId());

                    if (StringUtils.equalsIgnoreCase(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), connectionDto.getAccessNodeType())) {
                        List<String> connectionProcessIds = connectionDto.getAccessNodeProcessIdList();
                        connectionProcessIds.removeAll(taskProcessIdList);
                        if (!StringUtils.equalsIgnoreCase(taskDto.getAccessNodeType(), connectionDto.getAccessNodeType()) ||
                                CollectionUtils.isNotEmpty(connectionProcessIds)) {
                            validateMessage.put(dataParentNode.getId(), Lists.newArrayList(message));
                        }
                    }
                }
            });
        }
        if (!validateMessage.isEmpty()) {
            if (showListMsg) {
                throw new BizException("Task.ListWarnMessage", validateMessage);
            } else {
                Message message = validateMessage.values().iterator().next().get(0);
                throw new BizException(message.getCode(), message.getMsg());
            }
        }
    }

    /**
     * 删除任务
     * @see DataFlowEvent#DELETE
     * @param id   任务id
     * @param user 用户
     */
    public TaskDto remove(ObjectId id, UserDetail user) {
        //查询任务是否存在。
        //查询任务状态是否为停止状态。
        TaskDto taskDto = checkExistById(id, user);

        StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.DELETE, user);

        if (stateMachineResult.isOk()) {
            taskResetLogService.clearLogByTaskId(id.toHexString());
            String connectionName = sendRenewMq(taskDto, user, DataSyncMq.OP_TYPE_DELETE);
            if(StringUtils.isNotEmpty(connectionName)){
                throw new BizException("Clear.Slot",connectionName);
            }
        }
        //afterRemove(taskDto, user);

        return taskDto;
    }

    public void afterRemove(TaskDto taskDto, UserDetail user) {
        //将任务删除标识改成true
        ObjectId id = taskDto.getId();
        update(new Query(Criteria.where("_id").is(id)), Update.update("is_deleted", true));

        //delete AutoInspectResults
        taskAutoInspectResultsService.cleanResultsByTask(taskDto);

        //add message
        if (SyncType.MIGRATE.getValue().equals(taskDto.getSyncType())) {
            messageService.addMigration(taskDto.getName(), taskDto.getId().toString(), MsgTypeEnum.DELETED, Level.WARN, user);
        } else if (SyncType.SYNC.getValue().equals(taskDto.getSyncType())) {
            messageService.addSync(taskDto.getName(), taskDto.getId().toString(), MsgTypeEnum.DELETED, "", Level.WARN, user);
        }

        try {
            metadataInstancesService.deleteTaskMetadata(id.toHexString(), user);
            historyService.deleteTaskMetaHistory(id.toHexString(), user);
        } catch (Exception e) {
            log.warn("remove task, but remove schema error, task name = {}", taskDto.getName());
        }

        //删除收集的对象
        taskCollectionObjService.deleteById(taskDto.getId());
    }

    /**
     * 删除共享缓存
     *
     * @param id   任务id
     * @param user 用户
     */
    public void deleteShareCache(ObjectId id, UserDetail user) {
        //按照产品的意思，不管停止有没有成功，都把这条缓存任务删除调
        try {
            pause(id, user, true);
        } catch (Exception e) {
            log.error("停止异常，但是共享缓存仍然删除 {}", e.getMessage());
        }
        //将任务删除标识改成true
        update(new Query(Criteria.where("_id").is(id)), Update.update("is_deleted", true));
//        remove(id, user);
    }



    /**
     * 拷贝任务
     *
     * @param id   任务id
     * @param user 用户
     * @return
     */
    public TaskDto copy(ObjectId id, UserDetail user) {

        TaskDto taskDto = checkExistById(id, user);
        DAG dag = taskDto.getDag();

        log.debug("old task = {}", taskDto);
        //将所有的node id置为空
        Map<String, String> oldnewNodeIdMap = new HashMap<>();
        if (dag != null) {
            List<Node> nodes = dag.getNodes();
            for (Node node : nodes) {
                if (node == null) {
                    continue;
                }
                String newNodeId = UUID.randomUUID().toString();
                oldnewNodeIdMap.put(node.getId(), newNodeId);
                node.setId(newNodeId);
            }


            for (Node node : nodes) {
                //需要特殊处理一下主从合并节点
                if (node instanceof MergeTableNode) {
                    List<MergeTableProperties> mergeProperties = ((MergeTableNode) node).getMergeProperties();
                    String json = JsonUtil.toJsonUseJackson(mergeProperties);
                    if (json == null) {
                        continue;
                    }

                    for (Map.Entry<String, String> entry: oldnewNodeIdMap.entrySet()) {
                        json = json.replace(entry.getKey(), entry.getValue());
                    }

                    List<MergeTableProperties> mergeTableProperties = JsonUtil.parseJsonUseJackson(json, new TypeReference<List<MergeTableProperties>>() {
                    });

                    ((MergeTableNode) node).setMergeProperties(mergeTableProperties);
                } else if (node instanceof MigrateFieldRenameProcessorNode) {

                }
            }

            List<Edge> edges = dag.getEdges();
            for (Edge edge : edges) {
                edge.setId(UUID.randomUUID().toString());
                edge.setSource(oldnewNodeIdMap.get(edge.getSource()));
                edge.setTarget(oldnewNodeIdMap.get(edge.getTarget()));
            }

            nodes.stream().filter(n -> n instanceof JoinProcessorNode).forEach(n -> {
                JoinProcessorNode n1 = (JoinProcessorNode) n;
                n1.setLeftNodeId(oldnewNodeIdMap.get(n1.getLeftNodeId()));
                n1.setRightNodeId(oldnewNodeIdMap.get(n1.getRightNodeId()));
            });

            Dag dag1 = new Dag();
            dag1.setNodes(nodes);
            dag1.setEdges(edges);
            DAG build = DAG.build(dag1);
            taskDto.setDag(build);
        }

        String originalName = taskDto.getName();

        //将任务id设置为null,状态改为编辑中
        taskDto.setId(null);
        taskDto.setTaskRecordId(null);
        taskDto.setAgentId(null);

        //设置复制名称
        String copyName = taskDto.getName() + " - Copy";
        while (true) {
            try {
                //插入复制的数据源
                checkTaskName(copyName, user);
                break;
            } catch (BizException e) {
                if ("Task.RepeatName".equals(e.getErrorCode())) {
                    copyName = copyName + " - Copy";
                } else {
                    throw e;
                }
            }
        }

        log.debug("copy task success, task name = {}", copyName);

        taskDto.setName(copyName);
        taskDto.setStatus(TaskDto.STATUS_EDIT);
        taskDto.setStatuses(new ArrayList<>());
        taskDto.setStartTime(null);
        taskDto.setStopTime(null);
        taskDto.setErrorTime(null);
        Map<String, Object> attrs = taskDto.getAttrs();
        if (null != attrs) {
            attrs.remove("edgeMilestones");
            attrs.remove("syncProgress");
        }
        //taskDto.setTemp(null);

        //创建新任务， 直接调用事务不会生效
        TaskService taskService = SpringContextHelper.getBean(TaskService.class);

        log.info("create new task, task = {}", taskDto);
        taskDto = taskService.confirmById(taskDto, user, true);
        //taskService.flushStatus(taskDto, user);

        try {
            userLogService.addUserLog(Modular.MIGRATION, com.tapdata.tm.userLog.constant.Operation.COPY, user, id.toHexString(), originalName, taskDto.getName(), false);
        } catch (Exception e) {
            log.error("Logging to copy task fail", e);
        }


        // after copy could deduce model
        //transformSchemaAsyncService.transformSchema(dag, user, taskDto.getId());

        return taskDto;
    }

    /**
     * 查询任务运行历史记录
     *
     * @param filter filter
     * @param user   用户
     * @return
     */
    public Page<TaskRunHistoryDto> queryTaskRunHistory(Filter filter, UserDetail user) {
        return taskRunHistoryService.find(filter, user);
    }


    private static String buildEditVersion(TaskDto taskDto) {
        return String.valueOf(System.currentTimeMillis());
    }



    /**
     * 重置任务
     * @see DataFlowEvent#RENEW
     * @param id   任务id
     * @param user 用户
     */
    public void renew(ObjectId id, UserDetail user) {
        TaskDto taskDto = checkExistById(id, user);
//        String status = taskDto.getStatus();
//        if (TaskDto.STATUS_WAIT_START.equals(status)) {
//            return;
//        }

//        //只有暂停或者停止状态可以重置
//        if (!TaskOpStatusEnum.to_renew_status.v().contains(status)) {
//            //需要停止的时候才可以操作
//            log.info("The current status of the task does not allow resetting, task name = {}, status = {}", taskDto.getName(), status);
//
//            if (TaskDto.STATUS_DELETING.equals(status) || TaskDto.STATUS_DELETE_FAILED.equals(status)) {
//                throw new BizException("Task.Deleted");
//            }
//            throw new BizException("Task.statusIsNotStop");
//        } else


        StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.RENEW, user);
        if (stateMachineResult.isOk()) {
            boolean needCreateRecord = !TaskDto.STATUS_WAIT_START.equals(taskDto.getStatus());
            taskDto.setNeedCreateRecord(needCreateRecord);

            log.debug("check task status complete, task name = {}", taskDto.getName());
            taskResetLogService.clearLogByTaskId(id.toHexString());
            sendRenewMq(taskDto, user, DataSyncMq.OP_TYPE_RESET);
        }
        //afterRenew(taskDto, user);
    }

    public void afterRenew(TaskDto taskDto, UserDetail user) {
        StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.RENEW_DEL_SUCCESS, user);
        if (stateMachineResult.isFail()) {
            log.info("modify renew success failed, task id = {}", taskDto.getId());
            return;
        }

        renewAgentMeasurement(taskDto.getId().toString());
        log.debug("renew task complete, task name = {}", taskDto.getName());

        String lastTaskRecordId = new ObjectId().toString();
        //更新任务信息
        Update update = Update.update(TaskDto.LASTTASKRECORDID, lastTaskRecordId).unset("temp");
        updateById(taskDto.getId(), update, user);

        //清除校验结果
        taskAutoInspectResultsService.cleanResultsByTask(taskDto);

        if (taskDto.isNeedCreateRecord()) {
            taskDto.setStatus(TaskDto.STATUS_WAIT_START);
            taskDto.setAgentId(null);
            taskDto.setTaskRecordId(lastTaskRecordId);
            taskDto.setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());
            taskDto.setAccessNodeProcessIdList(Lists.newArrayList());
            TaskEntity taskSnapshot = new TaskEntity();
            BeanUtil.copyProperties(taskDto, taskSnapshot);

            disruptorService.sendMessage(DisruptorTopicEnum.CREATE_RECORD,
                    new TaskRecord(lastTaskRecordId, taskDto.getId().toHexString(), taskSnapshot, user.getUserId(), new Date()));
        }

        // publish queue
        renewNotSendMq(taskDto, user);
    }


    /**
     * 根据id校验任务是否存在
     *
     * @param id   id
     * @param user 用户
     * @return
     */
    public TaskDto checkExistById(ObjectId id, UserDetail user) {
        TaskDto taskDto = findById(id, user);
        if (taskDto == null) {
            throw new BizException("Task.NotFound", "The copied task does not exist");
        }

        return taskDto;
    }

    /**
     * 根据id校验任务是否存在
     *
     * @param id   id
     * @param user 用户
     * @return
     */
    public TaskDto checkExistById(ObjectId id, UserDetail user, String... fields) {
        Query query = new Query(Criteria.where("_id").is(id));
        if (fields != null && fields.length > 0) {
            query.fields().include(fields);
        }
        TaskDto taskDto = findOne(query, user);
        if (taskDto == null) {
            throw new BizException("Task.NotFound", "The copied task does not exist");
        }

        return taskDto;
    }

    /**
     * 根据id校验任务是否存在
     *
     * @param id   id
     * @return
     */
    public TaskDto checkExistById(ObjectId id, String... fields) {
        Query query = new Query(Criteria.where("_id").is(id));
        if (fields != null && fields.length > 0) {
            query.fields().include(fields);
        }
        TaskDto taskDto = findOne(query);
        if (taskDto == null) {
            throw new BizException("Task.NotFound", "The copied task does not exist");
        }

        return taskDto;
    }


    public List<MutiResponseMessage> batchStart(List<ObjectId> taskIds, UserDetail user,
                                                HttpServletRequest request, HttpServletResponse response) {
        List<MutiResponseMessage> responseMessages = new ArrayList<>();
        List<TaskDto> taskDtos = findAllTasksByIds(taskIds.stream().map(ObjectId::toHexString).collect(Collectors.toList()));
        for (TaskDto task : taskDtos) {
            MutiResponseMessage mutiResponseMessage = new MutiResponseMessage();
            mutiResponseMessage.setId(task.getId().toHexString());

            try {
                checkDagAgentConflict(task, false);
                start(task, user);
            } catch (Exception e) {
                log.warn("start task exception, task id = {}, e = {}", task.getId(), ThrowableUtils.getStackTraceByPn(e));
                if (e instanceof BizException) {
                    mutiResponseMessage.setCode(((BizException) e).getErrorCode());
                    mutiResponseMessage.setMessage(MessageUtil.getMessage(((BizException) e).getErrorCode()));
                } else {
                    try {
                        ResponseMessage<?> responseMessage = exceptionHandler.handlerException(e, request, response);
                        mutiResponseMessage.setCode(responseMessage.getCode());
                        mutiResponseMessage.setMessage(responseMessage.getMessage());
                    } catch (Throwable ex) {
                        log.warn("delete task, handle exception error, task id = {}",  task.getId().toHexString());
                    }
                }
            }
            responseMessages.add(mutiResponseMessage);
        }
        return responseMessages;
    }

    public List<MutiResponseMessage> batchStop(List<ObjectId> taskIds, UserDetail user,
                                               HttpServletRequest request,
                                               HttpServletResponse response) {
        List<MutiResponseMessage> responseMessages = new ArrayList<>();
        for (ObjectId taskId : taskIds) {
            MutiResponseMessage mutiResponseMessage = new MutiResponseMessage();
            mutiResponseMessage.setId(taskId.toHexString());
            try {
                pause(taskId, user, false);
            } catch (Exception e) {
                log.warn("stop task exception, task id = {}, e = {}", taskId, e);
                if (e instanceof BizException) {
                    mutiResponseMessage.setCode(((BizException) e).getErrorCode());
                    mutiResponseMessage.setMessage(MessageUtil.getMessage(((BizException) e).getErrorCode()));
                } else {
                    try {
                        ResponseMessage<?> responseMessage = exceptionHandler.handlerException(e, request, response);
                        mutiResponseMessage.setCode(responseMessage.getCode());
                        mutiResponseMessage.setMessage(responseMessage.getMessage());
                    } catch (Throwable ex) {
                        log.warn("delete task, handle exception error, task id = {}",  taskId.toHexString());
                    }
                }
            }
            responseMessages.add(mutiResponseMessage);
        }
        return responseMessages;
    }

    public List<MutiResponseMessage> batchDelete(List<ObjectId> taskIds, UserDetail user,
                                                 HttpServletRequest request,
                                                 HttpServletResponse response) {

        List<MutiResponseMessage> responseMessages = new ArrayList<>();
        for (ObjectId taskId : taskIds) {
            MutiResponseMessage mutiResponseMessage = new MutiResponseMessage();
            mutiResponseMessage.setId(taskId.toHexString());
            try {
                TaskDto taskDto = remove(taskId, user);
                //todo  需不需要手动删除
                inspectService.deleteByTaskId(taskId.toString());

                mutiResponseMessage.setCode(ResponseMessage.OK);
                mutiResponseMessage.setMessage(ResponseMessage.OK);
            } catch (Exception e) {
                log.warn("delete task exception, task id = {}, e = {}", taskId, ThrowableUtils.getStackTraceByPn(e));
                if (e instanceof BizException) {
                    mutiResponseMessage.setCode(((BizException) e).getErrorCode());
                    if("Clear.Slot".equals((((BizException) e).getErrorCode()))){
                        mutiResponseMessage.setMessage(e.getMessage());
                    }else{
                        mutiResponseMessage.setMessage(MessageUtil.getMessage(((BizException) e).getErrorCode()));
                    }
                } else {
                    try {
                        ResponseMessage<?> responseMessage = exceptionHandler.handlerException(e, request, response);
                        mutiResponseMessage.setCode(responseMessage.getCode());
                        mutiResponseMessage.setMessage(responseMessage.getMessage());
                    } catch (Throwable ex) {
                        log.warn("delete task, handle exception error, task id = {}",  taskId.toHexString());
                    }
                }
            }
            responseMessages.add(mutiResponseMessage);
        }
        return responseMessages;
    }


    public List<MutiResponseMessage> batchRenew(List<ObjectId> taskIds, UserDetail user,
                                                HttpServletRequest request, HttpServletResponse response) {
        List<MutiResponseMessage> responseMessages = new ArrayList<>();
        for (ObjectId taskId : taskIds) {
            MutiResponseMessage mutiResponseMessage = new MutiResponseMessage();
            mutiResponseMessage.setId(taskId.toHexString());
            try {
                renew(taskId, user);
                mutiResponseMessage.setCode(ResponseMessage.OK);
                mutiResponseMessage.setMessage(ResponseMessage.OK);
            } catch (Exception e) {
                log.warn("renew task exception, task id = {}, e = {}", taskId, e);
                if (e instanceof BizException) {
                    mutiResponseMessage.setCode(((BizException) e).getErrorCode());
                    mutiResponseMessage.setMessage(MessageUtil.getMessage(((BizException) e).getErrorCode()));
                } else {
                    try {
                        ResponseMessage<?> responseMessage = exceptionHandler.handlerException(e, request, response);
                        mutiResponseMessage.setCode(responseMessage.getCode());
                        mutiResponseMessage.setMessage(responseMessage.getMessage());
                    } catch (Throwable ex) {
                        log.warn("delete task, handle exception error, task id = {}",  taskId.toHexString());
                    }
                }
            }
            responseMessages.add(mutiResponseMessage);
        }
        return responseMessages;
    }

    /**
     * 任务的状态就通过 statues 来判断，statues 为空的时候，任务是编辑中
     *
     * @param filter     optional, page query parameters
     * @param userDetail
     * @return
     */
    public Page<TaskDto> scanTask(Filter filter, UserDetail userDetail) {
        return super.find(filter, userDetail);
    }
    public Page<TaskDto> find(Filter filter, UserDetail userDetail) {
        if (isAgentReq()) {
            return super.find(filter, userDetail);
        }

        Where where = filter.getWhere();
        if (where == null) {
            where = new Where();
            filter.setWhere(where);
        }
        if (where.get("status") == null) {
            Document statusCondition = new Document();
            statusCondition.put("$nin", Lists.of(TaskDto.STATUS_DELETE_FAILED, TaskDto.STATUS_DELETING));
            where.put("status", statusCondition);
        }
        //过滤掉挖掘任务
        String syncType = (String) where.get("syncType");
        if (StringUtils.isBlank(syncType)) {
            HashMap<String, String> logCollectorFilter = new HashMap<>();
            logCollectorFilter.put("$ne", "logCollector");
            where.put("syncType", logCollectorFilter);
        }

        //过滤调共享缓存任务
        HashMap<String, Object> notShareCache = new HashMap<>();
        notShareCache.put("$ne", true);
        where.put("shareCache", notShareCache);


        Boolean deleted = (Boolean) where.get("is_deleted");
        if (deleted == null) {
            Document document = new Document();
            document.put("$ne", true);
            where.put("is_deleted", document);
        }


        Page<TaskDto> taskDtoPage = new Page<>();
        List<TaskDto> items = new ArrayList<>();
        if (where.get("syncType") != null && (where.get("syncType") instanceof String)) {
            String synType = (String) where.get("syncType");
            if (SyncType.MIGRATE.getValue().equals(synType)) {
                taskDtoPage = findDataCopyList(filter, userDetail);
            } else if (SyncType.SYNC.getValue().equals(synType)) {
                taskDtoPage = findDataDevList(filter, userDetail);
            }
            items = taskDtoPage.getItems();
        } else {
            taskDtoPage = super.find(filter, userDetail);
        }


        if (CollectionUtils.isNotEmpty(items)) {
            //添加上推演进度情况
            List<String> taskIds = items.stream().map(b -> b.getId().toHexString()).collect(Collectors.toList());
            Criteria criteria = Criteria.where("dataFlowId").in(taskIds);
            Query query = new Query(criteria);
            List<MetadataTransformerDto> transformerDtos = transformerService.findAll(query);
            if (CollectionUtils.isNotEmpty(transformerDtos)) {
                Map<String, List<MetadataTransformerDto>> transformMap = transformerDtos.stream().collect(Collectors.groupingBy(m -> m.getDataFlowId()));

                for (TaskDto item : items) {
                    List<MetadataTransformerDto> metadataTransformerDtos = transformMap.get(item.getId().toString());
                    if (CollectionUtils.isEmpty(metadataTransformerDtos)) {
                        item.setTransformProcess(0);
                        item.setTransformStatus(MetadataTransformerDto.StatusEnum.running.name());
                    } else {
                        String status = MetadataTransformerDto.StatusEnum.done.name();
                        for (MetadataTransformerDto dto : metadataTransformerDtos) {
                            if (MetadataTransformerDto.StatusEnum.error.name().equals(dto.getStatus())) {
                                status = MetadataTransformerDto.StatusEnum.error.name();
                                break;
                            }
                            if (MetadataTransformerDto.StatusEnum.running.name().equals(dto.getStatus())) {
                                status = MetadataTransformerDto.StatusEnum.running.name();
                            }
                        }

                        item.setTransformStatus(status);

                        int total = 0;
                        int finished = 0;
                        for (MetadataTransformerDto dto : metadataTransformerDtos) {
                            total += dto.getTotal();
                            finished += dto.getFinished();
                        }
                        double process = finished / (total * 1d);
                        if (process > 1) {
                            process = 1;
                        }
                        item.setTransformProcess(((int) (process * 100)) / 100d);
                    }
                    //产品认为不把STATUS_SCHEDULE_FAILED  展现到页面上，STATUS_SCHEDULE_FAILED就直接转为error状态
                    item.setStatus(TaskStatusEnum.getMapStatus(item.getStatus()));
                }
            } else {
                for (TaskDto item : items) {
                    item.setTransformProcess(0);
                    item.setTransformStatus(MetadataTransformerDto.StatusEnum.running.name());
                }
            }


        }

        return taskDtoPage;
    }


    /**
     * 查询数据复制任务，直接用status查
     * 列表的筛选需要增加一个逻辑
     * 1、当搜索status为 edit 的任务时   需要过滤掉有子任务的任务
     * 2、当搜索status为 ready 的任务是，需要返回status为edit且有子任务的任务
     *
     * 补充数据校验状态，搜索不一致后显示不一致的任务并在任务状态旁显示提示图标,用户可点击运行监控查看校验结果
     *
     * @param userDetail
     * @return
     */
    private Page<TaskDto> findDataCopyList(Filter filter, UserDetail userDetail) {
        Where where = filter.getWhere();

        Criteria criteria = Criteria.where("is_deleted").ne(true).and("user_id").is(userDetail.getUserId());
        Criteria orToCriteria = parseOrToCriteria(where);

        // Supplementary data verification status
        Object inspectResult = where.get("inspectResult");
        if (Objects.nonNull(inspectResult)) {
            where.remove("inspectResult");

            boolean passed = inspectResult instanceof String && (inspectResult.toString().equals("agreement"));
            List<InspectDto> inspectDtoList = inspectService.findByResult(passed);

            List<String> taskIdList = inspectDtoList.stream().map(InspectDto::getFlowId).filter(StringUtils::isNotBlank).distinct().collect(Collectors.toList());
            criteria.and("_id").in(taskIdList.stream().map(ObjectId::new).collect(Collectors.toList()));
        }

        Query query = new Query();
        parseWhereCondition(where, query);
        parseFieldCondition(filter, query);

        criteria.andOperator(orToCriteria);
        query.addCriteria(criteria);

        TmPageable tmPageable = new TmPageable();
        Integer page = (filter.getSkip() / filter.getLimit()) + 1;
        tmPageable.setPage(page);
        tmPageable.setSize(filter.getLimit());

        String order = filter.getOrder() == null ? "createTime DESC" : String.valueOf(filter.getOrder());
        String sortKey = order.contains("currentEventTimestamp") ? "currentEventTimestamp" : "createTime";
        if (order.contains("ASC")) {
            tmPageable.setSort(Sort.by(sortKey).ascending());
        } else {
            tmPageable.setSort(Sort.by(sortKey).descending());
        }

        long total = repository.getMongoOperations().count(query, TaskEntity.class);
        List<TaskEntity> taskEntityList = repository.getMongoOperations().find(query.with(tmPageable), TaskEntity.class);
        List<TaskDto> taskDtoList = com.tapdata.tm.utils.BeanUtil.deepCloneList(taskEntityList, TaskDto.class);

        // Supplementary data verification status
        if (CollectionUtils.isNotEmpty(taskDtoList)) {
            List<String> taskIdList = taskDtoList.stream().map(t -> t.getId().toHexString()).collect(Collectors.toList());
            List<InspectDto> inspectDtoList = inspectService.findByTaskIdList(taskIdList);
            Map<String, List<InspectDto>> inspectDtoMap = inspectDtoList.stream().collect(Collectors.groupingBy(InspectDto::getFlowId));

            for (TaskDto taskDto : taskDtoList) {
                if (inspectDtoMap.containsKey(taskDto.getId().toHexString())) {
                    InspectDto inspectDto = inspectDtoMap.get(taskDto.getId().toHexString()).get(0);
                    taskDto.setInspectId(inspectDto.getId().toHexString());
                    taskDto.setShowInspectTips(StringUtils.equals(InspectResultEnum.FAILED.getValue(), inspectDto.getResult()));
                }
            }
        }

        Page<TaskDto> result = new Page<>();
        result.setItems(taskDtoList);
        result.setTotal(total);

        return result;
    }

    /**
     * 查询数据开发任务，用statuses查
     *
     * @param userDetail
     * @return
     */
    private Page<TaskDto> findDataDevList(Filter filter, UserDetail userDetail) {
        Query query = repository.filterToQuery(filter);
        query.limit(100000);
        query.skip(0);
        long count = repository.count(query, userDetail);
        query.skip(filter.getSkip());
        query.limit(filter.getLimit());
        List<TaskEntity> taskEntityList = repository.findAll(query, userDetail);
        List<TaskDto> taskDtoList = com.tapdata.tm.utils.BeanUtil.deepCloneList(taskEntityList, TaskDto.class);
        Page<TaskDto> taskDtoPage = new Page<>();
        taskDtoPage.setTotal(count);
        taskDtoPage.setItems(taskDtoList);
        return taskDtoPage;
    }

    private Node getSourceNode(TaskDto taskDto) {
        DAG dag = taskDto.getDag();
        if (dag == null) {
            return null;
        }

        List<Edge> edges = dag.getEdges();
        if (CollectionUtils.isNotEmpty(edges)) {
            Edge edge = edges.get(0);
            String source = edge.getSource();
            List<Node> nodeList = taskDto.getDag().getNodes();
            if (CollectionUtils.isNotEmpty(nodeList)) {
                List<Node> sourceList = nodeList.stream().filter(Node -> null != Node && null != Node.getId() && source.equals(Node.getId())).collect(Collectors.toList());
                if (CollectionUtils.isNotEmpty(sourceList) && null != sourceList.get(0)) {
                    return sourceList.get(0);
                }
            }
        }
        return null;
    }

    private Node getTargetNode(TaskDto taskDto) {
        List<Edge> edges = taskDto.getDag().getEdges();
        if (CollectionUtils.isNotEmpty(edges)) {
            Edge edge = edges.get(0);
            String target = edge.getTarget();
            List<Node> nodeList = taskDto.getDag().getNodes();
            if (CollectionUtils.isNotEmpty(nodeList)) {
                List<Node> sourceList = nodeList.stream().filter(Node -> null != Node && null != Node.getId() && target.equals(Node.getId())).collect(Collectors.toList());
                if (CollectionUtils.isNotEmpty(sourceList) && null != sourceList.get(0)) {
                    return sourceList.get(0);
                }
            }
        }
        return null;
    }

    public static String printInfos(DAG dag) {
        try {
            StringBuilder sb = new StringBuilder();
            List<Edge> edges = dag.getEdges();
            for (Edge edge : edges) {
                sb.append(dag.getNode(edge.getSource()).getName()).append(" -> ").append(dag.getNode(edge.getTarget()).getName()).append("\r\n");
            }
            return sb.toString();
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * 根据key查询是否存在一个共享缓存，
     *
     * @param key key可以是一个数据源，可以是一个表名，index,topic, collection
     * @return 返回共享缓存任务的id, 起止时间
     */
    public LogCollectorResult searchLogCollector(String key) {
        Criteria criteriaConnectionIds = Criteria.where("connectionIds").elemMatch(Criteria.where("$eq").is(key));
        Criteria criteriaTables = Criteria.where("tableNames").elemMatch(Criteria.where("$eq").is(key));
        Criteria criteria = Criteria.where("is_deleted").is(false).and("dag.nodes").elemMatch(Criteria.where("type").is("logCollector")
                .orOperator(criteriaConnectionIds, criteriaTables));

        Query query = new Query(criteria);
        //query.fields().include()
        return new LogCollectorResult();
    }


    /**
     * 创建共享缓存
     *
     * @param user
     * @return
     */
    public TaskDto createShareCacheTask(SaveShareCacheParam saveShareCacheParam, UserDetail user,
                                        HttpServletRequest request,
                                        HttpServletResponse response) {
        TaskDto taskDto = new TaskDto();

        parseCacheToTaskDto(saveShareCacheParam, taskDto);
        taskDto = confirmById(taskDto, user, true);
        //新建完成马上调度
        List<ObjectId> taskIds = Arrays.asList(taskDto.getId());
        batchStart(taskIds, user, request, response);
        return taskDto;
    }

    /**
     * 获取共享缓存列表
     *
     * @param filter
     * @param userDetail
     * @return
     */
    public Page<ShareCacheVo> findShareCache(Filter filter, UserDetail userDetail) {
        Where where = filter.getWhere();
        List<String> connectionIds = new ArrayList();
        if (null != where.get("connectionName")) {
            Map connectionName = (Map) where.remove("connectionName");
            String conectionNameStr = (String) connectionName.remove("$regex");
            connectionIds = dataSourceService.findIdByName(conectionNameStr);
            Map<String, Object> connectioIdMap = new HashMap<>();
            connectioIdMap.put("$in", connectionIds);
            where.put("dag.nodes.connectionId", connectioIdMap);
        }


        Page page = super.find(filter, userDetail);
        List<TaskDto> taskDtos = page.getItems();
        List<ShareCacheVo> shareCacheVos = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(taskDtos)) {
            for (TaskDto taskDto : taskDtos) {
                ShareCacheVo shareCacheVo = new ShareCacheVo();
                shareCacheVo.setStatus(taskDto.getStatus());
                Node sourceNode = getSourceNode(taskDto);
                if (null != sourceNode) {
                    String connectionId = ((DataParentNode) sourceNode).getConnectionId();
                    DataSourceConnectionDto dataSourceConnectionDto = dataSourceService.findOne(Query.query(Criteria.where("id").is(connectionId)));
                    if (null != dataSourceConnectionDto) {
                        shareCacheVo.setConnectionId(connectionId);
                        shareCacheVo.setConnectionName(dataSourceConnectionDto.getName());
                    }
                    String tableName = ((TableNode) sourceNode).getTableName();
                    shareCacheVo.setTableName(tableName);
                    shareCacheVo.setCreateTime(taskDto.getCreateAt());
                    if (null != sourceNode.getAttrs()) {
                        shareCacheVo.setFields((List<String>) sourceNode.getAttrs().get("fields"));
                    }

//                    MeasurementEntity measurementEntity = measurementService.findByTaskIdAndNodeId(taskDto.getId().toString(), sourceNode.getId());
//                    if (null != measurementEntity && null != measurementEntity.getStatistics() && null != measurementEntity.getStatistics().get("cdcTime")) {
//                        Map<String, Number> statistics = measurementEntity.getStatistics();
//                        //cdc 值为integer or long 需要处理，cdcTime   也可能为0 需要处理
//                        Number cdcTime = statistics.get("cdcTime");
//                        Date cdcTimeDate = null;
//                        if (cdcTime instanceof Integer) {
//                            cdcTimeDate = new Date(cdcTime.longValue());
//                        } else if (cdcTime instanceof Long) {
//                            cdcTimeDate = new Date((Long) cdcTime);
//                        }
//                        shareCacheVo.setCacheTimeAt(cdcTimeDate);
//                    }
                }

                CacheNode cacheNode = (CacheNode) getTargetNode(taskDto);
                if (null != cacheNode) {
                    BeanUtil.copyProperties(cacheNode, shareCacheVo);
                }

                shareCacheVo.setName(taskDto.getName());
                shareCacheVo.setCreateUser(taskDto.getCreateUser());
                shareCacheVo.setStatus(taskDto.getStatus());
                shareCacheVo.setStatuses(taskDto.getStatuses());
                shareCacheVo.setId(taskDto.getId().toString());
                shareCacheVos.add(shareCacheVo);
            }
        }
        page.setItems(shareCacheVos);
        return page;
    }

    public ShareCacheDetailVo findShareCacheById(String id) {
        TaskDto taskDto = findById(MongoUtils.toObjectId(id));
        Node sourceNode = getSourceNode(taskDto);
        CacheNode targetNode = (CacheNode) getTargetNode(taskDto);
        ShareCacheDetailVo shareCacheDetailVo = new ShareCacheDetailVo();
        shareCacheDetailVo.setName(taskDto.getName());
        String connectionId = ((DataNode) sourceNode).getConnectionId();
        DataSourceConnectionDto connectionDto = dataSourceService.findOne(Query.query(Criteria.where("id").is(connectionId)));
        if (null != connectionDto) {
            shareCacheDetailVo.setConnectionId(connectionDto.getId().toString());
            shareCacheDetailVo.setConnectionName(connectionDto.getName());
        }
        shareCacheDetailVo.setTableName(((TableNode) sourceNode).getTableName());
        shareCacheDetailVo.setCacheKeys(targetNode.getCacheKeys());

        if (null != sourceNode.getAttrs()) {
            shareCacheDetailVo.setFields((List<String>) sourceNode.getAttrs().get("fields"));
        }
        shareCacheDetailVo.setMaxRows(targetNode.getMaxRows());
        shareCacheDetailVo.setMaxMemory(targetNode.getMaxMemory());
        shareCacheDetailVo.setTtl(TimeUtil.parseSecondsToDay(targetNode.getTtl()));

        return shareCacheDetailVo;
    }

    private TaskDto parseCacheToTaskDto(SaveShareCacheParam saveShareCacheParam, TaskDto taskDto) {
        taskDto.setStatus(TaskDto.STATUS_EDIT);
        taskDto.setType(ParentTaskDto.TYPE_CDC);
        taskDto.setShareCache(true);
        taskDto.setLastUpdAt(new Date());
        taskDto.setName(saveShareCacheParam.getName());

        DAG dag = taskDto.getDag();
        String sourceId;
        if (dag != null && CollectionUtils.isNotEmpty(dag.getSources())) {
            sourceId = dag.getSources().get(0).getId();
        } else {
            sourceId = UUIDUtil.get64UUID();
        }

        String targetId;
        if (dag != null && CollectionUtils.isNotEmpty(dag.getTargets())) {
            targetId = dag.getTargets().get(0).getId();
        } else {
            targetId = UUIDUtil.get64UUID();
        }


        List<Edge> edges = saveShareCacheParam.getEdges();
        List<Map> nodeList = (List<Map>) saveShareCacheParam.getDag().get("nodes");
        if (CollectionUtils.isEmpty(edges) && CollectionUtils.isNotEmpty(nodeList)) {
            edges = new ArrayList<>();
            Edge edge = new Edge();

            Map sourceNodeMap = nodeList.get(0);
            TableNode tableNode = new TableNode();
            tableNode.setTableName((String) sourceNodeMap.get("tableName"));
            tableNode.setType("table");
            tableNode.setDatabaseType((String) sourceNodeMap.get("databaseType"));
            tableNode.setConnectionId((String) sourceNodeMap.get("connectionId"));

            Map<String, Object> attrs = new HashMap();
            if (null != sourceNodeMap.get("attrs")) {
                attrs = (Map<String, Object>) sourceNodeMap.get("attrs");
                tableNode.setAttrs(attrs);
            }

            Map targetNodeMap = nodeList.get(1);
            CacheNode cacheNode = new CacheNode();
            cacheNode.setCacheKeys((String) targetNodeMap.get("cacheKeys"));
            Integer maxRows = MapUtil.getInt(targetNodeMap, "maxRows");
            Integer maxMemory = MapUtil.getInt(targetNodeMap, "maxMemory");
            cacheNode.setMaxRows(maxRows == null ? Integer.MAX_VALUE : maxRows.longValue());
            cacheNode.setMaxMemory(maxMemory == null ? 500 : maxMemory.intValue());
            Integer ttl = MapUtil.getInt(targetNodeMap, "ttl");
            cacheNode.setTtl(ttl == null ? Integer.MAX_VALUE : TimeUtil.parseDayToSeconds(ttl));

            edge.setSource(sourceId);
            edge.setTarget(targetId);
            edges.add(edge);
            tableNode.setId(sourceId);
            cacheNode.setId(targetId);
            cacheNode.setFields((List<String>) attrs.get("fields"));
            cacheNode.setCacheName(saveShareCacheParam.getName());

            List<Node> nodes = new ArrayList<>();
            nodes.add(tableNode);
            nodes.add(cacheNode);
            Dag dag1 = new Dag(edges, nodes);
            DAG build = DAG.build(dag1);
            taskDto.setDag(build);
        }
        return taskDto;
    }


    /**
     * migrate 同步任务  即数据复制
     * sync   迁移  即数据开发
     * logCollector 挖掘任务
     *
     * @param user
     * @return
     */
    public Map<String, Object> chart(UserDetail user) {
        Map<String, Object> resultChart = new HashMap<>();
        Criteria criteria = Criteria.where("user_id").is(user.getUserId())
                .and("is_deleted").ne(true)
                .and("syncType").in(TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC)
                .and("status").nin(TaskDto.STATUS_DELETING, TaskDto.STATUS_DELETE_FAILED)
                //共享缓存的任务设计的有点问题
                .and("shareCache").ne(true);


        Query query = Query.query(criteria);
        query.fields().include("syncType", "status", "statuses");
        //把任务都查询出来
        List<TaskDto> taskDtoList = findAllDto(query, user);
        Map<String, List<TaskDto>> syncTypeToTaskList = taskDtoList.stream().collect(Collectors.groupingBy(TaskDto::getSyncType));

        List<TaskDto> migrateList =  syncTypeToTaskList.getOrDefault(SyncType.MIGRATE.getValue(), Collections.emptyList());
        resultChart.put("chart1", getDataCopyChart(migrateList));
//        resultChart.put("chart2", dataCopy);
        List<TaskDto> synList = syncTypeToTaskList.getOrDefault(SyncType.SYNC.getValue(), Collections.emptyList());
        resultChart.put("chart3", getDataDevChart(synList));
//        resultChart.put("chart4", dataDev);
        resultChart.put("chart5", inspectChart(user));
        resultChart.put("chart6", chart6(user));
        return resultChart;
    }


    public Map<String, Integer> inspectChart(UserDetail user) {
        Criteria criteria = Criteria.where("syncType").is(TaskDto.SYNC_TYPE_MIGRATE)
                .and("isAutoInspect").is(true)
                .and("is_deleted").ne(true);
        Query query = new Query(criteria);
        query.fields().include("_id", "status", "isAutoInspect", "canOpenInspect", "attrs.autoInspectProgress.tableCounts", "attrs.autoInspectProgress.tableIgnore", "attrs.autoInspectProgress.step");

        int openTaskNum = 0;
        int canTaskNum = 0;
        int errorTaskNum = 0;
        int diffTaskNum = 0;
        List<TaskDto> taskDtos = findAllDto(query, user);

        Set<String> taskSet = taskAutoInspectResultsService.groupByTask(user);
        if (CollectionUtils.isNotEmpty(taskDtos)) {
            openTaskNum = taskDtos.size();
            for (TaskDto taskDto : taskDtos) {
                if (taskDto.isCanOpenInspect()) {
                    canTaskNum++;
                }

                if (TaskDto.STATUS_ERROR.equals(taskDto.getStatus())) {
                    errorTaskNum++;
                }

                if (taskSet.contains(taskDto.getId().toHexString())) {
                    diffTaskNum++;
                }

            }
        }

        Map<String, Integer> chart5 = new HashMap<>();
        chart5.put("total", openTaskNum);
        chart5.put("error", errorTaskNum);
        chart5.put("can", canTaskNum);
        chart5.put("diff", diffTaskNum);
        return chart5;
    }


    /**
     * 获取chart1 复制任务概览
     * 获取数据复制列表条件如下
     * {
     * "is_deleted": false,
     * "shareCache": {
     * "$ne": true
     * },
     * "syncType": "migrate",
     * "user_id": "62172cfc49b865ee5379d3ed"
     * }
     *
     * @return
     */
    private Map<String, Object> getDataCopyChart(List<TaskDto> migrateList) {
        Map<String, Object> dataCopyPreview = new HashMap();


        Map<String, List<TaskDto>> statusToDataCopyTaskMap = migrateList.stream().collect(Collectors.groupingBy(TaskDto::getStatus));
        //和数据复制列表保持一致   pause归为停止，  schduler_fail 归为 error  调度中schdulering  归为启动中
        List<TaskDto> pauseTaskList = statusToDataCopyTaskMap.remove(TaskStatusEnum.STATUS_PAUSED.getValue());
        List<TaskDto> schdulingTaskList = statusToDataCopyTaskMap.remove(TaskStatusEnum.STATUS_SCHEDULING.getValue());

        if (CollectionUtils.isNotEmpty(pauseTaskList)) {
            statusToDataCopyTaskMap.getOrDefault(TaskStatusEnum.STATUS_STOP.getValue(), new ArrayList<TaskDto>()).addAll(pauseTaskList);
        }

        if (CollectionUtils.isNotEmpty(schdulingTaskList)) {
            statusToDataCopyTaskMap.getOrDefault(TaskStatusEnum.STATUS_WAIT_RUN.getValue(), new ArrayList<TaskDto>()).addAll(schdulingTaskList);
        }

        List<TaskDto> schduleFailTaskList = statusToDataCopyTaskMap.remove(TaskStatusEnum.STATUS_SCHEDULE_FAILED.getValue());
        if (CollectionUtils.isNotEmpty(schduleFailTaskList)) {
            statusToDataCopyTaskMap.getOrDefault(TaskStatusEnum.STATUS_ERROR.getValue(), new ArrayList<TaskDto>()).addAll(schduleFailTaskList);
        }


        //数据复制概览
        List<Map> dataCopyPreviewItems = new ArrayList();
        List<String> allStatus = TaskStatusEnum.getAllStatus();
        for (String taskStatus : allStatus) {

            Map<String, Object> singleMap = new HashMap();
            singleMap.put("_id", taskStatus);
            singleMap.put("count", statusToDataCopyTaskMap.getOrDefault(taskStatus, Collections.emptyList()).size());
            dataCopyPreviewItems.add(singleMap);
        }
        dataCopyPreview.put("total", migrateList.size());
        dataCopyPreview.put("items", dataCopyPreviewItems);
        return dataCopyPreview;
    }

    /**
     * 统计的是Task中的statuses
     *
     * @return
     */
    private Map<String, Object> getDataDevChart(List<TaskDto> synList) {
        Map<String, Object> dataCopyPreview = new HashMap();

        Map<String, Long> statusToCount = new HashMap<>();
        if (CollectionUtils.isNotEmpty(synList)) {
            for (TaskDto taskDto : synList) {
                MapUtils.increase(statusToCount, taskDto.getStatus());
            }
        }

        //数据复制概览
        List<Map> dataCopyPreviewItems = new ArrayList();
        List<String> allStatus = TaskEnum.getAllStatus();
        for (String taskStatus : allStatus) {
            Map<String, Object> singleMap = new HashMap();
            singleMap.put("_id", taskStatus);
            singleMap.put("count", statusToCount.getOrDefault(taskStatus, 0L));
            dataCopyPreviewItems.add(singleMap);
        }
        dataCopyPreview.put("total", synList.size());
        dataCopyPreview.put("items", dataCopyPreviewItems);
        return dataCopyPreview;
    }

    public List<TaskEntity> findByIds(List<ObjectId> idList) {
        List<TaskEntity> taskEntityList = new ArrayList<>();
        Query query = Query.query(Criteria.where("id").in(idList));
        query.fields().exclude("dag");
        taskEntityList = repository.getMongoOperations().find(query, TaskEntity.class);
        return taskEntityList;
    }

    /**
     * 获取数据复制任务详情
     * 增量所处时间点，参考 IncreaseSyncVO 的 cdcTime   其实就是 AgentStatistics 表的cdcTime
     * 全量开始时间，参考 Task 的milestone  code 是 READ_SNAPSHOT 的start
     * 增量开始时间，参考 Task 的milestone  code 是 READ_CDC_EVENT 的start
     * 任务完成时间  取 ParentTaskDto 的 finishTime
     * 增量最大滞后时间:  AgentStatistics  replicateLag
     * 任务开始时间： Task startTime
     * 总时长 参考   FullSyncVO 的结束结束 - 开始时间
     * 失败总次数:  暂时获取不到
     *
     * @param id
     * @param field
     * @param userDetail
     * @return
     */
    public TaskDetailVo findTaskDetailById(String id, Field field, UserDetail userDetail) {
        TaskDto taskDto = super.findById(MongoUtils.toObjectId(id), field, userDetail);
        TaskDetailVo taskDetailVo = BeanUtil.copyProperties(taskDto, TaskDetailVo.class);

        if (StringUtils.isNotEmpty(userDetail.getUsername())) {
            taskDetailVo.setCreateUser(userDetail.getUsername());
        } else {
            taskDetailVo.setCreateUser(userDetail.getEmail());
        }

        if (taskDto != null) {
            String taskId = taskDto.getId().toString();

            String type = taskDto.getType();
            if ("initial_sync".equals(type)) {
                //设置全量开始时间
                Date initStartTime = getMillstoneTime(taskDto, "READ_SNAPSHOT", "initial_sync");
                taskDetailVo.setInitStartTime(initStartTime);
            } else if ("cdc".equals(type)) {
                //增量开始时间
                Date cdcStartTime = getMillstoneTime(taskDto, "READ_CDC_EVENT", "cdc");
                taskDetailVo.setCdcStartTime(cdcStartTime);

//                // 增量所处时间点
//                Date eventTime = getEventTime(taskId);
//                taskDetailVo.setEventTime(eventTime);
//
//                //增量最大滞后时间
//                taskDetailVo.setCdcDelayTime(getCdcDelayTime(taskId));

            } else if ("initial_sync+cdc".equals(type)) {
                //全量开始时间
                Date initStartTime = getMillstoneTime(taskDto, "READ_SNAPSHOT", "initial_sync");
                taskDetailVo.setInitStartTime(initStartTime);

                //增量开始时间
                Date cdcStartTime = getMillstoneTime(taskDto, "READ_CDC_EVENT", "cdc");
                taskDetailVo.setCdcStartTime(cdcStartTime);

//                // 增量所处时间点
//                Date eventTime = getEventTime(taskId);
//                taskDetailVo.setEventTime(eventTime);
//
//                //增量最大滞后时间
//                taskDetailVo.setCdcDelayTime(getCdcDelayTime(taskId));
            }

            // 总时长  开始时间和结束时间都有才行
            taskDetailVo.setTaskLastHour(getLastHour(taskId));
            taskDetailVo.setStartTime(taskDto.getStartTime());
            //任务完成时间
            taskDetailVo.setTaskFinishTime(taskDto.getFinishTime());
            taskDetailVo.setType(taskDto.getType());
        }
        return taskDetailVo;
    }

    /**
     * 获取任务总时长
     *
     * @param taskId
     * @return
     */
    private Long getLastHour(String taskId) {
        Long taskLastHour = null;
        try {
            FullSyncVO fullSyncVO = snapshotEdgeProgressService.syncOverview(taskId);
            if (null != fullSyncVO) {
                if (null != fullSyncVO.getStartTs() && null != fullSyncVO.getEndTs()) {
                    taskLastHour = DateUtil.between(fullSyncVO.getStartTs(), fullSyncVO.getEndTs(), DateUnit.MS);
                }
            }
        } catch (Exception e) {
            log.error("获取 fullSyncVO 出错， taskId：{}", taskId);
        }
        return taskLastHour;
    }


    /**
     * 获取增量开始时间
     *
     * @param TaskDto
     * @return
     */
    private Date getMillstoneTime(TaskDto TaskDto, String code, String group) {
        Date millstoneTime = null;
        Optional<Milestone> optionalMilestone = Optional.empty();
        List<Milestone> milestones = TaskDto.getMilestones();
        if (null != milestones) {
            optionalMilestone = milestones.stream().filter(s -> (code.equals(s.getCode()) && (group).equals(s.getGroup()))).findFirst();
            if (optionalMilestone.isPresent() && null != optionalMilestone.get().getStart() && optionalMilestone.get().getStart() > 0) {
                millstoneTime = new Date(optionalMilestone.get().getStart());
            }

        }
        return millstoneTime;
    }

//    /**
//     * 增量延迟时间
//     *
//     * @return
//     */
//    private Long getCdcDelayTime(String taskId) {
//        Long cdcDelayTime = null;
//        MeasurementEntity measurementEntity = measurementService.findByTaskId(taskId);
//        if (null != measurementEntity && null != measurementEntity.getStatistics() && null != measurementEntity.getStatistics().get("replicateLag")) {
//            Number cdcDelayTimeNumber = (Number) measurementEntity.getStatistics().get("replicateLag");
//            cdcDelayTime = cdcDelayTimeNumber.longValue();
//        }
//        return cdcDelayTime;
//    }


//    /**
//     * 获取增量所处时间点
//     *
//     * @return
//     */
//    private Date getEventTime(String taskId) {
//        Date eventTime = null;
//        MeasurementEntity measurementEntity = measurementService.findByTaskId(taskId);
//        if (null != measurementEntity) {
//            Number cdcTimestamp = measurementEntity.getStatistics().getOrDefault("cdcTime", 0L);
//            Long cdcMillSeconds = cdcTimestamp.longValue();
//            if (cdcMillSeconds > 0) {
//                eventTime = new Date(cdcMillSeconds);
//            }
//        }
//        return eventTime;
//    }


    public Boolean checkRun(String taskId, UserDetail user) {
        TaskDto taskDto = checkExistById(MongoUtils.toObjectId(taskId), user, "status");
        return TaskDto.STATUS_EDIT.equals(taskDto.getStatus()) || TaskDto.STATUS_WAIT_START.equals(taskDto.getStatus());
    }

    public TransformerWsMessageDto findTransformParam(String taskId, UserDetail user) {
        TaskDto taskDto = checkExistById(MongoUtils.toObjectId(taskId), user);
        return transformSchemaService.getTransformParam(taskDto, user);
    }

    public TransformerWsMessageDto findTransformAllParam(String taskId, UserDetail user) {
        TaskDto taskDto = checkExistById(MongoUtils.toObjectId(taskId), user);
        return transformSchemaService.getTransformParam(taskDto, user, true);
    }

    public TaskDto findByTaskId(ObjectId id, String... fields) {
        Query query = new Query(Criteria.where("_id").is(id));
        query.fields().include(fields);
        return findOne(query);
    }

    public void rename(String taskId, String newName, UserDetail user) {
        ObjectId objectId = MongoUtils.toObjectId(taskId);
        TaskDto taskDto = checkExistById(MongoUtils.toObjectId(taskId), user, "name");
        if (newName.equals(taskDto.getName())) {
            return;
        }

        checkTaskName(newName, user, objectId);

        Update update = Update.update("name", newName);

        updateById(objectId, update, user);

    }

    public TaskStatsDto stats(UserDetail userDetail) {

        Map<String, Long> taskTypeStats = typeTaskStats(userDetail);

        TaskStatsDto taskStatsDto = new TaskStatsDto();
        taskStatsDto.setTaskTypeStats(taskTypeStats);
        return taskStatsDto;
    }

    private Map<String, Long> typeTaskStats(UserDetail userDetail) {
        org.springframework.data.mongodb.core.aggregation.Aggregation aggregation =
                org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation(
                        match(Criteria.where("user_id").is(userDetail.getUserId())
                                .and("customId").is(userDetail.getCustomerId())
                                .and("is_deleted").ne(true)
                                .and("status").nin(TaskDto.STATUS_DELETING, TaskDto.STATUS_DELETE_FAILED)),
                        group("type").count().as("count")
                );

        Map<String, Long> taskTypeStats = new HashMap<>();

        AggregationResults<Char1Group> result = repository.aggregate(aggregation, Char1Group.class);
        for (Char1Group part : result) {
            taskTypeStats.put(part.get_id(), part.getCount());
        }
        if (!taskTypeStats.containsKey(ParentTaskDto.TYPE_CDC)) {
            taskTypeStats.put(ParentTaskDto.TYPE_CDC, 0L);
        }
        if (!taskTypeStats.containsKey(ParentTaskDto.TYPE_INITIAL_SYNC)) {
            taskTypeStats.put(ParentTaskDto.TYPE_INITIAL_SYNC, 0L);
        }
        if (!taskTypeStats.containsKey(ParentTaskDto.TYPE_INITIAL_SYNC_CDC)) {
            taskTypeStats.put(ParentTaskDto.TYPE_INITIAL_SYNC_CDC, 0L);
        }
        Long total = taskTypeStats.values().stream().reduce(Long::sum).orElse(0L);
        taskTypeStats.put("total", total);
        return taskTypeStats;
    }

    public DataFlowInsightStatisticsDto statsTransport(UserDetail userDetail) {

        Criteria criteria = Criteria.where("is_deleted").ne(true);
        Query query = new Query(criteria);
        query.fields().include("_id");
        List<TaskDto> allDto = findAllDto(query, userDetail);
        List<String> ids = allDto.stream().map(a->a.getId().toHexString()).collect(Collectors.toList());

        List<LocalDate> localDates = new ArrayList<>();
        LocalDate now = LocalDate.now();
        LocalDate lastMonthDay = now.minusMonths(1);
        Map<LocalDate, Long> allInputNumMap = new HashMap<>();
        while (!now.equals(lastMonthDay)) {
            localDates.add(now);
            allInputNumMap.put(now, 0L);
            now = now.minusDays(1);
        }
        List<Date> localDateTimes = new ArrayList<>();
        for (LocalDate localDate : localDates) {
            for (int i = 0; i < 24; i++) {
                LocalDateTime localDateTime = LocalDateTime.of(localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth(), i, 0, 0);
                Date date = Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
                localDateTimes.add(date);
            }
        }


        Criteria in = Criteria.where("tags.taskId").in(ids)
                .and("date").in(localDateTimes)
                .and("grnty").is("hour")
                .and("tags.type").is("task");
        Query query1 = new Query(in);
        query1.fields().include("ss", "tags");
        DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyyMMdd");
        List<MeasurementEntity> measurementEntities = repository.getMongoOperations().find(query1, MeasurementEntity.class, "AgentMeasurementV2");

        Map<String, List<MeasurementEntity>> taskMap = measurementEntities.stream().collect(Collectors.groupingBy(m -> m.getTags().get("taskId")));




        taskMap.forEach((k1, v1) -> {
            Map<LocalDate, Long> inputNumMap = new HashMap<>();
            Map<LocalDate, List<Sample>> sampleMap = v1.stream().flatMap(m -> m.getSamples().stream()).collect(Collectors.groupingBy(s -> {
                Date date = s.getDate();
                return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
            }));
            List<LocalDate> collect = sampleMap.keySet().stream().sorted().collect(Collectors.toList());
            for (LocalDate k : collect) {
                List<Sample> v = sampleMap.get(k);
                long value = 0;
                Optional<Sample> max = v.stream().max(Comparator.comparing(Sample::getDate));
                if (max.isPresent()) {
                    Sample sample = max.get();
                    Map<String, Number> vs = sample.getVs();
                    value += Long.parseLong(String.valueOf(vs.get("inputInsertTotal")));
                    value += Long.parseLong(String.valueOf(vs.get("inputOthersTotal")));
                    value += Long.parseLong(String.valueOf(vs.get("inputDdlTotal")));
                    value += Long.parseLong(String.valueOf(vs.get("inputUpdateTotal")));
                    value += Long.parseLong(String.valueOf(vs.get("inputDeleteTotal")));
                }
                LocalDate localDate = k.minusDays(1L);
                Long lastNum = inputNumMap.get(localDate);
                if (lastNum != null) {
                    value = value - lastNum;
                }
                inputNumMap.put(k, value);
            }

            inputNumMap.forEach((k2, v2) -> {
                Long allNum = allInputNumMap.get(k2);
                if (allNum != null) {
                    v2 = v2 + allNum;
                }

                allInputNumMap.put(k2, v2);

            });

        });

        List<DataFlowInsightStatisticsDto.DataStatisticInfo> inputDataStatistics = new ArrayList<>();
        AtomicLong totalInputDataCount = new AtomicLong();
        allInputNumMap.forEach((k, v) -> {
            inputDataStatistics.add(new DataFlowInsightStatisticsDto.DataStatisticInfo(k.format(format), v));
            totalInputDataCount.addAndGet(v);
        });

        DataFlowInsightStatisticsDto dataFlowInsightStatisticsDto = new DataFlowInsightStatisticsDto();
        inputDataStatistics.sort(Comparator.comparing(DataFlowInsightStatisticsDto.DataStatisticInfo::getTime));
        dataFlowInsightStatisticsDto.setInputDataStatistics(inputDataStatistics);
        dataFlowInsightStatisticsDto.setTotalInputDataCount(totalInputDataCount.get());
        dataFlowInsightStatisticsDto.setGranularity("month");
        return dataFlowInsightStatisticsDto;
    }
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Char1Group {
        private String _id;
        private long count;
    }

    @Data
    public static class Char2Group {
        private long totalInput = 0;
        private long totalOutput = 0;
        private long totalInputDataSize = 0;
        private long totalOutputDataSize = 0;
        private long totalInsert = 0;
        private long totalInsertSize = 0;
        private long totalUpdate = 0;
        private long totalUpdateSize = 0;
        private long totalDelete = 0;
        private long totalDeleteSize = 0;
    }


    public void batchLoadTask(HttpServletResponse response, List<String> taskIds, UserDetail user) {
        List<TaskUpAndLoadDto> jsonList = new ArrayList<>();

        List<TaskDto> tasks = findAllTasksByIds(taskIds);
        Map<String, TaskDto> taskDtoMap = tasks.stream().collect(Collectors.toMap(t -> t.getId().toHexString(), Function.identity(), (e1, e2) -> e1));
        for (String taskId : taskIds) {
            TaskDto taskDto = taskDtoMap.get(taskId);
            if (taskDto != null) {
                taskDto.setCreateUser(null);
                taskDto.setCustomId(null);
                taskDto.setLastUpdBy(null);
                taskDto.setUserId(null);
                taskDto.setAgentId(null);
                taskDto.setListtags(null);
                taskDto.setAccessNodeProcessId(null);
                taskDto.setAccessNodeProcessIdList(new ArrayList<>());
                taskDto.setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());

                taskDto.setStatus(TaskDto.STATUS_EDIT);
                taskDto.setStatuses(new ArrayList<>());
                Map<String, Object> attrs = taskDto.getAttrs();
                if (attrs != null) {
                    attrs.remove("edgeMilestones");
                    attrs.remove("syncProgress");
                }
                jsonList.add(new TaskUpAndLoadDto("Task", JsonUtil.toJsonUseJackson(taskDto)));
                DAG dag = taskDto.getDag();
                List<Node> nodes = dag.getNodes();
                if (CollectionUtils.isNotEmpty(nodes)) {
                    try {
                        for (Node node : nodes) {
                            List<MetadataInstancesDto> metadataInstancesDtos = metadataInstancesService.findByNodeId(node.getId(), null, user, taskDto);
                            if (CollectionUtils.isNotEmpty(metadataInstancesDtos)) {
                                for (MetadataInstancesDto metadataInstancesDto : metadataInstancesDtos) {
                                    metadataInstancesDto.setCreateUser(null);
                                    metadataInstancesDto.setCustomId(null);
                                    metadataInstancesDto.setLastUpdBy(null);
                                    metadataInstancesDto.setUserId(null);
                                    jsonList.add(new TaskUpAndLoadDto("MetadataInstances", JsonUtil.toJsonUseJackson(metadataInstancesDto)));
                                }
                            }

                            if (node instanceof DataParentNode) {
                                String connectionId = ((DataParentNode<?>) node).getConnectionId();
                                DataSourceConnectionDto dataSourceConnectionDto = dataSourceService.findById(MongoUtils.toObjectId(connectionId), user);
                                dataSourceConnectionDto.setCreateUser(null);
                                dataSourceConnectionDto.setCustomId(null);
                                dataSourceConnectionDto.setLastUpdBy(null);
                                dataSourceConnectionDto.setUserId(null);
                                String databaseQualifiedName = MetaDataBuilderUtils.generateQualifiedName("database", dataSourceConnectionDto, null);
                                MetadataInstancesDto dataSourceMetadataInstance = metadataInstancesService.findOne(
                                        Query.query(Criteria.where("qualified_name").is(databaseQualifiedName).and("is_deleted").ne(true)), user);
                                jsonList.add(new TaskUpAndLoadDto("MetadataInstances", JsonUtil.toJsonUseJackson(dataSourceMetadataInstance)));
                                jsonList.add(new TaskUpAndLoadDto("Connections", JsonUtil.toJsonUseJackson(dataSourceConnectionDto)));
                            }
                        }
                    } catch (Exception e) {
                        log.error("node data error", e);
                    }
                }
            }
        }
        String json = JsonUtil.toJsonUseJackson(jsonList);

        AtomicReference<String> fileName = new AtomicReference<>("");
        String yyyymmdd = DateUtil.today().replaceAll("-", "");
        FunctionUtils.isTureOrFalse(taskIds.size() > 1).trueOrFalseHandle(
                () -> fileName.set("task_batch" + "-" + yyyymmdd),
                () -> fileName.set(taskDtoMap.get(taskIds.get(0)).getName() + "-" + yyyymmdd)
        );
        fileService1.viewImg1(json, response, fileName.get() + ".json.gz");
    }


    public void batchUpTask(MultipartFile multipartFile, UserDetail user, boolean cover, List<String> tags) {
        byte[] bytes;
        List<TaskUpAndLoadDto> taskUpAndLoadDtos;

        if (!Objects.requireNonNull(multipartFile.getOriginalFilename()).endsWith("json.gz")) {
            //不支持其他的格式文件
            throw new BizException("Task.ImportFormatError");
        }

        try {
            bytes = GZIPUtil.unGzip(multipartFile.getBytes());

            String json = new String(bytes, StandardCharsets.UTF_8);

            taskUpAndLoadDtos = JsonUtil.parseJsonUseJackson(json, new TypeReference<List<TaskUpAndLoadDto>>() {
            });
        } catch (Exception e) {
            //e.printStackTrace();
            //不支持其他的格式文件
            throw new BizException("Task.ImportFormatError");
        }

        if (taskUpAndLoadDtos == null) {
            //不支持其他的格式文件
            throw new BizException("Task.ImportFormatError");
        }

        List<MetadataInstancesDto> metadataInstancess = new ArrayList<>();
        List<TaskDto> tasks = new ArrayList<>();
        List<DataSourceConnectionDto> connections = new ArrayList<>();
        for (TaskUpAndLoadDto taskUpAndLoadDto : taskUpAndLoadDtos) {
            try {
                String dtoJson = taskUpAndLoadDto.getJson();
                if (StringUtils.isBlank(taskUpAndLoadDto.getJson())) {
                    continue;
                }
                if ("MetadataInstances".equals(taskUpAndLoadDto.getCollectionName())) {
                    metadataInstancess.add(JsonUtil.parseJsonUseJackson(dtoJson, MetadataInstancesDto.class));
                } else if ("Task".equals(taskUpAndLoadDto.getCollectionName())) {
                    tasks.add(JsonUtil.parseJsonUseJackson(dtoJson, TaskDto.class));
                } else if ("Connections".equals(taskUpAndLoadDto.getCollectionName())) {
                    connections.add(JsonUtil.parseJsonUseJackson(dtoJson, DataSourceConnectionDto.class));
                }
            } catch (Exception e) {
                log.error("error", e);
            }
        }

        try {
            metadataInstancesService.batchImport(metadataInstancess, user, cover);
        } catch (Exception e) {
            log.error("metadataInstancesService.batchImport error", e);
        }
        try {
            dataSourceService.batchImport(connections, user, cover);
        } catch (Exception e) {
            log.error("dataSourceService.batchImport error", e);
        }
        try {
            batchImport(tasks, user, cover, tags);
        } catch (Exception e) {
            log.error("tasks.batchImport error", e);
        }
    }

    public void batchImport(List<TaskDto> taskDtos, UserDetail user, boolean cover, List<String> tags) {

        List<Tag> tagList = new ArrayList<>();

        if (CollectionUtils.isNotEmpty(tags)) {
            Criteria criteriaTags = Criteria.where("_id").in(tags);
            Query query = new Query(criteriaTags);
            query.fields().include("_id", "value");
            List<MetadataDefinitionDto> allDto = metadataDefinitionService.findAllDto(query, user);
            if (CollectionUtils.isNotEmpty(allDto)) {
                tagList = allDto.stream().map(m -> new Tag(m.getId().toHexString(), m.getValue())).collect(Collectors.toList());
            }
        }

        for (TaskDto taskDto : taskDtos) {
            Query query = new Query(Criteria.where("_id").is(taskDto.getId()).and("is_deleted").ne(true));
            query.fields().include("id");
            TaskDto one = findOne(query);

            taskDto.setListtags(null);
            taskDto.setStatus(TaskDto.STATUS_EDIT);
            taskDto.setAccessNodeProcessId(null);
            taskDto.setAccessNodeProcessIdList(new ArrayList<>());
            taskDto.setAccessNodeType(AccessNodeTypeEnum.AUTOMATIC_PLATFORM_ALLOCATION.name());

            Map<String, Object> attrs = taskDto.getAttrs();
            if (attrs != null) {
                attrs.remove("edgeMilestones");
                attrs.remove("syncProgress");
            }

            if (one == null || cover) {
                ObjectId objectId = null;
                if (one != null) {
                    objectId = one.getId();
                }

                while (checkTaskNameNotError(taskDto.getName(), user, objectId)) {
                    taskDto.setName(taskDto.getName() + "_import");
                }

                if (CollectionUtils.isNotEmpty(tagList)) {
                    taskDto.setListtags(tagList);
                }
                if (one == null) {
                    //taskDto.setId(null);
                    TaskEntity taskEntity = repository.importEntity(convertToEntity(TaskEntity.class, taskDto), user);
                    taskDto = convertToDto(taskEntity, TaskDto.class);
                }

                DAG dag = taskDto.getDag();
                if (dag != null) {
                    Map<String, List<Message>> validate = dag.validate();
                    if (validate != null && validate.size() != 0) {
                        updateById(taskDto, user);
                        continue;
                    }
                }

                confirmById(taskDto, user, true, true);
            }
        }
    }

    /**
     * 处理filter里面的or 请求，传话成Criteria
     *
     * @param where
     * @return
     */
    public Criteria parseOrToCriteria(Where where) {
        //处理关键字搜索
        Criteria nameCriteria = new Criteria();
        if (null != where.get("or")) {
            List<Criteria> criteriaList = new ArrayList<>();
            List<Map<String, Map<String, String>>> orList = (List) where.remove("or");
            for (Map<String, Map<String, String>> orMap : orList) {
                orMap.forEach((key, value) -> {
                    if (value.containsKey("$regex")) {
                        String queryStr = value.get("$regex");
                        Criteria orCriteria = Criteria.where(key).regex(queryStr);
                        criteriaList.add(orCriteria);
                    }
                });
            }
            nameCriteria = new Criteria().orOperator(criteriaList);
        }
        return nameCriteria;
    }

    /**
     * 处理 where 第一层过滤条件
     *
     * @param where
     */
    private void parseWhereCondition(Where where, Query query) {
        where.forEach((prop, value) -> {
            if (!query.getQueryObject().containsKey(prop)) {
                query.addCriteria(Criteria.where(prop).is(value));
            }
        });
    }

    private void parseFieldCondition(Filter filter, Query query) {
        Field fields = filter.getFields();
        if (null != fields) {
            fields.forEach((filedName, get) -> {
                if ((Boolean) get) {
                    query.fields().include(filedName);
                }
            });
        }
    }

    //todo  待优化
    private void parseOrderBy(Object orderBy, Query query, Class clazz) {
        String orderByStr = "";
        if (null == orderBy) {
            orderByStr = "createTime DESC";
        } else {
            orderByStr = (String) orderBy;
        }
        java.lang.reflect.Field[] fields = clazz.getFields();
        for (java.lang.reflect.Field field : fields) {
            String fieldName = field.getName();
            if (orderByStr.contains(fieldName)) {
                if (orderByStr.contains("ASC")) {
                    query.with(Sort.by(fieldName).ascending());
                } else {
                    query.with(Sort.by(fieldName).descending());
                }
            }
        }
    }

    /**
     * 1 检查是否有模型推演 有 直接从meta instance拿数据（目标数据源类型 复制表名称以及表字段，需要考虑表改名、字段改名）
     * 2 没有推演模型从taskDto获取数据（目标数据源类型 复制表名称以及表字段，需要考虑表改名、字段改名）
     * 3 发websocket给flow engine
     * @param taskDto migrate task
     */
    public void getTableDDL (TaskDto taskDto) {
        DAG dag = taskDto.getDag();
        DatabaseNode target = (DatabaseNode) dag.getTargets().get(0);

        Query transformerQuery = new Query();
        transformerQuery.addCriteria(Criteria.where("dataFlowId").is(taskDto.getId()).and("sinkNodeId").is(target.getId()));
        List<MetadataTransformerItemDto> transformerItemList = metadataTransformerItemService.findAll(transformerQuery);

        List<MigrateTableDto> migrateTableList = Lists.newArrayList();
        List<MetadataInstancesDto> metadataInstancesList;
        if (CollectionUtils.isEmpty(transformerItemList)) {
            DatabaseNode source = (DatabaseNode) dag.getSources().get(0);
            String connectionId = source.getConnectionId();

            Query instanceQuery = new Query();
            instanceQuery.addCriteria(Criteria.where("source._id").is(connectionId).and("meta_type").is("table").and("is_deleted").ne("true"));
            metadataInstancesList = metadataInstancesService.findAll(instanceQuery);

        } else {
            List<String> qualifiedNameList = transformerItemList.stream().map(MetadataTransformerItemDto::getSinkQulifiedName).distinct().collect(Collectors.toList());
            Query instantiatedQuery = new Query();
            instantiatedQuery.addCriteria(Criteria.where("qualified_name").in(qualifiedNameList));

            metadataInstancesList = metadataInstancesService.findAll(instantiatedQuery);
        }

        if (CollectionUtils.isNotEmpty(metadataInstancesList)) {
            metadataInstancesList.forEach(t -> {
                String databaseType = t.getSource().getDatabase_type();
                String originalName = t.getOriginalName();
                List<com.tapdata.tm.commons.schema.Field> fieldList = t.getFields();

                List<com.tapdata.tm.commons.schema.Field> list = new ArrayList<>();
                BeanUtil.copyProperties(fieldList, list);
                migrateTableList.add(new MigrateTableDto(databaseType, originalName, list));
            });
        }
    }

    public List<TaskDto> findAllTasksByIds(List<String> list) {
        List<ObjectId> ids = list.stream().map(ObjectId::new).collect(Collectors.toList());

        Query query = new Query(Criteria.where("_id").in(ids));
        List<TaskEntity> entityList = findAllEntity(query);
        return CglibUtil.copyList(entityList, TaskDto::new);
//        return findAll(query);
    }

//    public void updateStatus(ObjectId taskId, String status) {
//        Query query = Query.query(Criteria.where("_id").is(taskId));
//        Update update = Update.update("status", status).set("last_updated", new Date());
//        update(query, update);
//    }





    /**
     * 重置子任务之后，情况指标观察数据
     * @param taskId
     */
    public void renewAgentMeasurement(String taskId) {
        //所有的任务重置操作，都会进这里
        //根据TaskId 把指标数据都删掉
//        measurementService.deleteTaskMeasurement(taskId);
        measurementServiceV2.deleteTaskMeasurement(taskId);
    }
    public void renewNotSendMq(TaskDto taskDto, UserDetail user) {
        log.info("renew task, task name = {}, username = {}", taskDto.getName(), user.getUsername());



        Update set = Update.update("agentId", null).set("agentTags", null).set("scheduleTimes", null)
                .set("scheduleTime", null)
                .unset("milestones").unset("tmCurrentTime").set("messages", null);


        if (taskDto.getAttrs() != null) {
            taskDto.getAttrs().remove("syncProgress");
            taskDto.getAttrs().remove("edgeMilestones");
            AutoInspectUtil.removeProgress(taskDto.getAttrs());

            set.set("attrs", taskDto.getAttrs());
        }

        //updateById(TaskDto.getId(), set, user);

        //清空当前子任务的所有的node运行信息TaskRuntimeInfo
        List<Node> nodes = taskDto.getDag().getNodes();
        if (nodes != null) {

            List<String> nodeIds = nodes.stream().map(Node::getId).collect(Collectors.toList());
            Criteria criteria = Criteria.where("taskId").is(taskDto.getId().toHexString())
                    .and("type").is(TaskSnapshotProgress.ProgressType.EDGE_PROGRESS.name())
                    .orOperator(Criteria.where("srcNodeId").in(nodeIds),
                            Criteria.where("tgtNodeId").in(nodeIds));
            Query query = new Query(criteria);

            snapshotEdgeProgressService.deleteAll(query);

            Criteria criteria1 = Criteria.where("taskId").is(taskDto.getId().toHexString())
                    .and("type").is(TaskSnapshotProgress.ProgressType.TASK_PROGRESS.name());
            Query query1 = new Query(criteria1);

            snapshotEdgeProgressService.deleteAll(query1);
//            taskNodeRuntimeInfoService.deleteAll(query);
//            taskDatabaseRuntimeInfoService.deleteAll(query);
        }

        //todo jiaxin 之后逻辑补偿完善后再开启
        //重置的时候需要将子任务的temp更新到子任务实体中
//        if (taskDto.getTempDag() != null) {
//            taskDto.setDag(taskDto.getTempDag());
//        }
        beforeSave(taskDto, user);
        set.unset("tempDag").set("isEdit", true);
//        Update update = new Update();
//        taskService.update(new Query(Criteria.where("_id").is(taskDto.getParentId())), update.unset("temp"));
        updateById(taskDto.getId(), set, user);

        //resetFlag(taskDto.getId(), user, "resetFlag");
    }

    private String sendRenewMq(TaskDto taskDto, UserDetail user, String opType) {
        String connectionName =null;
        boolean noAgent =false;
        if (checkPdkTask(taskDto, user)) {

            DataSyncMq mq = new DataSyncMq();
            mq.setTaskId(taskDto.getId().toHexString());
            mq.setOpType(opType);
            mq.setType(MessageType.DATA_SYNC.getType());


            Map<String, Object> data;
            String json = JsonUtil.toJsonUseJackson(mq);
            data = JsonUtil.parseJsonUseJackson(json, Map.class);

            if (StringUtils.equals(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), taskDto.getAccessNodeType())
                    && CollectionUtils.isNotEmpty(taskDto.getAccessNodeProcessIdList())) {

                List<Worker> availableAgent = workerService.findAvailableAgent(user);
                List<String> processIds = availableAgent.stream().map(Worker::getProcessId).collect(Collectors.toList());
                String agentId = null;
                for (String p : taskDto.getAccessNodeProcessIdList()) {
                    if (processIds.contains(p)) {
                        agentId = p;
                        break;
                    }
                }
                if (StringUtils.isBlank(agentId)) {
                    noAgent =true;
                    //任务指定的agent已经停用，当前操作不给用。
                   // throw new BizException("Agent.DesignatedAgentNotAvailable");
                    connectionName =judgePostgreClearSlot(taskDto,opType);

                }

                taskDto.setAgentId(agentId);

            } else {
                List<Worker> availableAgent = workerService.findAvailableAgent(user);
                if (CollectionUtils.isNotEmpty(availableAgent)) {
                    Worker worker = availableAgent.get(0);
                    taskDto.setAgentId(worker.getProcessId());
                } else {
                    noAgent =true;
                    connectionName =judgePostgreClearSlot(taskDto,opType);
                    taskDto.setAgentId(null);
                }
            }

            MessageQueueDto queueDto = new MessageQueueDto();
            queueDto.setReceiver(taskDto.getAgentId());
            queueDto.setData(data);
            queueDto.setType("pipe");

            log.debug("build stop task websocket context, processId = {}, userId = {}, queueDto = {}", taskDto.getAgentId(), user.getUserId(), queueDto);
            messageQueueService.sendMessage(queueDto);

            Update update = new Update()
                    .unset("startTime")
                    .unset("lastStartDate")
                    .unset("stopTime")
                    .unset("currentEventTimestamp")
                    .set("needCreateRecord", taskDto.isNeedCreateRecord());
            String nameSuffix = RandomStringUtils.randomAlphanumeric(6);

            if (DataSyncMq.OP_TYPE_DELETE.equals(opType)) {
                update.set("name", taskDto.getName() + "_" + nameSuffix);
            }
            this.update(new Query(Criteria.where("id").is(taskDto.getId())), update);

            //updateStatus(taskDto.getId(), DataSyncMq.OP_TYPE_RESET.equals(opType) ? TaskDto.STATUS_RENEWING : TaskDto.STATUS_DELETING);

//            //检查是否完成重置，设置8秒的超时时间
//            boolean checkFlag = false;
//            for (int i = 0; i < 60; i++) {
//                checkFlag = DataSyncMq.OP_TYPE_RESET.equals(opType) ? checkResetFlag(taskDto.getId(), user) : checkDeleteFlag(taskDto.getId(), user);
//                if (checkFlag) {
//                    break;
//                }
//                try {
//                    Thread.sleep(1000L);
//                } catch (InterruptedException e) {
//                    throw new BizException("SystemError");
//                }
//            }
//
//            if (!checkFlag) {
//                log.info((DataSyncMq.OP_TYPE_RESET.equals(opType) ? "reset" : "delete") + "Task reset timeout.");
//                throw new BizException(DataSyncMq.OP_TYPE_RESET.equals(opType) ? "Task.ResetTimeout" : "Task.DeleteTimeout");
//            }
            if (noAgent) {
                afterRemove(taskDto, user);
            }
        } else {
            if (DataSyncMq.OP_TYPE_RESET.equals(opType)) {
                afterRenew(taskDto, user);
            } else {
                afterRemove(taskDto, user);
            }
        }
        return connectionName;
    }


    private String judgePostgreClearSlot(TaskDto taskDto, String opType) {
        Node node = getSourceNode(taskDto);
        Map<String, Object> attrs = null;
        String databaseType = null;
        if (node instanceof DatabaseNode) {
            attrs = node.getAttrs();
            databaseType = ((DatabaseNode) node).getDatabaseType();
        } else if (node instanceof TableNode) {
            attrs = node.getAttrs();
            databaseType = ((TableNode) node).getDatabaseType();
        }

        if ("PostgreSQL".equalsIgnoreCase(databaseType) &&
                DataSyncMq.OP_TYPE_DELETE.equals(opType) && MapUtils.isNotEmpty(taskDto.getAttrs())) {
            return (String) attrs.get("connectionName");
        }
        return null;
    }

//    public boolean deleteById(TaskDto taskDto, UserDetail user) {
//        //如果子任务在运行中，将任务停止，再删除（在这之前，应该提示用户这个风险）
//        if (taskDto == null) {
//            return true;
//        }
//
//        sendRenewMq(taskDto, user, DataSyncMq.OP_TYPE_DELETE);
//
//        renewNotSendMq(taskDto, user);
//
//        if (runningStatus.contains(taskDto.getStatus())) {
//            log.warn("task is run, can not delete it");
//            throw new BizException("Task.DeleteTaskIsRun");
//        }
//
//        //TODO 删除当前模块的模型推演
//        resetFlag(taskDto.getId(), user, "deleteFlag");
//        return super.deleteById(taskDto.getId(), user);
//    }



    private boolean compareDag(DAG dag, DAG old) {
        List<Node> nodes = dag.getNodes();
        List<Node> oldNodes = old.getNodes();
        if (nodes.size() != oldNodes.size()) {
            return false;
        }

        Map<String, Node> oldMap = oldNodes.stream().collect(Collectors.toMap(Node::getId, n -> n));
        for (Node node : nodes) {
            Node oldNode = oldMap.get(node.getId());
            if (oldNode == null) {
                return false;
            }

            if (!node.equals(oldNode)) {
                return false;
            }
        }
        return true;
    }


    /**
     * 启动任务
     *
     * @param id
     */
    public void start(ObjectId id, UserDetail user) {
        String startFlag = "11";
        TaskDto taskDto = checkExistById(id, user);
        start(taskDto, user, startFlag);
    }

    /**
     * 状态机启动子任务之前执行
     *
     * @param taskDto
     * @param user 字符串开关，
     *                  第一位 是否需要共享挖掘处理， 1 是   0 否
     *                  第二位 是否开启打点任务      1 是   0 否
     */
    private void start(TaskDto taskDto, UserDetail user) {
        start(taskDto, user, "11");
    }
    private void start(TaskDto taskDto, UserDetail user, String startFlag) {

        checkDagAgentConflict(taskDto, false);
        if (!taskDto.getShareCache()) {
                Map<String, List<Message>> validateMessage = taskDto.getDag().validate();
                if (!validateMessage.isEmpty()) {
                    throw new BizException("Task.ListWarnMessage", validateMessage);
            }
        }
        //日志挖掘
        if (startFlag.charAt(0) == '1') {
            logCollectorService.logCollector(user, taskDto);
        }

        //打点任务，这个标识主要是防止任务跟子任务重复执行的
        if (startFlag.charAt(1) == '1') {
            //startConnHeartbeat(user, parentTask);
        }

        //模型推演,如果模型已经存在，则需要推演
//        DAG dag = taskDto.getDag();

        //校验当前状态是否允许启动。
        if (!TaskOpStatusEnum.to_start_status.v().contains(taskDto.getStatus())) {
            log.warn("task current status not allow to start, task = {}, status = {}", taskDto.getName(), taskDto.getStatus());
            if (TaskDto.STATUS_DELETING.equals(taskDto.getStatus()) || TaskDto.STATUS_DELETE_FAILED.equals(taskDto.getStatus())) {
                throw new BizException("Task.Deleted");
            }
            throw new BizException("Task.StartStatusInvalid");
        }

        if (TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType()) || TaskDto.SYNC_TYPE_SYNC.equals(taskDto.getSyncType())) {
            for (int i = 1; i < 6; i++) {
                TaskDto transformedCheck = findByTaskId(taskDto.getId(), "transformed");
                if (transformedCheck.getTransformed() != null && transformedCheck.getTransformed()) {
                    run(taskDto, user);
                    return;
                }
                try {
                    long sleepTime = 1;
                    for (int j = 0; j < i; j++) {
                        sleepTime = sleepTime * 2;
                    }
                    sleepTime = sleepTime * 1000;
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    throw new BizException("SystemError");
                }
            }
            throw new BizException("Task.StartCheckModelFailed");
        } else {
            run(taskDto, user);
        }
    }


    /**
     * @see com.tapdata.tm.statemachine.enums.DataFlowEvent#START
     * @param taskDto
     * @param user
     */
    public void run(TaskDto taskDto, UserDetail user) {
        StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.START, user);
        if (stateMachineResult.isFail()) {
            //如果更新失败，则表示可能为并发启动操作，本次不做处理
            log.info("concurrent start operations, this operation don‘t effective, task name = {}", taskDto.getName());
            return;
        }
        Query query = new Query(Criteria.where("id").is(taskDto.getId()).and("status").is(taskDto.getStatus()));
        //需要将重启标识清除
        update(query, Update.update("isEdit", false).set("restartFlag", false), user);
        updateTaskRecordStatus(taskDto, TaskDto.STATUS_SCHEDULING, user);
        taskScheduleService.scheduling(taskDto, user);
    }


    /**
     * @see DataFlowEvent#SCHEDULE_SUCCESS
     * @param dto
     * @param status
     * @param userDetail
     */
    public void updateTaskRecordStatus(TaskDto dto, String status, UserDetail userDetail) {
        dto.setStatus(status);
        if (StringUtils.isNotBlank(dto.getTaskRecordId())) {
            SyncTaskStatusDto info = SyncTaskStatusDto.builder()
                    .taskId(dto.getId().toHexString())
                    .taskName(dto.getName())
                    .taskRecordId(dto.getTaskRecordId())
                    .taskStatus(status)
                    .updateBy(userDetail.getUserId())
                    .updatorName(userDetail.getUsername())
                    .agentId(dto.getAgentId())
                    .syncType(dto.getSyncType())
                    .build();
            disruptorService.sendMessage(DisruptorTopicEnum.TASK_STATUS, info);
        }
    }


    /**
     * 暂停子任务
     *
     * @param id
     */
    public void pause(ObjectId id, UserDetail user, boolean force) {
        TaskDto TaskDto = checkExistById(id, user);
        pause(TaskDto, user, force);
    }

    /**
     * 暂停子任务  将子任务停止，不清空中间状态
     *
     * @param TaskDto 子任务
     * @param user       用户
     * @param force      是否强制停止
     */
    public void pause(TaskDto TaskDto, UserDetail user, boolean force) {
        pause(TaskDto, user, force, false);
    }

    /**
     * 暂停子任务  将子任务停止，不清空中间状态
     *
     * @param id   任务id
     * @param user       用户
     * @param force      是否强制停止
     */
    //@Transactional
    public void pause(ObjectId id, UserDetail user, boolean force, boolean restart) {
        TaskDto taskDto = checkExistById(id, user);
        pause(taskDto, user, force, restart);
    }


    /**
     * @see DataFlowEvent#STOP
     * @see DataFlowEvent#FORCE_STOP
     * @param taskDto
     * @param user
     * @param force
     * @param restart
     */
    public void pause(TaskDto taskDto, UserDetail user, boolean force, boolean restart) {

        //重启的特殊处理，共享挖掘的比较多
        if (TaskDto.STATUS_STOP.equals(taskDto.getStatus()) && restart) {
            Update update = Update.update("restartFlag", true).set("restartUserId", user.getUserId());
            Query query = new Query(Criteria.where("_id").is(taskDto.getId()));
            update(query, update, user);
            return;
        }

        String pauseStatus = TaskDto.STATUS_STOPPING;
        StateMachineResult stateMachineResult;
        if (force) {
            stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.FORCE_STOP, user);
        } else {
            stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.STOP, user);
        }

        if (stateMachineResult.isFail()) {
            //没有更新成功，说明可能是并发操作导致
            log.info("concurrent pause operations, this operation don‘t effective, task name = {}", taskDto.getName());
            return;
        }

        //将状态改为暂停中，给flowengin发送暂停消息，在回调的消息中将任务改为已暂停
        if (restart) {
            Update update = new Update();
            update.set("restartFlag", true).set("restartUserId", user.getUserId());
            Query query1 = new Query(Criteria.where("_id").is(taskDto.getId()));
            update(query1, update, user);
        }

        Update update = Update.update("stopRetryTimes", 0);
        updateById(taskDto.getId(), update, user);


        sendStoppingMsg(taskDto.getId().toHexString(), taskDto.getAgentId(), user, force);

        updateTaskRecordStatus(taskDto, pauseStatus, user);
    }

    public void sendStoppingMsg(String taskId, String agentId, UserDetail user, boolean force) {
        DataSyncMq dataSyncMq = new DataSyncMq();
        dataSyncMq.setTaskId(taskId);
        dataSyncMq.setForce(force);
        dataSyncMq.setOpType(DataSyncMq.OP_TYPE_STOP);
        dataSyncMq.setType(MessageType.DATA_SYNC.getType());

        Map<String, Object> data;
        String json = JsonUtil.toJsonUseJackson(dataSyncMq);
        data = JsonUtil.parseJsonUseJackson(json, Map.class);
        MessageQueueDto queueDto = new MessageQueueDto();
        queueDto.setReceiver(agentId);
        queueDto.setData(data);
        queueDto.setType("pipe");

        log.debug("build stop task websocket context, processId = {}, userId = {}, queueDto = {}", agentId, user.getUserId(), queueDto);
        messageQueueService.sendMessage(queueDto);
    }


    /**
     * 收到子任务已经运行的消息
     * @see DataFlowEvent#RUNNING
     * @param id
     */
    public String running(ObjectId id, UserDetail user) {

        //判断子任务是否存在
        TaskDto taskDto = checkExistById(id, user, "_id", "status", "name", "taskRecordId", "startTime");
        //将子任务状态改成运行中
        if (!TaskDto.STATUS_WAIT_RUN.equals(taskDto.getStatus())) {
            log.info("concurrent runError operations, this operation don‘t effective, task name = {}", taskDto.getName());
            return null;
        }
        Query query1 = new Query(Criteria.where("_id").is(taskDto.getId()));

        Date now = DateUtil.date();
        Update update = Update.update("scheduleDate", null);
        if (taskDto.getStartTime() == null) {
            update.set("startTime", now);
        }
        if (taskDto.getLastStartDate() == null) {
            update.set("lastStartDate", now.getTime());
        }

        monitoringLogsService.startTaskMonitoringLog(taskDto, user, now);

        StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.RUNNING, user);
        if (stateMachineResult.isFail()) {
            log.info("concurrent running operations, this operation don‘t effective, task name = {}", taskDto.getName());
            return null;
        }

        update(query1, update, user);
        updateTaskRecordStatus(taskDto, TaskDto.STATUS_RUNNING, user);
        return id.toHexString();
    }

    /**
     * 收到任务运行失败的消息
     * @see DataFlowEvent#ERROR
     * @param id
     */
    public String runError(ObjectId id, UserDetail user, String errMsg, String errStack) {
        //判断任务是否存在。
        TaskDto taskDto = checkExistById(id, user, "_id", "status", "name", "taskRecordId");

        //将子任务状态更新成错误.
        StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.ERROR, user);
        if (stateMachineResult.isFail()) {
            log.info("concurrent runError operations, this operation don‘t effective, task name = {}", taskDto.getName());
            return null;
        }
        updateTaskRecordStatus(taskDto, TaskDto.STATUS_ERROR, user);

        return id.toHexString();

    }

    /**
     * 收到子任务运行完成的消息
     * @see DataFlowEvent#COMPLETED
     * @param id
     */
    public String complete(ObjectId id, UserDetail user) {
        //判断子任务是否存在
        TaskDto taskDto = checkExistById(id, user, "_id", "status", "name", "taskRecordId");

        StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.COMPLETED, user);
        if (stateMachineResult.isFail()) {
            log.info("concurrent complete operations, this operation don‘t effective, task name = {}", taskDto.getName());
            return null;
        }
        updateTaskRecordStatus(taskDto, TaskDto.STATUS_COMPLETE, user);

        return id.toHexString();
    }

    /**
     * 收到子任务已经停止的消息
     * @see DataFlowEvent#STOPPED
     * @param id
     */
    public String stopped(ObjectId id, UserDetail user) {
        //判断子任务是否存在。
        TaskDto taskDto = checkExistById(id, user, "dag", "name", "status", "_id", "taskRecordId");

        StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.STOPPED, user);

        if (stateMachineResult.isFail()) {
            log.info("concurrent stopped operations, this operation don‘t effective, task name = {}", taskDto.getName());
            return null;
        }
        updateTaskRecordStatus(taskDto, TaskDto.STATUS_STOP, user);
        return id.toHexString();
    }

    /**
     * 里程碑信息， 结构迁移信息， 全量同步信息，增量同步信息
     * 里程碑信息为子任务表中的里程碑信息， 结构迁移与全量同步保存在节点运行中间状态表中。 增量同步信息保存在
     *
     * @param id   任务id
     * @param endTime 前一次查询到的数据的结束时间， 本次查询应该为查询结束时间之后的数据， 为空则查询全部
     */
    public RunTimeInfo runtimeInfo(ObjectId id, Long endTime, UserDetail user) {
        log.debug("query task runtime info, task id = {}, endTime = {}, user = {}", id, endTime, user);

        //查询子任务是否存在
        TaskDto TaskDto = findById(id, user);
        if (TaskDto == null) {
            return null;
        }
        //查询所有的里程碑信息
        List<Milestone> milestones = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(TaskDto.getMilestones())) {
            milestones.addAll(TaskDto.getMilestones());
        }
        RunTimeInfo runTimeInfo = new RunTimeInfo();
        runTimeInfo.setMilestones(milestones);

        log.debug("runtime info ={}", runTimeInfo);
        return runTimeInfo;
    }

    public void updateNode(ObjectId objectId, String nodeId, Document param, UserDetail user) {
        TaskDto TaskDto = checkExistById(objectId, user);
        Criteria criteria = Criteria.where("_id").is(objectId).and("dag.nodes").elemMatch(Criteria.where("id").is(nodeId));
        Document set = (Document) param.get("$set");
        for (String s : set.keySet()) {
            set.put("dag.nodes.$." + s, set.get(s));
            set.remove(s);
        }
        param.put("$set", set);

        Update update = Update.fromDocument(param);
        update(new Query(criteria), update, user);

    }


    public void updateSyncProgress(ObjectId taskId, Document document) {
        document.forEach((k, v) -> {
            Criteria criteria = Criteria.where("_id").is(taskId);
            Update update = new Update().set("attrs.syncProgress." + k, v);
            update(new Query(criteria), update);
        });
    }


    public void increaseClear(ObjectId taskId, String srcNode, String tgtNode, UserDetail user) {
        //清理只需要清楚syncProgress数据就行
        TaskDto TaskDto = checkExistById(taskId, user, "attrs");
        clear(srcNode, tgtNode, user, TaskDto);

    }

    private void clear(String srcNode, String tgtNode, UserDetail user, TaskDto TaskDto) {
        Map<String, Object> attrs = TaskDto.getAttrs();
        Object syncProgress = attrs.get("syncProgress");
        if (syncProgress == null) {
            return;
        }

        Map syncProgressMap = (Map) syncProgress;
        List<String> key = Lists.newArrayList(srcNode, tgtNode);

        syncProgressMap.remove(JsonUtil.toJsonUseJackson(key));

        Update update = Update.update("attrs", attrs);
        //不需要刷新主任状态， 所以调用super, 本来中重新的自带刷新主任务状态
        super.updateById(TaskDto.getId(), update, user);
    }

    public void increaseBacktracking(ObjectId taskId, String srcNode, String tgtNode, TaskDto.SyncPoint point, UserDetail user) {
        TaskDto taskDto = checkExistById(taskId, user, "parentId", "attrs", "dag", "syncPoints");
        clear(srcNode, tgtNode, user, taskDto);


        //更新主任务中的syncPoints时间点
        DAG dag = taskDto.getDag();
        Node node = dag.getNode(tgtNode);
        if (node instanceof DataParentNode) {
            String connectionId = ((DataParentNode<?>) node).getConnectionId();
            if (StringUtils.isNotBlank(connectionId)) {
                List<TaskDto.SyncPoint> syncPoints = taskDto.getSyncPoints();
                if (CollectionUtils.isEmpty(syncPoints)) {
                    syncPoints = new ArrayList<>();
                }


                boolean exist = false;
                TaskDto.SyncPoint syncPoint = new TaskDto.SyncPoint();
                for (TaskDto.SyncPoint item : syncPoints) {
                    if (connectionId.equals(item.getConnectionId())) {
                        syncPoint = item;
                        exist = true;
                        break;
                    }
                }

                syncPoint.setPointType(point.getPointType());
                syncPoint.setDateTime(point.getDateTime());
                syncPoint.setTimeZone(point.getTimeZone());
                syncPoint.setConnectionId(connectionId);

                if (exist) {
                    Criteria criteriaPoint = Criteria.where("_id").is(taskDto.getId()).and("syncPoints")
                            .elemMatch(Criteria.where("connectionId").is(connectionId));
                    Update update = Update.update("syncPoints.$", syncPoint);
                    //更新内嵌文档
                    update(new Query(criteriaPoint), update);
                } else {
                    syncPoints.add(syncPoint);
                    Criteria criteriaPoint = Criteria.where("_id").is(taskDto.getId());
                    Update update = Update.update("syncPoints", syncPoints);
                    update(new Query(criteriaPoint), update);
                }
            }
        }

    }



//    public void reseted(ObjectId objectId, UserDetail userDetail) {
//        TaskDto TaskDto = checkExistById(objectId, userDetail, "_id");
//        if (TaskDto != null) {
//            super.updateById(objectId, Update.update("resetFlag", true), userDetail);
//        }
//    }
//
//    public void deleted(ObjectId objectId, UserDetail userDetail) {
//        TaskDto TaskDto = checkExistById(objectId, userDetail, "_id");
//        if (TaskDto != null) {
//            super.updateById(objectId, Update.update("deleteFlag", true), userDetail);
//        }
//    }

    public boolean checkPdkTask(TaskDto taskDto, UserDetail user) {
        if (true) {
            return true;
        }
        DAG dag = taskDto.getDag();
        if (dag == null) {
            return false;
        }
        List<String> connections = new ArrayList<>();
        boolean specialTask = false;
        List<Node> sources = dag.getSources();
        for (Node source : sources) {
            if (source instanceof LogCollectorNode) {
                List<String> connectionIds = ((LogCollectorNode) source).getConnectionIds();
                if (CollectionUtils.isNotEmpty(connectionIds)) {
                    connections = connectionIds;
                    specialTask = true;
                }
            }
        }

        if (!specialTask) {
            List<Node> nodes = dag.getNodes();
            if (CollectionUtils.isEmpty(nodes)) {
                return false;
            }

            connections = nodes.stream().filter(n -> n instanceof DataParentNode).map(n -> ((DataParentNode<?>) n).getConnectionId())
                    .collect(Collectors.toList());
        }

        List<DataSourceConnectionDto> connectionDtos = dataSourceService.findInfoByConnectionIdList(connections, user, "pdkType");

        for (DataSourceConnectionDto connectionDto : connectionDtos) {
            if (DataSourceDefinitionDto.PDK_TYPE.equals(connectionDto.getPdkType())) {
                return true;
            }
        }

        return false;
    }

//    public boolean checkDeleteFlag(ObjectId id, UserDetail user) {
//        TaskDto TaskDto = checkExistById(id, user, "deleteFlag");
//        if (TaskDto.getDeleteFlag() != null) {
//            return TaskDto.getDeleteFlag();
//        }
//        return false;
//    }
//
//    public boolean checkResetFlag(ObjectId id, UserDetail user) {
//        TaskDto TaskDto = checkExistById(id, user, "resetFlag");
//        if (TaskDto.getResetFlag() != null) {
//            return TaskDto.getResetFlag();
//        }
//        return false;
//    }
//    public void resetFlag(ObjectId id, UserDetail user, String flag) {
//        updateById(id, new Update().unset(flag), user);
//    }

    public void startPlanMigrateDagTask() {
        Criteria migrateCriteria = Criteria.where("status").is(TaskDto.STATUS_WAIT_START)
                .and("planStartDateFlag").is(true)
                .and("planStartDate").lte(DateUtil.current());
        Query taskQuery = new Query(migrateCriteria);
        List<TaskDto> taskList = findAll(taskQuery);
        if (CollectionUtils.isNotEmpty(taskList)) {
            taskList = taskList.stream().filter(t -> Objects.nonNull(t.getTransformed()) && t.getTransformed())
                    .collect(Collectors.toList());

            List<String> userIdList = taskList.stream().map(TaskDto::getUserId).distinct().collect(Collectors.toList());
            List<UserDetail> userList = userService.getUserByIdList(userIdList);

            Map<String, UserDetail> userMap = new HashMap<>();
            if (CollectionUtils.isNotEmpty(userList)) {
                userMap = userList.stream().collect(Collectors.toMap(UserDetail::getUserId, Function.identity()));
            }

            Map<String, UserDetail> finalUserMap = userMap;
            for (TaskDto taskDto : taskList) {
                run(taskDto, finalUserMap.get(taskDto.getUserId()));
                //run(taskDto, finalUserMap.get(taskDto.getUserId()));
                //启动过后，应该更新掉这个自动启动计划
                Update unset = new Update().unset("planStartDateFlag").unset("planStartDate");
                updateById(taskDto.getId(), unset, finalUserMap.get(taskDto.getUserId()));
            }


        }
    }

    public void startPlanCronTask() {
        Criteria migrateCriteria = Criteria.where("crontabExpressionFlag").is(true)
                .and("crontabExpression").exists(true)
                .and("is_deleted").is(false)
                .andOperator(Criteria.where("status").nin(TaskDto.STATUS_EDIT,TaskDto.STATUS_STOPPING,
                        TaskDto.STATUS_RUNNING,TaskDto.STATUS_RENEWING,TaskDto.STATUS_DELETING));
        Query taskQuery = new Query(migrateCriteria);
        List<TaskDto> taskList = findAll(taskQuery);
        if (CollectionUtils.isNotEmpty(taskList)) {
            taskList = taskList.stream().filter(t -> Objects.nonNull(t.getTransformed()) && t.getTransformed())
                    .collect(Collectors.toList());
            for (TaskDto taskDto : taskList) {
                scheduleService.executeTask(taskDto);
            }
        }
    }

    public TaskDto findByCacheName(String cacheName, UserDetail user) {
        Criteria taskCriteria = Criteria.where("dag.nodes").elemMatch(Criteria.where("catalog").is("memCache").and("cacheName").is(cacheName));
        Query query = new Query(taskCriteria);

        return findOne(query, user);
    }

    public void updateDag(TaskDto TaskDto, UserDetail user, boolean saveHistory) {
        TaskDto TaskDto1 = checkExistById(TaskDto.getId(), user);

        Criteria criteria = Criteria.where("_id").is(TaskDto.getId());
        Update update = Update.update("dag", TaskDto.getDag());
        long tmCurrentTime = System.currentTimeMillis();
        if (saveHistory) {
            update.set("tmCurrentTime", tmCurrentTime);
        }
        repository.update(new Query(criteria), update, user);

        if (saveHistory) {
            TaskHistory taskHistory = new TaskHistory();
            BeanUtils.copyProperties(TaskDto1, taskHistory);
            taskHistory.setTaskId(TaskDto1.getId().toHexString());
            taskHistory.setId(ObjectId.get());

            //保存任务历史
            repository.getMongoOperations().insert(taskHistory, "DDlTaskHistories");
        }

    }

    public TaskDto findByVersionTime(String id, Long time) {
        Criteria criteria = Criteria.where("taskId").is(id);
        criteria.and("tmCurrentTime").is(time);

        Query query = new Query(criteria);

        TaskDto dDlTaskHistories = repository.getMongoOperations().findOne(query, TaskHistory.class, "DDlTaskHistories");

        if (dDlTaskHistories == null) {
            dDlTaskHistories = findById(MongoUtils.toObjectId(id));
        } else {
            dDlTaskHistories.setId(MongoUtils.toObjectId(id));
        }
        return dDlTaskHistories;
    }

    /**
     *
     * @param time 最近时间戳
     * @return
     */
    public void clean(String taskId, Long time) {
        Criteria criteria = Criteria.where("taskId").is(taskId);
        criteria.and("tmCurrentTime").gt(time);

        Query query = new Query(criteria);
        repository.getMongoOperations().remove(query, "DDlTaskHistories");

        //清理模型
        //MetaDataHistoryService historyService = SpringContextHelper.getBean(MetaDataHistoryService.class);
        historyService.clean(taskId, time);
    }

    public Map<String, Object> totalAutoInspectResultsDiffTables(IdParam param) {
        String taskId = param.getId();
        Assert.notBlank(taskId, "id not blank");

        Map<String, Object> data = new HashMap<>();

        TaskDto taskDto = findByTaskId(new ObjectId(taskId), AutoInspectConstants.AUTO_INSPECT_PROGRESS_PATH);
        if (null != taskDto) {
            AutoInspectProgress progress = AutoInspectUtil.toAutoInspectProgress(taskDto.getAttrs());
            if (null != progress) {
                data.put("totals", progress.getTableCounts());
                data.put("ignore", progress.getTableIgnore());
                Map<String, Object> map = taskAutoInspectResultsService.totalDiffTables(taskId);
                if (null != map) {
                    data.put("diffTables", map.get("tables"));
                    data.put("diffRecords", map.get("totals"));
                }
            }
        }
        return data;
    }

    public void updateTaskLogSetting(String taskId, LogSettingParam logSettingParam, UserDetail userDetail) {
        ObjectId taskObjectId = new ObjectId(taskId);
        TaskDto task = findById(taskObjectId);
        if (null == task) {
            throw new BizException("Task.NotFound", "The task does not exist");
        }

        Map<String, Object> logSetting = task.getLogSetting();
        if (null == logSetting) {
            logSetting = new HashMap<>();
        }

        String level = logSettingParam.getLevel();
        logSetting.put("level", level);
        if (level.equalsIgnoreCase("DEBUG")) {
            logSetting.put("recordCeiling", logSettingParam.getRecordCeiling());
            logSetting.put("intervalCeiling", logSettingParam.getIntervalCeiling());
        }

        Update update = new Update();
        update.set("logSetting", logSetting);
        updateById(taskObjectId, update, userDetail);
    }

    public Map<String, Long> chart6(UserDetail user) {
        Criteria criteria = Criteria.where("is_deleted").ne(true).and("syncType").in(TaskDto.SYNC_TYPE_SYNC, TaskDto.SYNC_TYPE_MIGRATE);
        Query query = new Query(criteria);
        query.fields().include("_id");
        List<TaskDto> allDto = findAllDto(query, user);
        List<String> ids = allDto.stream().map(a->a.getId().toHexString()).collect(Collectors.toList());

        List<MeasurementEntity>  allMeasurements = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(ids)) {
            ids.parallelStream().forEach(id -> {
                MeasurementEntity measurement = measurementServiceV2.findLastMinuteByTaskId(id);
                if (measurement != null) {
                    allMeasurements.add(measurement);
                }
            });
        }

        long output = 0;
        long input = 0;
        long insert = 0;
        long update = 0;
        long delete = 0;

        for (MeasurementEntity allMeasurement : allMeasurements) {
            if (allMeasurement == null) {
                continue;
            }
            List<Sample> samples = allMeasurement.getSamples();
            if (CollectionUtils.isNotEmpty(samples)) {
                Optional<Sample> max = samples.stream().max(Comparator.comparing(Sample::getDate));
                if (max.isPresent()) {
                    Sample sample = max.get();
                    Map<String, Number> vs = sample.getVs();
                    long inputInsertTotal = Long.parseLong(String.valueOf(vs.get("inputInsertTotal")));
                    long inputOthersTotal = Long.parseLong(String.valueOf(vs.get("inputOthersTotal")));
                    long inputDdlTotal = Long.parseLong(String.valueOf(vs.get("inputDdlTotal")));
                    long inputUpdateTotal = Long.parseLong(String.valueOf(vs.get("inputUpdateTotal")));
                    long inputDeleteTotal = Long.parseLong(String.valueOf(vs.get("inputDeleteTotal")));

                    long outputInsertTotal = Long.parseLong(String.valueOf(vs.get("outputInsertTotal")));
                    long outputOthersTotal = Long.parseLong(String.valueOf(vs.get("outputOthersTotal")));
                    long outputDdlTotal = Long.parseLong(String.valueOf(vs.get("outputDdlTotal")));
                    long outputUpdateTotal = Long.parseLong(String.valueOf(vs.get("outputUpdateTotal")));
                    long outputDeleteTotal = Long.parseLong(String.valueOf(vs.get("outputDeleteTotal")));
                    output += outputInsertTotal;
                    output += outputOthersTotal;
                    output += outputDdlTotal;
                    output += outputUpdateTotal;
                    output += outputDeleteTotal;

                    input += inputInsertTotal;
                    input += inputOthersTotal;
                    input += inputDdlTotal;
                    input += inputUpdateTotal;
                    input += inputDeleteTotal;

                    insert += inputInsertTotal;
                    update += inputUpdateTotal;
                    delete += inputDeleteTotal;

                }
            }
        }

        Map<String, Long> chart6Map = new HashMap<>();
        chart6Map.put("outputTotal", output);
        chart6Map.put("inputTotal", input);
        chart6Map.put("insertedTotal", insert);
        chart6Map.put("updatedTotal", update);
        chart6Map.put("deletedTotal", delete);
        return chart6Map;
    }


}
