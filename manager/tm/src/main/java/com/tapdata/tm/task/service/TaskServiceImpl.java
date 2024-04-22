package com.tapdata.tm.task.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.map.MapUtil;
import cn.hutool.extra.cglib.CglibUtil;
import com.alibaba.fastjson.JSON;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.Settings.service.SettingsServiceImpl;
import com.tapdata.tm.agent.service.AgentGroupService;
import com.tapdata.tm.autoinspect.constants.AutoInspectConstants;
import com.tapdata.tm.autoinspect.entity.AutoInspectProgress;
import com.tapdata.tm.autoinspect.service.TaskAutoInspectResultsService;
import com.tapdata.tm.autoinspect.utils.AutoInspectUtil;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.handler.ExceptionHandler;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.dag.*;
import com.tapdata.tm.commons.dag.nodes.*;
import com.tapdata.tm.commons.dag.process.*;
import com.tapdata.tm.commons.dag.process.script.ScriptProcessNode;
import com.tapdata.tm.commons.dag.process.script.py.MigratePyProcessNode;
import com.tapdata.tm.commons.dag.process.script.py.PyProcessNode;
import com.tapdata.tm.commons.dag.vo.*;
import com.tapdata.tm.commons.externalStorage.ExternalStorageDto;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.task.constant.NotifyEnum;
import com.tapdata.tm.commons.task.dto.*;
import com.tapdata.tm.commons.task.dto.alarm.AlarmSettingVO;
import com.tapdata.tm.commons.task.dto.migrate.MigrateTableDto;
import com.tapdata.tm.commons.task.dto.progress.TaskSnapshotProgress;
import com.tapdata.tm.commons.util.*;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.customNode.dto.CustomNodeDto;
import com.tapdata.tm.customNode.service.CustomNodeService;
import com.tapdata.tm.dataflowinsight.dto.DataFlowInsightStatisticsDto;
import com.tapdata.tm.disruptor.constants.DisruptorTopicEnum;
import com.tapdata.tm.disruptor.service.DisruptorService;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceServiceImpl;
import com.tapdata.tm.externalStorage.service.ExternalStorageService;
import com.tapdata.tm.file.service.FileService;
import com.tapdata.tm.inspect.bean.Source;
import com.tapdata.tm.inspect.bean.Stats;
import com.tapdata.tm.inspect.constant.InspectResultEnum;
import com.tapdata.tm.inspect.constant.InspectStatusEnum;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.dto.InspectResultDto;
import com.tapdata.tm.inspect.service.InspectResultService;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.lock.service.LockControlService;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.service.MessageServiceImpl;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueServiceImpl;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.metadatainstance.service.MetaDataHistoryService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesServiceImpl;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.param.IdParam;
import com.tapdata.tm.monitor.service.MeasurementServiceV2;
import com.tapdata.tm.monitoringlogs.service.MonitoringLogsService;
import com.tapdata.tm.permissions.DataPermissionHelper;
import com.tapdata.tm.permissions.constants.DataPermissionActionEnums;
import com.tapdata.tm.permissions.constants.DataPermissionMenuEnums;
import com.tapdata.tm.schedule.ChartSchedule;
import com.tapdata.tm.schedule.service.ScheduleService;
import com.tapdata.tm.statemachine.enums.DataFlowEvent;
import com.tapdata.tm.statemachine.model.StateMachineResult;
import com.tapdata.tm.statemachine.service.StateMachineService;
import com.tapdata.tm.task.bean.*;
import com.tapdata.tm.task.constant.*;
import com.tapdata.tm.task.dto.CheckEchoOneNodeParam;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.entity.TaskRecord;
import com.tapdata.tm.task.param.LogSettingParam;
import com.tapdata.tm.task.param.SaveShareCacheParam;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.task.service.batchup.BatchUpChecker;
import com.tapdata.tm.task.service.utils.TaskServiceUtil;
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
import com.tapdata.tm.worker.vo.CalculationEngineVo;
import com.tapdata.tm.ws.enums.MessageType;
import io.tapdata.common.sample.request.Sample;
import io.tapdata.exception.TapCodeException;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;
import org.quartz.CronScheduleBuilder;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;
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
public class TaskServiceImpl extends TaskService{
    public static final String USER_ID = "user_id";
    public static final String COLLECTION_ID = "collectionId";
    protected static final String PROCESSOR_THREAD_NUM="processorThreadNum";
    protected static final String CATALOG="catalog";
    protected static final String ELEMENT_TYEP="elementType";
    protected static final String PROCESSOR="processor";
    public static final String RM_ID_KEY = "rm_id";
    public static final String SYNC_TYPE = "syncType";
    public static final String ILLEGAL_ARGUMENT = "IllegalArgument";
    public static final String INITIAL_SYNC_CDC = "initial_sync+cdc";
    public static final String QUALIFIED_NAME = "qualified_name";
    public static final String FIELDS = "fields";
    public static final String IS_DELETED = "is_deleted";
    public static final String THE_COPIED_TASK_DOES_NOT_EXIST = "The copied task does not exist";
    public static final String CRONTAB_EXPRESSION_FLAG = "crontabExpressionFlag";
    public static final String CONNECTION_ID = "connectionId";
    public static final String TM_CURRENT_TIME = "tmCurrentTime";
    public static final String PLAN_START_DATE_FLAG = "planStartDateFlag";
    public static final String IS_PRIMARY_KEY = "isPrimaryKey";
    public static final String INITIAL_SYNC = "initial_sync";
    public static final String CURRENT_EVENT_TIMESTAMP = "currentEventTimestamp";
    public static final String ATTRS = "attrs";
    public static final String SYNC_POINTS = "syncPoints";
    public static final String DAG_NODES = "dag.nodes";
    public static final String REGEX = "$regex";
    public static final String DELETE_TASK_HANDLE_EXCEPTION_ERROR_TASK_ID = "delete task, handle exception error, task id = {}";
    public static final String TOTAL = "total";
    public static final String CONNECTION_NAME = "connectionName";
    public static final String TABLE = "table";
    public static final String TASK_LIST_WARN_MESSAGE = "Task.ListWarnMessage";
    public static final String EDGE_MILESTONES = "edgeMilestones";
    public static final String SYNC_PROGRESS = "syncProgress";
    public static final String TASK_NOT_FOUND = "Task.NotFound";
    public static final String STATUS = "status";
    public static final String CREATE_TIME = "createTime";
    public static final String DAG_NODES_CONNECTION_ID = "dag.nodes.connectionId";
    public static final String TABLE_NAME = "tableName";
    public static final String COUNT = "count";
    public static final String DATETIME_PATTERN = "yyyyMMdd";
    public static final String METADATA_INSTANCES = "MetadataInstances";
    public static final String CHILDREN = "children";
    public static final String TARGET_PATH = "targetPath";
    public static final String FIELD = "field";
    public static final String COLLECTIONS = "collections";
    public static final String RELATIONSHIPS = "relationships";
    public static final String TASK_IMPORT_FORMAT_ERROR = "Task.ImportFormatError";
    public static final String START_TIME = "startTime";
    public static final String STOP_RETRY_TIMES = "stopRetryTimes";
    public static final String SCHEDULE_DATE = "scheduleDate";
    public static final String FUNCTION_RETRY_STATUS = "functionRetryStatus";
    public static final String TASK_ID = "taskId";
    public static final String MAPPINGS = "mappings";
    public static final String TASK_RECORD_ID = "taskRecordId";
    public static final String RESTART_FLAG = "restartFlag";
    public static final String AGENT_ID = "agentId";
    public static final String SOURCE = "source";
    public static final String EMBEDDED_PATH = "embeddedPath";
    public static final String COLUMN = "column";
    public static final String STOPED_DATE = "stopedDate";
    public static final String TARGET = "target";

    @NotNull
    private static String getTableName() {
        return TABLE_NAME;
    }

    private MessageServiceImpl messageService;
    private SnapshotEdgeProgressService snapshotEdgeProgressService;
    private InspectService inspectService;
    private TaskRunHistoryService taskRunHistoryService;
    private TransformSchemaAsyncService transformSchemaAsyncService;
    private TransformSchemaService transformSchemaService;
    private DataSourceServiceImpl dataSourceService;
    private MetadataTransformerService transformerService;
    private MetadataInstancesServiceImpl metadataInstancesService;
    private MetadataTransformerItemService metadataTransformerItemService;
    private MetaDataHistoryService historyService;
    private WorkerService workerService;
    private FileService fileService1;
    private UserLogService userLogService;
    private MessageQueueServiceImpl messageQueueService;
    private UserService userService;
    private DisruptorService disruptorService;
    private MonitoringLogsService monitoringLogsService;
    private TaskAutoInspectResultsService taskAutoInspectResultsService;
    private TaskSaveService taskSaveService;
    private MeasurementServiceV2 measurementServiceV2;

    private LogCollectorService logCollectorService;

    private TaskResetLogService taskResetLogService;

    private TaskCollectionObjService taskCollectionObjService;

    private ExceptionHandler exceptionHandler;

    private StateMachineService stateMachineService;
    private TaskScheduleService taskScheduleService;

    private ScheduleService scheduleService;

    private MetadataDefinitionService metadataDefinitionService;

    private InspectResultService inspectResultService;

    private CustomNodeService customNodeService;

    private ExternalStorageService externalStorageService;


    private CustomSqlService customSqlService;


    private LockControlService lockControlService;

    private TaskUpdateDagService taskUpdateDagService;

    private DateNodeService dateNodeService;

    private final Map<String, ReentrantLock> scheduleLockMap = new ConcurrentHashMap<>();

    private SettingsServiceImpl settingsService;

    private TaskNodeService taskNodeService;

    private AgentGroupService agentGroupService;
    private DataSourceDefinitionService dataSourceDefinitionService;
    private BatchUpChecker batchUpChecker;

    public TaskServiceImpl(@NonNull TaskRepository repository) {
        super(repository);
    }

	public Supplier<TaskDto> dataPermissionFindById(ObjectId taskId, Field fields) {
		return () -> {
			if (null != fields) {
				fields.put(USER_ID, true);
				fields.put(SYNC_TYPE, true);
				fields.put(DataPermissionHelper.FIELD_NAME, true);
				fields.put(ConnHeartbeatUtils.TASK_RELATION_FIELD, true);
			}
			return findById(taskId, fields);
		};
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

        if (StringUtils.isNotEmpty(taskDto.getCrontabExpression()) && taskDto.getCrontabExpressionFlag() != null && taskDto.getCrontabExpressionFlag()) {
            try {
                CronScheduleBuilder.cronSchedule(taskDto.getCrontabExpression());
            } catch (Exception e) {
                throw new BizException("Task.CronError");
            }
        }

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

        customSqlService.checkCustomSqlTask(taskDto, user);
        dateNodeService.checkTaskDateNode(taskDto, user);

        boolean rename = false;
        if (taskDto.getId() != null) {
            Criteria criteria = Criteria.where("_id").is(taskDto.getId().toHexString());
            Query query = new Query(criteria);
            query.fields().include("name");
            TaskDto old = findOne(query, user);
            if (old != null) {
                String name = old.getName();
                if (StringUtils.isNotBlank(name) && StringUtils.isNotBlank(taskDto.getName()) && !name.equals(taskDto.getName())) {
                    rename = true;
                }
            }
        }

        String editVersion = buildEditVersion(taskDto);
        taskDto.setEditVersion(editVersion);
        taskDto.setTestTaskId(new ObjectId().toHexString());
        taskDto.setTransformTaskId(new ObjectId().toHexString());

        //模型推演
        setDefault(taskDto);
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

    public <T> AggregationResults<T> aggregate(org.springframework.data.mongodb.core.aggregation.Aggregation aggregation, Class<T> outputType) {
        return repository.aggregate(aggregation, outputType);
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
        taskDto.setType(getStringValue(taskDto.getType(), INITIAL_SYNC_CDC));

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
        //setDefault(task);

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
                    syncPoint.setNodeId(tableNode.getId());
                    syncPoint.setNodeName(tableNode.getName());
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

        if (StringUtils.isNotEmpty(taskDto.getCrontabExpression()) && taskDto.getCrontabExpressionFlag() != null && taskDto.getCrontabExpressionFlag()) {
            try {
                CronScheduleBuilder.cronSchedule(taskDto.getCrontabExpression());
            } catch (Exception e) {
                throw new BizException("Task.CronError");
            }
        }

        if (taskDto.getId() != null) {
            oldTaskDto = findById(taskDto.getId());
            if (oldTaskDto != null) {
                taskDto.setSyncType(oldTaskDto.getSyncType());
                taskDto.setTestTaskId(oldTaskDto.getTestTaskId());
                taskDto.setTransformTaskId(oldTaskDto.getTransformTaskId());

                TaskServiceUtil.copyAccessNodeInfo(oldTaskDto, taskDto);

                if (TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType()) && !ParentTaskDto.TYPE_CDC.equals(taskDto.getType())) {
                    DAG newDag = taskDto.getDag();
                    if (newDag != null) {
                        List<String> runTables = measurementServiceV2.findRunTable(taskDto.getId().toHexString(), oldTaskDto.getTaskRecordId());
                        if (CollectionUtils.isNotEmpty(runTables)) {
                            LinkedList<DatabaseNode> newSourceNode = newDag.getSourceNode();
                            if (CollectionUtils.isNotEmpty(newSourceNode)) {
                                DatabaseNode newFirst = newSourceNode.getFirst();
                                if (newFirst.getTableNames() != null) {
                                    List<String> newTableNames = new ArrayList<>(newFirst.getTableNames());
                                    newTableNames.removeAll(runTables);
                                    taskDto.setLdpNewTables(newTableNames);
                                }

                            }
                        } else {
                            List<String> ldpNewTables = taskDto.getLdpNewTables();
                            if (CollectionUtils.isNotEmpty(ldpNewTables)) {
                                taskDto.setLdpNewTables(null);
                            }
                        }
                    }
                }
                log.debug("old task = {}", oldTaskDto);
            }
        }

        if (oldTaskDto == null) {
            log.debug("task not found, need create new task, task id = {}", taskDto.getId());
            return create(taskDto, user);
        }


        customSqlService.checkCustomSqlTask(taskDto, user);
        dateNodeService.checkTaskDateNode(taskDto, user);

        boolean agentReq = isAgentReq();
        if (!agentReq) {
            if (taskDto.getEditVersion() != null && !oldTaskDto.getEditVersion().equals(taskDto.getEditVersion())) {
                if (taskDto.getPageVersion() != null && oldTaskDto.getPageVersion() != null && !oldTaskDto.getPageVersion().equals(taskDto.getPageVersion())) {
                    throw new BizException("Task.OldVersion");
                }
            }
        }

        //改名不能重复
        if (StringUtils.isNotBlank(taskDto.getName()) && !taskDto.getName().equals(oldTaskDto.getName())) {
            checkTaskName(taskDto.getName(), user, taskDto.getId());
        }

        // supplement migrate_field_rename_processor fieldMapping data
        //supplementMigrateFieldMapping(taskDto, user);
        taskSaveService.syncTaskSetting(taskDto, user);

        //校验dag
        DAG dag = taskDto.getDag();
        int dagHash = 0;
        if (dag != null) {
            if (TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
                if (CollectionUtils.isNotEmpty(dag.getSourceNode())) {
                    transformSchemaAsyncService.transformSchema(dag, user, taskDto.getId());
                }
            } else {
                transformSchemaService.transformSchema(dag, user, taskDto.getId());
            }
        }
        log.debug("check task dag complete, task id =- {}", taskDto.getId());

        if (!isAgentReq()) {
            String editVersion = buildEditVersion(taskDto);
            taskDto.setEditVersion(editVersion);
        }
        checkUnwindProcess(dag);


        //更新任务
        log.debug("update task, task dto = {}", taskDto);
        //推演的时候改的，这里必须清空掉。清空只是不会被修改。
        taskDto.setTransformed(null);
        taskDto.setTransformUuid(null);
        taskDto.setTransformDagHash(dagHash);

        taskDto.setWriteBatchSize(null);
        taskDto.setWriteBatchWaitMs(null);

        if (StringUtils.isEmpty(taskDto.getTestTaskId())) {
            taskDto.setTestTaskId(new ObjectId().toHexString());
        }
        if (StringUtils.isEmpty(taskDto.getTransformTaskId())) {
            taskDto.setTransformTaskId(new ObjectId().toHexString());
        }

        return save(taskDto, user);

    }

    public TaskDto updateAfter(TaskDto taskDto, UserDetail user) {
        taskSaveService.syncTaskSetting(taskDto, user);
        return save(taskDto, user);
    }

    private void supplementMigrateFieldMapping(TaskDto taskDto, UserDetail userDetail) {
        if (!TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
            return;
        }

        DAG dag = taskDto.getDag();
        if (Objects.isNull(dag) || CollectionUtils.isEmpty(dag.getNodes())) {
            return;
        }

        dag.getNodes().forEach(node -> {
            if (node instanceof MigrateFieldRenameProcessorNode) {
                MigrateFieldRenameProcessorNode fieldNode = (MigrateFieldRenameProcessorNode) node;
                LinkedList<TableFieldInfo> fieldsMapping = fieldNode.getFieldsMapping();

                if (CollectionUtils.isNotEmpty(fieldsMapping)) {
                    List<String> tableNames = fieldsMapping.stream()
                            .map(TableFieldInfo::getOriginTableName)
                            .collect(Collectors.toList());
                    LinkedList<Node<?>> preNodes = node.getDag().getPreNodes(node.getId());
                    if (CollectionUtils.isEmpty(preNodes)) {
                        return;
                    }
                    Node previousNode = preNodes.getLast();

                    List<MetadataInstancesDto> metaList = metadataInstancesService.findByNodeId(previousNode.getId(),
                            userDetail, taskDto.getId().toHexString(), QUALIFIED_NAME, FIELDS);
                    Map<String, List<com.tapdata.tm.commons.schema.Field>> fieldMap = metaList.stream()
                            .collect(Collectors.toMap(MetadataInstancesDto::getAncestorsName, MetadataInstancesDto::getFields));
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

                        List<com.tapdata.tm.commons.schema.Field> tableFields = fieldMap.get(table.getOriginTableName());
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
        if (StringUtils.isBlank(newName)) {
            throw new BizException("Task.NameIsNull");
        }
        if (checkTaskNameNotError(newName, user, id)) {
            throw new BizException("Task.RepeatName");
        }
    }

    public boolean checkTaskNameNotError(String newName, UserDetail user, ObjectId id) {

        Criteria criteria = Criteria.where("name").is(newName).and(IS_DELETED).ne(true);
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
        taskDto = confirmById(taskDto, user, confirm);
        try {
            start(taskDto, user, "11");
        } catch (Exception e) {
            monitoringLogsService.startTaskErrorLog(taskDto, user, e, Level.ERROR);
            throw e;
        }
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
            TaskServiceUtil.copyAccessNodeInfo(temp, taskDto);
        }
        // check task inspect flag
        checkTaskInspectFlag(taskDto);

        checkDagAgentConflict(taskDto, user, true);

        checkDDLConflict(taskDto);

        //saveInspect(existedTask, taskDto, user);

        return confirmById(taskDto, user, confirm, false);
    }

    protected void checkDDLConflict(TaskDto taskDto) {
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
                    boolean anyMatch = taskDto.getDag().getNodes().stream().anyMatch(n -> n instanceof MigrateJsProcessorNode || n instanceof MigratePyProcessNode);
                    FunctionUtils.isTure(anyMatch).throwMessage("Task.DDL.Conflict.Migrate");
                },
                () -> {
                    boolean anyMatch = taskDto.getDag().getNodes().stream().anyMatch(n -> n instanceof JsProcessorNode || n instanceof PyProcessNode);
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
    protected Boolean isInspectPropertyChanged(TaskDto existedTask, TaskDto newTask) {
        Boolean changed = false;

        DatabaseNode existedSourceDataNode = (DatabaseNode) getSourceNode(existedTask);
        DatabaseNode existedTargetDataNode = (DatabaseNode) getTargetNode(existedTask);


        DatabaseNode newSourceDataNode = (DatabaseNode) getSourceNode(newTask);
        DatabaseNode newTargetDataNode = (DatabaseNode) getTargetNode(newTask);

        if (null == existedSourceDataNode || null == existedTargetDataNode ||
                null == newSourceDataNode || null == newTargetDataNode){
            throw new BizException(ILLEGAL_ARGUMENT,"dataNode");
        }
        if (!existedSourceDataNode.getName().equals(newSourceDataNode.getName()) ||
                !existedTargetDataNode.getName().equals(newTargetDataNode.getName())
        ) {
            changed = true;
        } else {
            List<SyncObjects> newSyncObjects = newTargetDataNode.getSyncObjects();
            List<SyncObjects> existedSyncObjects = existedTargetDataNode.getSyncObjects();
            Optional<SyncObjects> newTableSyncObject = newSyncObjects.stream().filter(e -> TABLE.equals(e.getType())).findFirst();
            Optional<SyncObjects> existedTableSyncObject = existedSyncObjects.stream().filter(e -> TABLE.equals(e.getType())).findFirst();

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
                    throw new BizException(TASK_LIST_WARN_MESSAGE, validateMessage);
                }
            }
        }

        updateById(taskDto, user);

        StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.CONFIRM, user);

        return taskDto;
    }

    public void checkEngineStatus(TaskDto taskDto, UserDetail user) {
        String errCode = "Agent.Not.Found";
        String accessNodeType = taskDto.getAccessNodeType();
        List<String> taskProcessIdList = agentGroupService.getProcessNodeListWithGroup(taskDto, user);
        if (AccessNodeTypeEnum.isGroupManually(accessNodeType) && taskProcessIdList.isEmpty()) {
            throw new BizException(errCode);
        }
        List<Worker> availableAgentByAccessNode = workerService.findAvailableAgentByAccessNode(user, taskProcessIdList);
        if (CollectionUtils.isEmpty(availableAgentByAccessNode)) {
            throw new BizException(errCode);
        }
    }

    public void checkDagAgentConflict(TaskDto taskDto, UserDetail user, boolean showListMsg) {
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
            AtomicReference<String> nodeType = new AtomicReference<>();
            AtomicReference<String> nodeId = new AtomicReference<>();
            dag.getNodes().forEach(node -> {
                if (node instanceof DataParentNode) {
                    DataParentNode<?> dataParentNode = (DataParentNode<?>) node;
                    DataSourceConnectionDto connectionDto = collect.get(dataParentNode.getConnectionId());
                    Assert.notNull(connectionDto, "task connectionDto is null id:" + dataParentNode.getConnectionId());

                    checkEchoOneNode(taskDto, new CheckEchoOneNodeParam(connectionDto, dataParentNode, taskProcessIdList, validateMessage, message, nodeType, nodeId), user);
                }
            });
        }
        if (!validateMessage.isEmpty()) {
            if (showListMsg) {
                throw new BizException(TASK_LIST_WARN_MESSAGE, validateMessage);
            } else {
                Message message = validateMessage.values().iterator().next().get(0);
                throw new BizException(message.getCode(), message.getMsg());
            }
        }
    }

    protected boolean checkEchoOneNode(TaskDto taskDto, CheckEchoOneNodeParam param, UserDetail user) {
        DataSourceConnectionDto connectionDto = param.getConnectionDto();
        DataParentNode<?> dataParentNode = param.getDataParentNode();
        List<String> taskProcessIdList = param.getTaskProcessIdList();
        Map<String, List<Message>> validateMessage = param.getValidateMessage();
        Message message = param.getMessage();
        AtomicReference<String> nodeType = param.getNodeType();
        AtomicReference<String> nodeId = param.getNodeId();
        String accessNodeType = connectionDto.getAccessNodeType();
        if (!AccessNodeTypeEnum.isManually(accessNodeType)) {
            return true;
        }
        String parentNodeId = dataParentNode.getId();
        if (contrast(nodeType, parentNodeId, accessNodeType, validateMessage, message)) {
            return true;
        }
        if (AccessNodeTypeEnum.isUserManually(accessNodeType)) {
            List<String> connectionProcessIds = agentGroupService.getProcessNodeListWithGroup(connectionDto, user);
            connectionProcessIds.removeAll(taskProcessIdList);
            if (!StringUtils.equalsIgnoreCase(taskDto.getAccessNodeType(), accessNodeType)
                    || !connectionProcessIds.isEmpty()) {
                validateMessage.put(parentNodeId, Lists.newArrayList(message));
            }
        } else {
            contrast(nodeId, parentNodeId, connectionDto.getAccessNodeProcessId(), validateMessage, message);
        }
        return false;
    }

    protected boolean contrast(AtomicReference<String> ato,
                               String nodeId,
                               String atoValue,
                               Map<String, List<Message>> validateMessage,
                               Message message) {
        if (null == ato.get()) {
            ato.set(atoValue);
            return false;
        }
        if (!ato.get().equalsIgnoreCase(atoValue)) {
            validateMessage.put(nodeId, Lists.newArrayList(message));
            return true;
        }
        return false;
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
            boolean noAgent = findAgent(taskDto, user);

            Update update = resetUpdate();
            String nameSuffix = RandomStringUtils.randomAlphanumeric(6);
            update.set("name", taskDto.getName() + "_" + nameSuffix);
            update.set("deleteName", taskDto.getName());
            this.update(new Query(Criteria.where("id").is(taskDto.getId())), update);

            if (noAgent) {
                afterRemove(taskDto, user);
                String connectionName = judgePostgreClearSlot(taskDto, DataSyncMq.OP_TYPE_DELETE);
                if(StringUtils.isNotEmpty(connectionName)){
                    throw new BizException("Clear.Slot",connectionName);
                }
            } else {
                sendRenewMq(taskDto, user, DataSyncMq.OP_TYPE_DELETE);
            }

        }
        //afterRemove(taskDto, user);

        return taskDto;
    }

    public void afterRemove(TaskDto taskDto, UserDetail user) {
        //将任务删除标识改成true
        ObjectId id = taskDto.getId();
        Update update = resetUpdate();
        update.set(IS_DELETED, true);
        update(new Query(Criteria.where("_id").is(id)), update);

        //delete AutoInspectResults
        taskAutoInspectResultsService.cleanResultsByTask(taskDto);

        //add message
        if (SyncType.MIGRATE.getValue().equals(taskDto.getSyncType())) {
            messageService.addMigration(taskDto.getDeleteName(), taskDto.getId().toString(), MsgTypeEnum.DELETED, Level.WARN, user);
        } else if (SyncType.SYNC.getValue().equals(taskDto.getSyncType())) {
            messageService.addSync(taskDto.getDeleteName(), taskDto.getId().toString(), MsgTypeEnum.DELETED, "", Level.WARN, user);
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
        update(new Query(Criteria.where("_id").is(id)), Update.update(IS_DELETED, true));
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

            // 更新增量同步时间配置
            if (null != taskDto.getSyncPoints()) {
                for (TaskDto.SyncPoint sp : taskDto.getSyncPoints()) {
                    if (null == sp.getNodeId()) continue;
                    sp.setNodeId(oldnewNodeIdMap.get(sp.getNodeId()));
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
        taskDto.setCrontabScheduleMsg(null);
        if (taskDto.getAttrs() != null) {
            taskDto.getAttrs().remove("SNAPSHOT_ORDER_LIST");
        }
        Map<String, Object> attrs = taskDto.getAttrs();
        if (null != attrs) {
            attrs.remove(EDGE_MILESTONES);
            attrs.remove(SYNC_PROGRESS);
        }
        //taskDto.setTemp(null);
        if(!checkCloudTaskLimit(id,user,false)){
            taskDto.setCrontabExpressionFlag(false);
            taskDto.setCrontabExpression(null);
        }
        //创建新任务， 直接调用事务不会生效
        TaskServiceImpl taskService = SpringContextHelper.getBean(TaskServiceImpl.class);

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
        renew(id, user, false);
    }

    public void renew(ObjectId id, UserDetail user, boolean system) {
        TaskDto taskDto = checkExistById(id, user);
        boolean needCreateRecord = !Lists.of(TaskDto.STATUS_DELETE_FAILED, TaskDto.STATUS_RENEW_FAILED, TaskDto.STATUS_WAIT_START).contains(taskDto.getStatus());
        //boolean needCreateRecord = !TaskDto.STATUS_WAIT_START.equals(taskDto.getStatus());
        TaskEntity taskSnapshot = null;
        if (needCreateRecord) {
            taskSnapshot = new TaskEntity();
            BeanUtil.copyProperties(taskDto, taskSnapshot);
        }
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

        boolean noAgent = findAgent(taskDto, user);
        if (noAgent) {
            throw new BizException("Task.ResetAgentNotFound");
        }
        StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.RENEW, user);
        if (stateMachineResult.isOk()) {
            log.debug("check task status complete, task name = {}", taskDto.getName());
            taskResetLogService.clearLogByTaskId(id.toHexString());


            sendRenewMq(taskDto, user, DataSyncMq.OP_TYPE_RESET);

            if (needCreateRecord) {
                String lastTaskRecordId = new ObjectId().toHexString();
                Update update = Update.update(TaskDto.LASTTASKRECORDID, lastTaskRecordId);
                updateById(id.toHexString(), update, user);

                if (null != taskSnapshot){
                    taskSnapshot.setTaskRecordId(lastTaskRecordId);
                }
                disruptorService.sendMessage(DisruptorTopicEnum.CREATE_RECORD,
                        new TaskRecord(lastTaskRecordId, taskDto.getId().toHexString(), taskSnapshot, system ? "system" : user.getUserId(), new Date()));
            }
        } else {
            //如果状态机修改重置中失败，应该提醒用户重置操作重复了，或者任务当前状态被刷新了。
            log.info("Reset task, but the task status has been refreshed, task name = {}", taskDto.getName());
            throw new BizException("Task.ResetStatusInvalid");
        }
        //afterRenew(taskDto, user);
    }


    public void afterRenew(TaskDto taskDto, UserDetail user) {

        UpdateResult updateResult = renewNotSendMq(taskDto, user);

        if (updateResult.getMatchedCount() > 0) {

            //这个指标不应该清理，原因：性能太慢  查看历史记录的时候需要看到这些指标信息 新的任务已经用了新的runId 回规避页面上的显示问题 -- Berry
            //renewAgentMeasurement(taskDto.getId().toString());
            log.debug("renew task complete, task name = {}", taskDto.getName());

            //清除校验结果
            FunctionUtils.ignoreAnyError(() -> taskAutoInspectResultsService.cleanResultsByTask(taskDto));
            //由于清理的逻辑可能比较慢，导致任务已经是待启动状态，但是清理没有完成，任务重新启动会存在问题，所以这里需要先清理再改状态。 -- Berry
            StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.RENEW_DEL_SUCCESS, user);
            if (stateMachineResult.isFail()) {
                log.warn("Modify renew success failed, task name = {}", taskDto.getName());
            }
        }



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
            throw new BizException(TASK_NOT_FOUND, THE_COPIED_TASK_DOES_NOT_EXIST);
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
            throw new BizException(TASK_NOT_FOUND, THE_COPIED_TASK_DOES_NOT_EXIST);
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
            throw new BizException(TASK_NOT_FOUND, THE_COPIED_TASK_DOES_NOT_EXIST);
        }

        return taskDto;
    }


    public List<MutiResponseMessage> batchStart(List<ObjectId> taskIds, UserDetail user,
                                                HttpServletRequest request, HttpServletResponse response) {
        List<MutiResponseMessage> responseMessages = new ArrayList<>();
        List<TaskDto> taskDtos = findAllTasksByIds(taskIds.stream().map(ObjectId::toHexString).collect(Collectors.toList()));
        int index = 1;
        for (TaskDto task : taskDtos) {
            MutiResponseMessage mutiResponseMessage = new MutiResponseMessage();
            mutiResponseMessage.setId(task.getId().toHexString());
            try {
                if (settingsService.isCloud()) {
                    CalculationEngineVo calculationEngineVo = taskScheduleService.cloudTaskLimitNum(task, user, true);
                    int runningNum = subCronOrPlanNum(task, calculationEngineVo.getRunningNum());
                    if (runningNum >= calculationEngineVo.getTaskLimit() ||
                            index > calculationEngineVo.getTotalLimit()) {
                        throw new BizException("Task.ScheduleLimit");
                    }
                }
                start(task, user, "11");
            } catch (Exception e) {
                log.warn("start task exception, task id = {}, e = {}", task.getId(), ThrowableUtils.getStackTraceByPn(e));
                monitoringLogsService.startTaskErrorLog(task, user, e, Level.ERROR);
                if (e instanceof BizException) {
                    mutiResponseMessage.setCode(((BizException) e).getErrorCode());
                    mutiResponseMessage.setMessage(MessageUtil.getMessage(((BizException) e).getErrorCode(), ((BizException) e).getArgs()));
                } else {
                    try {
                        ResponseMessage<?> responseMessage = exceptionHandler.handlerException(e, request, response);
                        mutiResponseMessage.setCode(responseMessage.getCode());
                        mutiResponseMessage.setMessage(responseMessage.getMessage());
                    } catch (Throwable ex) {
                        log.warn(DELETE_TASK_HANDLE_EXCEPTION_ERROR_TASK_ID, task.getId().toHexString());
                    }
                }
            }
            index++;
            responseMessages.add(mutiResponseMessage);
        }
        return responseMessages;
    }

    public int subCronOrPlanNum(TaskDto task, int runningNum) {
        TaskDto taskScheduleFlag = findByTaskId(task.getId(), PLAN_START_DATE_FLAG, CRONTAB_EXPRESSION_FLAG);
        if (checkIsCronOrPlanTask(taskScheduleFlag) && runningNum > 0) {
            runningNum -= 1;
        }
        return runningNum;
    }

    public boolean checkIsCronOrPlanTask(TaskDto task) {
        if(null == task) throw new IllegalArgumentException("Task can not be null");
        if ((task.getCrontabExpressionFlag() != null && task.getCrontabExpressionFlag()) || task.isPlanStartDateFlag()) {
            return true;
        } else {
            return false;
        }
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
                    mutiResponseMessage.setMessage(MessageUtil.getMessage(((BizException) e).getErrorCode(), ((BizException) e).getArgs()));
                } else {
                    try {
                        ResponseMessage<?> responseMessage = exceptionHandler.handlerException(e, request, response);
                        mutiResponseMessage.setCode(responseMessage.getCode());
                        mutiResponseMessage.setMessage(responseMessage.getMessage());
                    } catch (Throwable ex) {
                        log.warn(DELETE_TASK_HANDLE_EXCEPTION_ERROR_TASK_ID,  taskId.toHexString());
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
                        mutiResponseMessage.setMessage(MessageUtil.getMessage(((BizException) e).getErrorCode(), ((BizException) e).getArgs()));
                    }
                } else {
                    try {
                        ResponseMessage<?> responseMessage = exceptionHandler.handlerException(e, request, response);
                        mutiResponseMessage.setCode(responseMessage.getCode());
                        mutiResponseMessage.setMessage(responseMessage.getMessage());
                    } catch (Throwable ex) {
                        log.warn(DELETE_TASK_HANDLE_EXCEPTION_ERROR_TASK_ID,  taskId.toHexString());
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
                    mutiResponseMessage.setMessage(MessageUtil.getMessage(((BizException) e).getErrorCode(), ((BizException) e).getArgs()));
                } else {
                    try {
                        ResponseMessage<?> responseMessage = exceptionHandler.handlerException(e, request, response);
                        mutiResponseMessage.setCode(responseMessage.getCode());
                        mutiResponseMessage.setMessage(responseMessage.getMessage());
                    } catch (Throwable ex) {
                        log.warn(DELETE_TASK_HANDLE_EXCEPTION_ERROR_TASK_ID,  taskId.toHexString());
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
            Page<TaskDto>  page = super.find(filter, userDetail);
            deleteNotifyEnumData(page.getItems());
            log.debug("page{}", JSON.toJSONString(page));
            return page;
        }
        Where where = filter.getWhere();
        if (where == null) {
            where = new Where();
            filter.setWhere(where);
        }
        if (where.get(STATUS) == null) {
            Document statusCondition = new Document();
            statusCondition.put("$nin", Lists.of(TaskDto.STATUS_DELETE_FAILED, TaskDto.STATUS_DELETING));
            where.put(STATUS, statusCondition);
        }
        //过滤掉挖掘任务
        String syncType = (String) where.get(SYNC_TYPE);
        if (StringUtils.isBlank(syncType)) {
            Document logCollectorFilter = new Document();
            logCollectorFilter.put("$nin", Lists.of(TaskDto.SYNC_TYPE_LOG_COLLECTOR, TaskDto.SYNC_TYPE_CONN_HEARTBEAT));
            where.put(SYNC_TYPE, logCollectorFilter);
        }

        //过滤调共享缓存任务
        HashMap<String, Object> notShareCache = new HashMap<>();
        notShareCache.put("$ne", true);
        where.put("shareCache", notShareCache);


        Boolean deleted = (Boolean) where.get(IS_DELETED);
        if (deleted == null) {
            Document document = new Document();
            document.put("$ne", true);
            where.put(IS_DELETED, document);
        }

        Page<TaskDto> taskDtoPage = new Page<>();
        List<TaskDto> items = new ArrayList<>();
        if (where.get(SYNC_TYPE) != null && (where.get(SYNC_TYPE) instanceof String)) {
            String synType = (String) where.get(SYNC_TYPE);
            if (SyncType.MIGRATE.getValue().equals(synType)) {
                taskDtoPage = findDataCopyList(filter, userDetail);
            } else if (SyncType.SYNC.getValue().equals(synType)) {
                taskDtoPage = findDataDevList(filter, userDetail);
            } else if (SyncType.CONN_HEARTBEAT.getValue().equals(synType)) {
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
                        double process;
                        if (total == 0){
                            process = 0;
                        }else {
                            process = finished / (total * 1d);
                        }
                        if (process > 1) {
                            process = 1;
                        }
                        item.setTransformProcess(((int) (process * 100)) / 100d);
                    }
                    //产品认为不把STATUS_SCHEDULE_FAILED  展现到页面上，STATUS_SCHEDULE_FAILED就直接转为error状态
                    item.setStatus(TaskStatusEnum.getMapStatus(item.getStatus()));

                    if (StringUtils.isNotBlank(item.getCrontabScheduleMsg())) {
                        item.setCrontabScheduleMsg(MessageUtil.getMessage(item.getCrontabScheduleMsg()));
                    }
                }
            } else {
                for (TaskDto item : items) {
                    item.setTransformProcess(0);
                    item.setTransformStatus(MetadataTransformerDto.StatusEnum.running.name());

                    if (StringUtils.isNotBlank(item.getCrontabScheduleMsg())) {
                        item.setCrontabScheduleMsg(MessageUtil.getMessage(item.getCrontabScheduleMsg()));
                    }
                }
            }

            // Internationalized Shared Mining Warning Messages
            for (TaskDto item : items) {
                if (StringUtils.isNotBlank(item.getShareCdcStopMessage())) {
                    item.setShareCdcStopMessage(MessageUtil.getMessage(item.getShareCdcStopMessage()));
                }
            }

        }

        return taskDtoPage;
    }

    public Page<TaskDto> superFind(Filter filter, UserDetail userDetail) {
        return super.find(filter, userDetail);
    }


    public void deleteNotifyEnumData(List<TaskDto> taskDtoList) {
//        log.info("deleteNotifyEnumData");
        if (CollectionUtils.isEmpty(taskDtoList)) {
            return;
        }
        for (TaskDto taskDto : taskDtoList) {
            List<AlarmSettingVO> alarmSettings = taskDto.getAlarmSettings();
            if (CollectionUtils.isNotEmpty(alarmSettings)) {
                for (AlarmSettingVO alarmSettingDto : alarmSettings) {
//                    log.info("alarmSettingDto{}", JSONObject.toJSONString(alarmSettingDto));
                    alarmSettingDto.getNotify().remove(NotifyEnum.SMS);
                    alarmSettingDto.getNotify().remove(NotifyEnum.WECHAT);
//                    log.info("alarmSettingDto after{}", JSONObject.toJSONString(alarmSettingDto));

                }
            }
            if (taskDto.getDag().getNodes() != null) {
                for (Node node : taskDto.getDag().getNodes()) {
                    if (CollectionUtils.isNotEmpty(node.getAlarmSettings())) {
                        List<AlarmSettingVO> alarmSetting = node.getAlarmSettings();
                        for (AlarmSettingVO alarmSettingVO : alarmSetting) {
//                            log.info("alarmSettingDto Node{}", JSONObject.toJSONString(alarmSettingVO));
                            if (CollectionUtils.isNotEmpty(alarmSettingVO.getNotify())) {
                                alarmSettingVO.getNotify().remove(NotifyEnum.SMS);
                                alarmSettingVO.getNotify().remove(NotifyEnum.WECHAT);
//                                log.info("alarmSettingDto  Node after{}", JSONObject.toJSONString(alarmSettingVO));

                            }
                        }
                    }
                }
            }
        }
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

        Criteria criteria = new Criteria();
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
			if (!userDetail.isRoot() && !DataPermissionHelper.setFilterConditions(true, query, userDetail)) {
					criteria.and(USER_ID).is(userDetail.getUserId());
			}

        TmPageable tmPageable = new TmPageable();
        Integer page = (filter.getSkip() / filter.getLimit()) + 1;
        tmPageable.setPage(page);
        tmPageable.setSize(filter.getLimit());

        String order = filter.getOrder() == null ? "createTime DESC" : String.valueOf(filter.getOrder());
        String sortKey = order.contains(CURRENT_EVENT_TIMESTAMP) ? CURRENT_EVENT_TIMESTAMP : CREATE_TIME;
        if (order.contains("ASC")) {
            tmPageable.setSort(Sort.by(sortKey).ascending());
        } else {
            tmPageable.setSort(Sort.by(sortKey).descending());
        }

        long total = repository.getMongoOperations().count(query, TaskEntity.class);
        List<TaskEntity> taskEntityList = repository.getMongoOperations().find(query.with(tmPageable), TaskEntity.class);
        List<TaskDto> taskDtoList = com.tapdata.tm.utils.BeanUtil.deepCloneList(taskEntityList, TaskDto.class, DataPermissionHelper::convert);

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
        query = repository.filterToQuery(filter);
        query.skip(filter.getSkip());
        query.limit(filter.getLimit());
        List<TaskEntity> taskEntityList = repository.findAll(query, userDetail);
        List<TaskDto> taskDtoList = com.tapdata.tm.utils.BeanUtil.deepCloneList(taskEntityList, TaskDto.class, DataPermissionHelper::convert);
        Page<TaskDto> taskDtoPage = new Page<>();
        taskDtoPage.setTotal(count);
        taskDtoPage.setItems(taskDtoList);
        return taskDtoPage;
    }

    protected Node getSourceNode(TaskDto taskDto) {
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

    protected Node getTargetNode(TaskDto taskDto) {
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
        Criteria criteria = Criteria.where(IS_DELETED).is(false).and(DAG_NODES).elemMatch(Criteria.where("type").is("logCollector")
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
        taskDto.setSyncType(TaskDto.SYNC_TYPE_MEM_CACHE);

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
        if (null != where.get(CONNECTION_NAME)) {
            Map connectionName = (Map) where.remove(CONNECTION_NAME);
            String conectionNameStr = (String) connectionName.remove(REGEX);
            connectionIds = dataSourceService.findIdByName(conectionNameStr);
            Map<String, Object> connectioIdMap = new HashMap<>();
            connectioIdMap.put("$in", connectionIds);
            where.put(DAG_NODES_CONNECTION_ID, connectioIdMap);
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
                        shareCacheVo.setFields((List<String>) sourceNode.getAttrs().get(FIELDS));
                    }

                    if (taskDto.getCurrentEventTimestamp() != null) {
                        shareCacheVo.setCacheTimeAt(new Date(taskDto.getCurrentEventTimestamp()));
                    }
                }

                CacheNode cacheNode = (CacheNode) getTargetNode(taskDto);
                if (null != cacheNode) {
                    BeanUtil.copyProperties(cacheNode, shareCacheVo);
                    String externalStorageId = cacheNode.getExternalStorageId();
                    if (StringUtils.isNotEmpty(externalStorageId)) {
                        ExternalStorageDto externalStorageDto = externalStorageService.findById(MongoUtils.toObjectId(externalStorageId));
                        if (externalStorageDto != null) {
                            shareCacheVo.setExternalStorageName(externalStorageDto.getName());
                        }
                    }
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
        shareCacheDetailVo.setId(id);
        shareCacheDetailVo.setName(taskDto.getName());
        shareCacheDetailVo.setStatus(taskDto.getStatus());
        if (null == sourceNode || null == targetNode){
            throw new BizException(ILLEGAL_ARGUMENT,"sourceNode");
        }
        String connectionId = ((DataNode) sourceNode).getConnectionId();
        DataSourceConnectionDto connectionDto = dataSourceService.findOne(Query.query(Criteria.where("id").is(connectionId)));
        if (null != connectionDto) {
            shareCacheDetailVo.setConnectionId(connectionDto.getId().toString());
            shareCacheDetailVo.setConnectionName(connectionDto.getName());
        }
        shareCacheDetailVo.setTableName(((TableNode) sourceNode).getTableName());
        shareCacheDetailVo.setCacheKeys(targetNode.getCacheKeys());
        shareCacheDetailVo.setAutoCreateIndex(targetNode.getAutoCreateIndex());
        shareCacheDetailVo.setCreateTime(taskDto.getCreateAt());
        shareCacheDetailVo.setCreateUser(taskDto.getCreateUser());
        if (null != sourceNode.getAttrs()) {
            shareCacheDetailVo.setFields((List<String>) sourceNode.getAttrs().get(FIELDS));
        }

        if (taskDto.getCurrentEventTimestamp() != null) {
            shareCacheDetailVo.setCacheTimeAt(new Date(taskDto.getCurrentEventTimestamp()));
        }
        shareCacheDetailVo.setMaxRows(targetNode.getMaxRows());
        shareCacheDetailVo.setMaxMemory(targetNode.getMaxMemory());
        shareCacheDetailVo.setTtl(TimeUtil.parseSecondsToDay(targetNode.getTtl()));
        shareCacheDetailVo.setExternalStorageId(targetNode.getExternalStorageId());

        return shareCacheDetailVo;
    }

    private TaskDto parseCacheToTaskDto(SaveShareCacheParam saveShareCacheParam, TaskDto taskDto) {
        taskDto.setStatus(TaskDto.STATUS_EDIT);
        taskDto.setType(ParentTaskDto.TYPE_CDC);
        taskDto.setShareCache(true);
        taskDto.setLastUpdAt(new Date());
        taskDto.setName(saveShareCacheParam.getName());
        taskDto.setShareCdcEnable(false);

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
            tableNode.setTableName((String) sourceNodeMap.get(TABLE_NAME));
            tableNode.setType(TABLE);
            tableNode.setDatabaseType((String) sourceNodeMap.get("databaseType"));
            tableNode.setConnectionId((String) sourceNodeMap.get(CONNECTION_ID));

            Field field = new Field();
            field.put("name", true);
            String connectionName = Optional.ofNullable(tableNode.getConnectionId())
                    .map(ObjectId::new)
                    .map(connId -> {
                        return dataSourceService.findById(connId, field);
                    }).map(DataSourceConnectionDto::getName).orElse(null);
            if (null == connectionName) {
                throw new BizException("Datasource.NotFound");
            }
            tableNode.setName(connectionName + "-" + tableNode.getTableName());

            Map<String, Object> attrs = new HashMap();
            if (null != sourceNodeMap.get(ATTRS)) {
                attrs = (Map<String, Object>) sourceNodeMap.get(ATTRS);
                tableNode.setAttrs(attrs);
            }

            Map targetNodeMap = nodeList.get(1);
            CacheNode cacheNode = new CacheNode();
            cacheNode.setCacheKeys((String) targetNodeMap.get("cacheKeys"));
            cacheNode.setNeedCreateIndex((List<String>) targetNodeMap.get("needCreateIndex"));
            cacheNode.setAutoCreateIndex((Boolean) targetNodeMap.get("autoCreateIndex"));
            Integer maxRows = MapUtil.getInt(targetNodeMap, "maxRows");
            Integer maxMemory = MapUtil.getInt(targetNodeMap, "maxMemory");
            cacheNode.setMaxRows(maxRows == null ? Integer.MAX_VALUE : maxRows.longValue());
            cacheNode.setMaxMemory(maxMemory == null ? 500 : maxMemory.intValue());
            Integer ttl = MapUtil.getInt(targetNodeMap, "ttl");
            cacheNode.setTtl(ttl == null ? Integer.MAX_VALUE : TimeUtil.parseDayToSeconds(ttl));
            String externalStorageId = MapUtil.getStr(targetNodeMap, "externalStorageId");
            cacheNode.setExternalStorageId(externalStorageId);

            edge.setSource(sourceId);
            edge.setTarget(targetId);
            edges.add(edge);
            tableNode.setId(sourceId);
            cacheNode.setId(targetId);
            cacheNode.setFields((List<String>) attrs.get(FIELDS));
            cacheNode.setCacheName(saveShareCacheParam.getName());
            cacheNode.setName(cacheNode.getCacheName());

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
        Criteria criteria = new Criteria()
                .and(IS_DELETED).ne(true)
                .and(SYNC_TYPE).in(TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC)
                .and(STATUS).nin(TaskDto.STATUS_DELETING, TaskDto.STATUS_DELETE_FAILED)
                //共享缓存的任务设计的有点问题
                .and("shareCache").ne(true);


        Query query = Query.query(criteria);
        query.fields().include(SYNC_TYPE, STATUS, "statuses");
        //把任务都查询出来
        List<TaskDto> taskDtoList = DataPermissionMenuEnums.MigrateTack.checkAndSetFilter(
                user, DataPermissionActionEnums.View, () -> findAllDto(query, user)
        );
        Map<String, List<TaskDto>> syncTypeToTaskList = taskDtoList.stream().collect(Collectors.groupingBy(TaskDto::getSyncType));

        List<TaskDto> migrateList =  syncTypeToTaskList.getOrDefault(SyncType.MIGRATE.getValue(), Collections.emptyList());
        resultChart.put("chart1", getDataCopyChart(migrateList));
//        resultChart.put("chart2", dataCopy);
        List<TaskDto> synList = syncTypeToTaskList.getOrDefault(SyncType.SYNC.getValue(), Collections.emptyList());
        resultChart.put("chart3", getDataDevChart(synList));
//        resultChart.put("chart4", dataDev);
        resultChart.put("chart5", inspectChart(user));
        Chart6Vo chart6Vo = ChartSchedule.cache.get(user.getUserId());
        if (chart6Vo == null) {
            chart6Vo = chart6(user);
            ChartSchedule.put(user.getUserId(), chart6Vo);
        }
        resultChart.put("chart6", chart6Vo);
        return resultChart;
    }


    public Map<String, Integer> inspectChart(UserDetail user) {
        Criteria criteria = Criteria.where(SYNC_TYPE).is(TaskDto.SYNC_TYPE_MIGRATE)
                .and("isAutoInspect").is(true)
                .and(IS_DELETED).ne(true);
        Query query = new Query(criteria);
        query.fields().include("_id", STATUS, "isAutoInspect", "canOpenInspect", "attrs.autoInspectProgress.tableCounts", "attrs.autoInspectProgress.tableIgnore", "attrs.autoInspectProgress.step");

        int openTaskNum = 0;
        int canTaskNum = 0;
        int errorTaskNum = 0;
        int diffTaskNum = 0;
        List<TaskDto> taskDtos = findAllDto(query, user);

        Set<String> taskSet = taskAutoInspectResultsService.groupByTask(user);
        if (CollectionUtils.isNotEmpty(taskDtos)) {
            openTaskNum = taskDtos.size();
            for (TaskDto taskDto : taskDtos) {
                if (taskDto.getCanOpenInspect() != null && taskDto.getCanOpenInspect()) {
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
        chart5.put(TOTAL, openTaskNum);
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


        Map<String, List<TaskDto>> statusToDataCopyTaskMap = migrateList.stream().filter(t -> t.getStatus() != null).collect(Collectors.groupingBy(TaskDto::getStatus));
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
            singleMap.put(COUNT, statusToDataCopyTaskMap.getOrDefault(taskStatus, Collections.emptyList()).size());
            dataCopyPreviewItems.add(singleMap);
        }
        dataCopyPreview.put(TOTAL, migrateList.size());
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
            singleMap.put(COUNT, statusToCount.getOrDefault(taskStatus, 0L));
            dataCopyPreviewItems.add(singleMap);
        }
        dataCopyPreview.put(TOTAL, synList.size());
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
            if (INITIAL_SYNC.equals(type)) {
                //设置全量开始时间
                Date initStartTime = getMillstoneTime(taskDto, "READ_SNAPSHOT", INITIAL_SYNC);
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

            } else if (INITIAL_SYNC_CDC.equals(type)) {
                //全量开始时间
                Date initStartTime = getMillstoneTime(taskDto, "READ_SNAPSHOT", INITIAL_SYNC);
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
        TaskDto taskDto = checkExistById(MongoUtils.toObjectId(taskId), user, STATUS);
        return TaskDto.STATUS_EDIT.equals(taskDto.getStatus()) || TaskDto.STATUS_WAIT_START.equals(taskDto.getStatus());
    }

    public TransformerWsMessageDto findTransformParam(String taskId, UserDetail user) {
        TaskDto taskDto = checkExistById(MongoUtils.toObjectId(taskId), user);
        if (isAgentReq()) {
            List<TaskDto> list = new ArrayList<>();
            list.add(taskDto);
            deleteNotifyEnumData(list);

        }
        return transformSchemaService.getTransformParam(taskDto, user);
    }

    public TransformerWsMessageDto findTransformAllParam(String taskId, UserDetail user) {
        TaskDto taskDto = checkExistById(MongoUtils.toObjectId(taskId), user);
        if (isAgentReq()) {
            List<TaskDto> list = new ArrayList<>();
            list.add(taskDto);
            deleteNotifyEnumData(list);

        }
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
                        match(Criteria.where(USER_ID).is(userDetail.getUserId())
                                .and("customId").is(userDetail.getCustomerId())
                                .and(IS_DELETED).ne(true)
                                .and(STATUS).nin(TaskDto.STATUS_DELETING, TaskDto.STATUS_DELETE_FAILED)
                                .and(SYNC_TYPE).in(TaskDto.SYNC_TYPE_MIGRATE,TaskDto.SYNC_TYPE_SYNC)),
                        group("type").count().as(COUNT)
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
        taskTypeStats.put(TOTAL, total);
        return taskTypeStats;
    }

    private DataFlowInsightStatisticsDto mergerStatistics(List<LocalDate> localDates, DataFlowInsightStatisticsDto oldStatistics, DataFlowInsightStatisticsDto newStatistics) {
        Map<String, DataFlowInsightStatisticsDto.DataStatisticInfo> oldMap = new HashMap<>();
        if (oldStatistics != null && CollectionUtils.isNotEmpty(oldStatistics.getInputDataStatistics())) {
            oldMap = oldStatistics.getInputDataStatistics().stream().collect(Collectors.toMap(DataFlowInsightStatisticsDto.DataStatisticInfo::getTime, v -> v, (k1, k2) -> k2));
        }

        Map<String, DataFlowInsightStatisticsDto.DataStatisticInfo> newMap = new HashMap<>();
        if (newStatistics != null && CollectionUtils.isNotEmpty(newStatistics.getInputDataStatistics())) {
            newMap = newStatistics.getInputDataStatistics().stream().collect(Collectors.toMap(DataFlowInsightStatisticsDto.DataStatisticInfo::getTime, v -> v, (k1, k2) -> k2));
        }

        List<DataFlowInsightStatisticsDto.DataStatisticInfo> inputDataStatistics = new ArrayList<>();

        final DateTimeFormatter format = DateTimeFormatter.ofPattern(DATETIME_PATTERN);
        for (LocalDate localDate : localDates) {
            String time = localDate.format(format);
            if (newMap.get(time) != null) {
                inputDataStatistics.add(newMap.get(time));
            } else if (oldMap.get(time) != null) {
                inputDataStatistics.add(oldMap.get(time));
            }
        }

        inputDataStatistics.sort(Comparator.comparing(DataFlowInsightStatisticsDto.DataStatisticInfo::getTime));
        DataFlowInsightStatisticsDto dataFlowInsightStatisticsDto = new DataFlowInsightStatisticsDto();
        dataFlowInsightStatisticsDto.setInputDataStatistics(inputDataStatistics);
        BigInteger count = inputDataStatistics.stream().map(DataFlowInsightStatisticsDto.DataStatisticInfo::getCount).reduce(BigInteger.ZERO, BigInteger::add);
        dataFlowInsightStatisticsDto.setTotalInputDataCount(count);
        dataFlowInsightStatisticsDto.setGranularity("month");
        return dataFlowInsightStatisticsDto;
    }

    private List<LocalDate> getNewLocalDate(List<LocalDate> localDates, DataFlowInsightStatisticsDto oldStatistics) {
        List<DataFlowInsightStatisticsDto.DataStatisticInfo> inputDataStatistics = oldStatistics.getInputDataStatistics();
        if (CollectionUtils.isEmpty(inputDataStatistics)) {
            return localDates;
        }
        final DateTimeFormatter format = DateTimeFormatter.ofPattern(DATETIME_PATTERN);
        List<LocalDate> newLocalDates = new ArrayList<>();
        inputDataStatistics.remove(inputDataStatistics.size()-1);
        List<LocalDate> oldLocalDate = inputDataStatistics.stream().map(s -> LocalDate.parse(s.getTime(), format)).collect(Collectors.toList());
        for (LocalDate localDate : localDates) {
            if (!oldLocalDate.contains(localDate)) {
                newLocalDates.add(localDate);
            }
        }

        return newLocalDates;
    }

    public DataFlowInsightStatisticsDto statsTransport(UserDetail userDetail) {
        List<LocalDate> localDates = new ArrayList<>();
        LocalDate now = LocalDate.now();
        LocalDate lastMonthDay = now.minusMonths(1);
        while (!now.equals(lastMonthDay)) {
            localDates.add(now);
            now = now.minusDays(1);
        }

        DataFlowInsightStatisticsDto oldStatistics = InputNumCache.USER_INPUT_NUM_CACHE.get(userDetail.getUserId());
        if (oldStatistics == null) {
            DataFlowInsightStatisticsDto dataFlowInsightStatisticsDto = statsTransport(userDetail, localDates);
            InputNumCache.USER_INPUT_NUM_CACHE.put(userDetail.getUserId(), dataFlowInsightStatisticsDto);
            return dataFlowInsightStatisticsDto;
        }
        List<LocalDate> newLocalDates = getNewLocalDate(localDates, oldStatistics);
        DataFlowInsightStatisticsDto newStatistics = statsTransport(userDetail, newLocalDates);
        DataFlowInsightStatisticsDto dataFlowInsightStatisticsDto = mergerStatistics(localDates, oldStatistics, newStatistics);
        InputNumCache.USER_INPUT_NUM_CACHE.put(userDetail.getUserId(), dataFlowInsightStatisticsDto);
        return dataFlowInsightStatisticsDto;
    }



    public DataFlowInsightStatisticsDto statsTransport(UserDetail userDetail, List<LocalDate> localDates) {

        Criteria criteria = Criteria.where(IS_DELETED).ne(true);
        Query query = new Query(criteria);
        query.fields().include("_id");
        List<TaskDto> allDto = findAllDto(query, userDetail);
        List<String> ids = allDto.stream().map(a->a.getId().toHexString()).collect(Collectors.toList());

        Map<LocalDate, BigInteger> allInputNumMap = new HashMap<>();
        for (LocalDate date : localDates) {
            allInputNumMap.put(date, BigInteger.ZERO);
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
        DateTimeFormatter format = DateTimeFormatter.ofPattern(DATETIME_PATTERN);
        List<MeasurementEntity> measurementEntities = measurementServiceV2.find(query1);

        Map<String, List<MeasurementEntity>> taskMap = measurementEntities.stream().collect(Collectors.groupingBy(m -> m.getTags().get(TASK_ID)));




        taskMap.forEach((k1, v1) -> {
            Map<LocalDate, BigInteger> inputNumMap = new HashMap<>();
            Map<LocalDate, List<Sample>> sampleMap = v1.stream().flatMap(m -> m.getSamples().stream()).collect(Collectors.groupingBy(s -> {
                Date date = s.getDate();
                return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
            }));
            List<LocalDate> collect = sampleMap.keySet().stream().sorted().collect(Collectors.toList());
            for (LocalDate k : collect) {
                List<Sample> v = sampleMap.get(k);
                BigInteger value = BigInteger.ZERO;
                Optional<Sample> max = v.stream().max(Comparator.comparing(Sample::getDate));
                if (max.isPresent()) {
                    Sample sample = max.get();
                    Map<String, Number> vs = sample.getVs();
                    value = value.add(NumberUtil.parseDataTotal(vs.get("inputInsertTotal")));
                    value = value.add(NumberUtil.parseDataTotal(vs.get("inputOthersTotal")));
                    value = value.add(NumberUtil.parseDataTotal(vs.get("inputDdlTotal")));
                    value = value.add(NumberUtil.parseDataTotal(vs.get("inputUpdateTotal")));
                    value = value.add(NumberUtil.parseDataTotal(vs.get("inputDeleteTotal")));
                }
                LocalDate localDate = k.minusDays(1L);
                BigInteger lastNum = inputNumMap.get(localDate);
                if (lastNum != null) {
                    value = value.subtract(lastNum);
                }
                inputNumMap.put(k, value);
            }

            inputNumMap.forEach((k2, v2) -> {
                BigInteger allNum = allInputNumMap.get(k2);
                if (allNum != null) {
                    v2 = v2.add(allNum);
                }

                allInputNumMap.put(k2, v2);

            });

        });

        List<DataFlowInsightStatisticsDto.DataStatisticInfo> inputDataStatistics = new ArrayList<>();
        AtomicReference<BigInteger> totalInputDataCount = new AtomicReference<>(BigInteger.ZERO);
        allInputNumMap.forEach((k, v) -> {
            inputDataStatistics.add(new DataFlowInsightStatisticsDto.DataStatisticInfo(k.format(format), v));
            totalInputDataCount.set(totalInputDataCount.get().add(v));
        });

        DataFlowInsightStatisticsDto dataFlowInsightStatisticsDto = new DataFlowInsightStatisticsDto();
        inputDataStatistics.sort(Comparator.comparing(DataFlowInsightStatisticsDto.DataStatisticInfo::getTime));
        dataFlowInsightStatisticsDto.setInputDataStatistics(inputDataStatistics);
        dataFlowInsightStatisticsDto.setTotalInputDataCount(totalInputDataCount.get());
        dataFlowInsightStatisticsDto.setGranularity("month");
        return dataFlowInsightStatisticsDto;
    }

    /**
     * 根据连接目标节点的连接id获取任务
     * @param connectionIds
     * @param user
     */
    public Map<String, List<TaskDto>> getByConIdOfTargetNode(List<String> connectionIds, String status, String position, UserDetail user, int page, int pageSize) {


        Map<String, List<TaskDto>> taskMap = new HashMap<>();
        for (String connectionId : connectionIds) {
            Criteria criteria = Criteria.where(DAG_NODES_CONNECTION_ID).is(connectionId)
                    .and(IS_DELETED).ne(true);
            if (StringUtils.isNotBlank(status)) {
                criteria.and(STATUS).is(status);
            } else {
                criteria.and(STATUS).nin(Lists.of(TaskDto.STATUS_DELETING, TaskDto.STATUS_DELETE_FAILED));
            }
            Query query = new Query(criteria);
            query.fields().include("dag");
            List<TaskDto> allTasks = findAllDto(query, user);

            List<ObjectId> containsTaskIds = new ArrayList<>();

            be:
            for (TaskDto task : allTasks) {
                DAG dag = task.getDag();
                if (position.equals(SOURCE)) {
                    List<Node> sources = dag.getSources();
                    if (CollectionUtils.isNotEmpty(sources)) {
                        for (Node source : sources) {
                            if (source instanceof DataParentNode) {
                                String connectionId1 = ((DataParentNode<?>) source).getConnectionId();
                                if (connectionId.equals(connectionId1)) {
                                    containsTaskIds.add(task.getId());
                                    continue be;
                                }
                            }
                        }
                    }
                } else {
                    List<Node> targets = dag.getTargets();
                    if (CollectionUtils.isNotEmpty(targets)) {
                        for (Node target : targets) {
                            if (target instanceof DataParentNode) {
                                String connectionId1 = ((DataParentNode<?>) target).getConnectionId();
                                if (connectionId.equals(connectionId1)) {
                                    containsTaskIds.add(task.getId());
                                    continue be;
                                }
                            }
                        }
                    }
                }
            }

            Criteria criteria1 = Criteria.where("_id").in(containsTaskIds);
            Query query1 = new Query(criteria1);
            query1.with(Sort.by(CREATE_TIME).descending());
            List<TaskDto> taskDtos = findAllDto(new Query(criteria1), user);
            taskMap.put(connectionId, taskDtos);
        }
        return taskMap;


    }

    public long countTaskNumber(UserDetail user) {
        return count(new Query(Criteria.where(IS_DELETED).is(false).and(STATUS).nin(TaskDto.STATUS_DELETE_FAILED, TaskDto.STATUS_DELETING)), user);
    }

    public List<SampleTaskVo> findByConId(String sourceConnectionId, String targetConnectionId, String syncType, String status, Where where, UserDetail user) {

        Criteria criteria = repository.whereToCriteria(where);
        criteria.and(IS_DELETED).ne(true);
        List<String> conIds = new ArrayList<>();
        if (StringUtils.isNotBlank(sourceConnectionId)) {
            conIds.add(sourceConnectionId);
        }

        if (StringUtils.isNotBlank(targetConnectionId)) {
            conIds.add(targetConnectionId);
        }

        if (CollectionUtils.isNotEmpty(conIds)) {
            criteria.and(DAG_NODES_CONNECTION_ID).in(conIds);
        }

        if (StringUtils.isNotBlank(syncType)) {
            criteria.and(SYNC_TYPE).is(syncType);
        } else {
            criteria.and(SYNC_TYPE).in(TaskDto.SYNC_TYPE_MIGRATE, TaskDto.SYNC_TYPE_SYNC);
        }

        if (StringUtils.isNotBlank(status)) {
            criteria.and(STATUS).is(status);
        } else {
            criteria.and(STATUS).nin(TaskDto.STATUS_DELETING, TaskDto.STATUS_DELETE_FAILED);
        }

        Query query = new Query(criteria);
        List<TaskDto> tasks = findAllDto(query, user);

        List<SampleTaskVo> sampleTaskVos = tasks.stream().map(
                t -> {
                    List<String> sourceIds = new ArrayList<>();
                    List<Node> sources = t.getDag().getSources();
                    if (CollectionUtils.isNotEmpty(sources)) {
                        for (Node source : sources) {
                            if (source instanceof DataParentNode ) {
                                sourceIds.add(((DataParentNode<?>) source).getConnectionId());
                            }
                        }
                    }

                    List<String> tgtIds = new ArrayList<>();
                    List<Node> targets = t.getDag().getTargets();
                    if (CollectionUtils.isNotEmpty(targets)) {
                        for (Node tgt : targets) {
                            if (tgt instanceof DataParentNode ) {
                                tgtIds.add(((DataParentNode<?>) tgt).getConnectionId());
                            }
                        }
                    }

                    SampleTaskVo sampleTaskVo = null;
                    if (StringUtils.isNotBlank(sourceConnectionId)) {
                        if (sourceIds.contains(sourceConnectionId)) {
                            sampleTaskVo = new SampleTaskVo();
                        }
                    } else {
                        sampleTaskVo = new SampleTaskVo();
                    }

                    if (StringUtils.isNotBlank(targetConnectionId)) {
                        if (tgtIds.contains(targetConnectionId)) {
                            sampleTaskVo = new SampleTaskVo();
                        }
                    } else {
                        sampleTaskVo = new SampleTaskVo();
                    }



                    if (sampleTaskVo != null) {
                        sampleTaskVo.setId(t.getId().toHexString());
                        sampleTaskVo.setName(t.getName());
                        sampleTaskVo.setCreateTime(t.getCreateAt());
                        sampleTaskVo.setLastUpdated(t.getLastUpdAt());
                        sampleTaskVo.setStatus(t.getStatus());
                        sampleTaskVo.setSyncType(t.getSyncType());
                        sampleTaskVo.setSourceConnectionIds(sourceIds);
                        sampleTaskVo.setTargetConnectionId(tgtIds);
                        sampleTaskVo.setCurrentEventTimestamp(t.getCurrentEventTimestamp());
                        sampleTaskVo.setCreateUser(t.getCreateUser());
                        sampleTaskVo.setStartTime(t.getStartTime());

                        DAG dag = t.getDag();
                        Date currentEventTimestamp = new Date();
                        Long delay = 0L;
                        if (dag != null) {
                            LinkedList<Edge> edges = dag.getEdges();
                            for (Edge edge : edges) {
                                Date eventTime = LogCollectorService.getAttrsValues(edge.getSource(), edge.getTarget(), "eventTime", t.getAttrs());
                                Date sourceTime = LogCollectorService.getAttrsValues(edge.getSource(), edge.getTarget(), "sourceTime", t.getAttrs());
                                if (null != eventTime && null != sourceTime) {
                                    long delayTime = sourceTime.getTime() - eventTime.getTime();
                                    delayTime = delayTime > 0 ? delayTime : 0;
                                    if (delayTime > delay) {
                                        delay = delayTime;
                                    }
                                }

                                if (eventTime != null) {
                                    if (eventTime.getTime() < currentEventTimestamp.getTime()) {
                                        currentEventTimestamp = eventTime;
                                    }
                                }

                            }
                        }

                        sampleTaskVo.setDelayTime(delay);
                        if (sampleTaskVo.getCurrentEventTimestamp() == null) {
                            sampleTaskVo.setCurrentEventTimestamp(currentEventTimestamp.getTime());
                        }
                    }
                    return sampleTaskVo;
                }
        ).filter(Objects::nonNull).collect(Collectors.toList());

        return sampleTaskVos;
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
                agentGroupService.uploadAgentInfo(taskDto, user);

                taskDto.setStatus(TaskDto.STATUS_EDIT);
                taskDto.setStatuses(new ArrayList<>());
							taskDto.setAttrs(new HashMap<>()); // 导出任务不保留运行时信息
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
                                    jsonList.add(new TaskUpAndLoadDto(METADATA_INSTANCES, JsonUtil.toJsonUseJackson(metadataInstancesDto)));
                                }
                            }

                            if (node instanceof DataParentNode) {
                                String connectionId = ((DataParentNode<?>) node).getConnectionId();
                                DataSourceConnectionDto dataSourceConnectionDto = dataSourceService.findById(MongoUtils.toObjectId(connectionId), user);
                                dataSourceConnectionDto.setCreateUser(null);
                                dataSourceConnectionDto.setCustomId(null);
                                dataSourceConnectionDto.setLastUpdBy(null);
                                dataSourceConnectionDto.setUserId(null);
                                dataSourceConnectionDto.setListtags(null);
                                DataSourceDefinitionDto byPdkHash = dataSourceDefinitionService.findByPdkHash(dataSourceConnectionDto.getPdkHash(), Integer.MAX_VALUE, user);
                                dataSourceConnectionDto.setDefinitionPdkAPIVersion(byPdkHash.getPdkAPIVersion());
                                String databaseQualifiedName = MetaDataBuilderUtils.generateQualifiedName("database", dataSourceConnectionDto, null);
                                MetadataInstancesDto dataSourceMetadataInstance = metadataInstancesService.findOne(
                                        Query.query(Criteria.where(QUALIFIED_NAME).is(databaseQualifiedName).and(IS_DELETED).ne(true)), user);
                                jsonList.add(new TaskUpAndLoadDto(METADATA_INSTANCES, JsonUtil.toJsonUseJackson(dataSourceMetadataInstance)));
                                jsonList.add(new TaskUpAndLoadDto("Connections", JsonUtil.toJsonUseJackson(dataSourceConnectionDto)));
                            }

                            if (node instanceof CustomProcessorNode) {
                                String customNodeId = ((CustomProcessorNode) node).getCustomNodeId();
                                CustomNodeDto customNodeDto = customNodeService.findById(MongoUtils.toObjectId(customNodeId), user);
                                jsonList.add(new TaskUpAndLoadDto("CustomNodeTemps", JsonUtil.toJsonUseJackson(customNodeDto)));
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
        String yyyymmdd = DateUtil.today().replace("-", "");
        FunctionUtils.isTureOrFalse(taskIds.size() > 1).trueOrFalseHandle(
                () -> fileName.set("task_batch" + "-" + yyyymmdd),
                () -> fileName.set(taskDtoMap.get(taskIds.get(0)).getName() + "-" + yyyymmdd)
        );
        fileService1.viewImg1(json, response, fileName.get() + ".json.gz");
    }

    private static Map<String, Object> getTableSchema(Map<String, Object> full, String table) {
        List<String> tableNames = Arrays.asList(table.split("\\."));
        Map<String, Object> databasesLayer = (Map<String, Object>) full.get("databases");
        Map<String, Object> currentDatabaseSchema = (Map<String, Object>) databasesLayer.get(tableNames.get(0));
        Map<String, Object> schemasLayer = (Map<String, Object>) currentDatabaseSchema.get("schemas");
        Map<String, Object> currentSchema = (Map<String, Object>) schemasLayer.get(tableNames.get(1));
        Map<String, Object> tables = (Map<String, Object>) currentSchema.get("tables");
        return tables;
    }

    private void genProperties(Map<String, Object> parent, Map<String, Object> contentMapping, Map<String, Object> relationshipsMapping, Map<String, Object> full, Map<String, String> sourceToJS, Map<String, Map<String, Map<String, Object>>> renameFields, Map<String, List<Map<String, Object>>> contentDeleteOperations, Map<String, List<Map<String, Object>>> contentRenameOperations) {
        String parentId = (String)parent.get(RM_ID_KEY);
        List<String> children = (List<String>) ((Map<String, Object>) relationshipsMapping.get(parentId)).get(CHILDREN);
        if (children == null || children.size() == 0) {
            return;
        }
        List<Map<String, Object>> childrenNode = new ArrayList<>();
        for (String child : children) {
            Map<String, Object> childNode = new HashMap<>();
            Map<String, Object> map = (Map<String, Object>) contentMapping.get(child);
            Map<String, String> setting = (Map<String, String>) map.get("settings");

            Map<String, Object> tables = getTableSchema(full, (String) ((Map<String, Object>) contentMapping.get(child)).get(TABLE));

            String tpTable = ((String) ((Map<String, Object>) contentMapping.get(child)).get(TABLE)).split("\\.")[((String) ((Map<String, Object>) contentMapping.get(child)).get(TABLE)).split("\\.").length - 1];
            List<Map<String, String>> joinKeys = new ArrayList<>();
            Map<String, Object> currentTable = (Map<String, Object>) tables.get(tpTable);
            Map<String, Object> currentColumns = (Map<String, Object>) currentTable.get("columns");
            Map<String, Map<String, Object>> currentRenameFields = renameFields.get(tpTable);
            Map<String, Object> parentTable = (Map<String, Object>) tables.get((String) parent.get(TABLE_NAME));
            Map<String, Object> parentColumns = (Map<String, Object>) parentTable.get("columns");
            Map<String, Map<String, Object>> parentRenameFields = renameFields.get((String) parent.get(TABLE_NAME));

            String parentTargetPath = (String) parent.get(TARGET_PATH);
            if (parentTargetPath == null) {
                parentTargetPath = "";
            }
            String mergeType = "updateWrite";
            String targetPath = "";
            if ("NEW_DOCUMENT".equals(setting.get("type"))) {
                targetPath = parentTargetPath;
            }
            if ("EMBEDDED_DOCUMENT".equals(setting.get("type"))) {
                targetPath = getEmbeddedDocumentPath(parentTargetPath, setting);
            }
            if ("EMBEDDED_DOCUMENT_ARRAY".equals(setting.get("type"))) {
                if (parentTargetPath.equals("")) {
                    targetPath = setting.get(EMBEDDED_PATH);
                } else {
                    targetPath = parentTargetPath + "." + setting.get(EMBEDDED_PATH);
                }
                mergeType = "updateIntoArray";
                List<String> arrayKeys = new ArrayList<>();
                Map<String, Object> fields = (Map<String, Object>) map.get(FIELDS);
                for (String field : fields.keySet()) {
                    Map<String, Object> fieldMap = (Map<String, Object>) fields.get(field);
                    Map<String, Object> source = (Map<String, Object>) fieldMap.get(SOURCE);
                    Boolean isPrimaryKey = (Boolean) source.get(IS_PRIMARY_KEY);
                    if (isPrimaryKey != null && isPrimaryKey) {
                        arrayKeys.add((currentRenameFields.get(field).get(TARGET)).toString());
                    }
                }
                childNode.put("arrayKeys", arrayKeys);
            }

            childNode.put(TARGET_PATH, targetPath);
            childNode.put("mergeType", mergeType);

            childNode.put(TABLE_NAME, tpTable);
            childNode.put(CHILDREN, new ArrayList<>());
            childNode.put("id", sourceToJS.get(child));
            childNode.put(RM_ID_KEY, child);

            // 由于使用外键做关联, 所以似乎 RM 只能合并来自一个源的数据, 所以 tables 表结构使用其中一个就可以

            Map<String,Map<String,String>> sourceJoinKeyMapping = new HashMap<>();
            Map<String,Map<String,String>> targetJoinKeyMapping = new HashMap<>();
            for (String columnKey : currentColumns.keySet()) {
                Map<String, Object> column = (Map<String, Object>) currentColumns.get(columnKey);
                Map<String, Object> foreignKey = (Map<String, Object>) column.get("foreignKey");
                if (foreignKey == null) {
                    continue;
                }
                if (((String)foreignKey.get(TABLE)).equals(parent.get(TABLE_NAME))) {
                    Map<String, String> joinKey = new HashMap<>();
                    String sourceJoinKey = currentRenameFields.get(columnKey).get(TARGET).toString();
                    Map<String, String> newFieldMap = new HashMap<>();
                    newFieldMap.put(SOURCE, columnKey);
                    newFieldMap.put(TARGET, sourceJoinKey);
                    sourceJoinKeyMapping.put(sourceJoinKey, newFieldMap);
                    joinKey.put(SOURCE, sourceJoinKey);
                    String targetJoinKey = parentRenameFields.get((String) foreignKey.get(COLUMN)).get(TARGET).toString();
                    HashMap<String, String> targetNewFieldMap = new HashMap<>();
                    targetNewFieldMap.put(SOURCE, (String) foreignKey.get(COLUMN));
                    targetNewFieldMap.put(TARGET, targetJoinKey);
                    if (parent.get(TARGET_PATH).equals("")) {
                        joinKey.put(TARGET, targetJoinKey);
                    } else {
                        joinKey.put(TARGET, parent.get(TARGET_PATH) + "." + targetJoinKey);
                    }
                    targetJoinKeyMapping.put(targetJoinKey,targetNewFieldMap);
                    joinKeys.add(joinKey);
                }
            }

            parentColumnsFindJoinKeys(parent, renameFields, parentColumns, tpTable, joinKeys, sourceJoinKeyMapping, targetJoinKeyMapping);
            childNode.put("joinKeys", joinKeys);
            joinKeys.forEach(joinKeyMap->{
                String sourceJoinKey = joinKeyMap.get(SOURCE);
                addRenameOpIfDeleteOpHasJoinKey(contentDeleteOperations, contentRenameOperations, child, sourceJoinKeyMapping, sourceJoinKey);
                String targetJoinKey = joinKeyMap.get(TARGET);
                addRenameOpIfDeleteOpHasJoinKey(contentDeleteOperations, contentRenameOperations, parentId, targetJoinKeyMapping, targetJoinKey);
            });
            genProperties(childNode, contentMapping, relationshipsMapping, full, sourceToJS, renameFields, contentDeleteOperations, contentRenameOperations);
            childrenNode.add(childNode);
        }
        parent.put(CHILDREN, childrenNode);
    }


    protected void addRenameOpIfDeleteOpHasJoinKey(Map<String, List<Map<String, Object>>> contentDeleteOperations, Map<String, List<Map<String, Object>>> contentRenameOperations, String tableId, Map<String,Map<String, String>> joinKeyMapping, String joinKey) {
        List<Map<String, Object>> childDeleteOperations = contentDeleteOperations.get(tableId);
        boolean removeJoinKeyFlag = removeDeleteOperation(childDeleteOperations, joinKeyMapping, joinKey);
        if (removeJoinKeyFlag) {
            List<Map<String, Object>> childRenameOperations = contentRenameOperations.get(tableId);
            Map<String, Object> renameOperation = getRenameOperation(joinKeyMapping.get(joinKey).get(SOURCE), joinKeyMapping.get(joinKey).get(TARGET));
            childRenameOperations.add(renameOperation);
        }
    }

    protected static boolean removeDeleteOperation(List<Map<String, Object>> deleteOperations, Map<String, Map<String, String>> joinKeyMapping, String joinKey) {
        boolean flag = deleteOperations.removeIf((delOperations) -> {
            String deleteField = (String) delOperations.get(FIELD);
            String originalField = joinKeyMapping.get(joinKey).get(SOURCE);
            if (deleteField.equals(originalField)) {
                return true;
            }
            return false;
        });
        return flag;
    }

    protected String getEmbeddedDocumentPath(String parentTargetPath, Map<String, String> setting) {
        String targetPath;
        if (parentTargetPath.equals("")) {
            targetPath = setting.get(EMBEDDED_PATH);
        } else {
            if (StringUtils.isBlank(setting.get(EMBEDDED_PATH))) {
                targetPath = parentTargetPath;
            } else {
                targetPath = parentTargetPath + "." + setting.get(EMBEDDED_PATH);
            }
        }
        return targetPath;
    }

    protected void parentColumnsFindJoinKeys(Map<String, Object> parent, Map<String, Map<String, Map<String, Object>>> renameFields, Map<String, Object> parentColumns, String tpTable, List<Map<String, String>> joinKeys, Map<String,Map<String, String>> souceJoinKeyMapping, Map<String,Map<String, String>> targetJoinKeyMapping) {
        Map<String, Map<String, Object>> parentRenameFields = renameFields.get((String) parent.get(TABLE_NAME));
        for (String columnKey : parentColumns.keySet()) {
            Map<String, Object> column = (Map<String, Object>) parentColumns.get(columnKey);
            Map<String, Object> foreignKey = (Map<String, Object>) column.get("foreignKey");
            if (foreignKey == null) {
                continue;
            }
            if (((String) foreignKey.get(TABLE)).equals(tpTable)) {
                Map<String, String> joinKey = new HashMap<>();
                String sourceJoinKey = renameFields.get(tpTable).get(((String) foreignKey.get(COLUMN))).get(TARGET).toString();
                Map<String,String> sourceNewFieldMap=new HashMap<>();
                sourceNewFieldMap.put(SOURCE, (String) foreignKey.get(COLUMN));
                sourceNewFieldMap.put(TARGET,sourceJoinKey);
                souceJoinKeyMapping.put(sourceJoinKey,sourceNewFieldMap);
                joinKey.put(SOURCE, sourceJoinKey);
                Map<String,String> targetNewFieldMap=new HashMap<>();
                String targetJoinKey = parentRenameFields.get(columnKey).get(TARGET).toString();
                targetNewFieldMap.put(SOURCE, columnKey);
                targetNewFieldMap.put(TARGET, targetJoinKey);
                String targetPath = parent.get(TARGET_PATH).toString();
                if (!StringUtils.isBlank(targetPath)) {
                    targetJoinKey=targetPath + "." + targetJoinKey;
                }
                joinKey.put(TARGET, targetJoinKey);
                targetJoinKeyMapping.put(targetJoinKey,targetNewFieldMap);
                joinKeys.add(joinKey);
            }
        }
    }

    private String replaceId(String id, Map<String, String> globalIdMap) {
        if (globalIdMap.containsKey(id)) {
            return globalIdMap.get(id);
        }
        String newId = UUID.randomUUID().toString();
        globalIdMap.put(id, newId);
        return newId;
    }

    protected void replaceRmProjectId(Map<String, Object> rmProject) {
        Map<String, String> globalIdMap = new HashMap<>();
        Map<String, Object> project = (Map<String, Object>) rmProject.get("project");
        Map<String, Object> content = (Map<String, Object>) project.get("content");
        Map<String, Object> contentCollections = (Map<String, Object>) content.get(COLLECTIONS);
        Set<String> contentCollectionKeys = new HashSet<>(contentCollections.keySet());
        for (String key : contentCollectionKeys) {
            Map<String, Object> collection = (Map<String, Object>) contentCollections.get(key);
            String replaceKey = replaceId(key, globalIdMap);
            contentCollections.remove(key);
            contentCollections.put(replaceKey, collection);
        }

        Map<String, Object> contentMapping = (Map<String, Object>) content.get(MAPPINGS);
        Set<String> contentMappingKeys = new HashSet<>(contentMapping.keySet());
        for (String key : contentMappingKeys) {
            Map<String, Object> mapping = (Map<String, Object>) contentMapping.get(key);
            String replaceKey = replaceId(key, globalIdMap);
            contentMapping.remove(key);
            contentMapping.put(replaceKey, mapping);
        }
        contentMappingKeys = new HashSet<>(contentMapping.keySet());
        for (String key : contentMappingKeys) {
            Map<String, Object> mapping = (Map<String, Object>) contentMapping.get(key);
            mapping.put(COLLECTION_ID, replaceId((String) mapping.get(COLLECTION_ID), globalIdMap));
        }
        replaceRelationShipsKey(globalIdMap, content);
    }

    protected void replaceRelationShipsKey(Map<String, String> globalIdMap, Map<String, Object> content) {
        Map<String, Object> relationships = content.get(RELATIONSHIPS) == null ? new HashMap<>() : (Map<String, Object>) content.get(RELATIONSHIPS);
        Map<String, Object> relationshipsCollection = relationships.get(COLLECTIONS) == null ? new HashMap<>() : (Map<String, Object>) relationships.get(COLLECTIONS);
        Set<String> relationshipsCollectionKeys = new HashSet<>(relationshipsCollection.keySet());
        Map<String, Object> relationshipsMapping = relationships.get(MAPPINGS) == null ? new HashMap<>() : (Map<String, Object>) relationships.get(MAPPINGS);
        for (String key : relationshipsCollectionKeys) {
            Map<String, Object> collection = (Map<String, Object>) relationshipsCollection.get(key);
            String replaceKey = replaceId(key, globalIdMap);
            relationshipsCollection.remove(key);
            relationshipsCollection.put(replaceKey, collection);
        }
        for (String key : relationshipsCollection.keySet()) {
            Map<String, Object> collection = (Map<String, Object>) relationshipsCollection.get(key);
            List<String> oldMappingIds = (List<String>) collection.get(MAPPINGS);
            List<String> newMappingIds = new ArrayList<>();
            for (String oldMappingId : oldMappingIds) {
                newMappingIds.add(replaceId(oldMappingId, globalIdMap));
            }
            collection.put(MAPPINGS, newMappingIds);
        }

        Set<String> relationshipsMappingKeys = new HashSet<>(relationshipsMapping.keySet());
        for(String key : relationshipsMappingKeys) {
            Map<String, Object> mapping = (Map<String, Object>) relationshipsMapping.get(key);
            String replaceKey = replaceId(key, globalIdMap);
            relationshipsMapping.remove(key);
            relationshipsMapping.put(replaceKey, mapping);
        }

        for(String key : relationshipsMapping.keySet()) {
            Map<String, Object> mapping = (Map<String, Object>) relationshipsMapping.get(key);
            List<String> oldChildren = (List<String>) mapping.get(CHILDREN);
            List<String> newChildren = new ArrayList<>();
            for (String oldChild : oldChildren) {
                newChildren.add(replaceId(oldChild, globalIdMap));
            }
            mapping.put(CHILDREN, newChildren);
        }
    }

    protected Map<String, String> parseTaskFromRm(String rmJson, String sourceConnectionId, String targetConnectionId, UserDetail user) {
        Map<String, String> sourceToJs = new HashMap<>();
        Map<String, String> parsedTpTasks = new HashMap<>();
        Map<String, Object> rmProject = new HashMap<>();
        Map<String, Map<String, Map<String, Object>>> renameFields = new HashMap();
        try {
            rmProject = new ObjectMapper().readValue(rmJson, HashMap.class);
        } catch (Exception e) {
            throw new BizException("Can not convert rmProject");
        }

        replaceRmProjectId(rmProject);

        List<Map<String, Object>> tpTasks = new ArrayList<>();
        HashMap<String, Object> project = (HashMap<String, Object>) rmProject.get("project");
        HashMap<String, Object> content = (HashMap<String, Object>) project.get("content");
        HashMap<String, Object> contentCollections = (HashMap<String, Object>) content.get(COLLECTIONS);
        for (String key : contentCollections.keySet()) {
            Map<String, Object> task = new HashMap<>();
            task.put("id", key);
            task.put("name", ((Map<String, Map<String, String>>)contentCollections.get(key)).get("name"));
            task.put("targetModelName", ((Map<String, Map<String, String>>)contentCollections.get(key)).get("name"));
            tpTasks.add(task);
        }
        for (Map<String, Object> task : tpTasks) {
            task.put("editVersion", System.currentTimeMillis());
            task.put(SYNC_TYPE, "sync");
            task.put("type", INITIAL_SYNC_CDC);
            task.put("mappingTemplate", "sync");
            task.put(STATUS, "edit");
            task.put(USER_ID, user.getUserId());
            task.put("customId", user.getCustomerId());
            task.put("createUser", user.getUsername());

            String targetModelName = (String) task.get("targetModelName");

            Map<String, Object> dag = new HashMap<>();
            List<Map<String, Object>> nodes = new ArrayList<>();
            List<Map<String, Object>> edges = new ArrayList<>();

            Map<String, Object> contentMapping = content.get(MAPPINGS) == null ? new HashMap<>() : (Map<String, Object>) content.get(MAPPINGS);


            String tpTable;

            // 把源节点都加进去, 这里如果有一些 字段改名, 或者新字段生成的操作, 增加一个 JS 处理器
            List<String> sourceNodes = new ArrayList<>();
            Map<String,List<Map<String, Object>>> contentDeleteOperations = new HashMap<>();
            Map<String,List<Map<String, Object>>> contentRenameOperations = new HashMap<>();
            for (String key : contentMapping.keySet()) {
                Map<String, Object> node = new HashMap<>();
                Map<String, Object> contentMappingzValue = (Map<String, Object>) contentMapping.get(key);
                if (!contentMappingzValue.get(COLLECTION_ID).equals(task.get("id"))) {
                    continue;
                }

                // 用 . 分割, 取最后一位
                String table = (String) contentMappingzValue.get(TABLE);
                tpTable = table.split("\\.")[table.split("\\.").length - 1];
                Map<String, Map<String, Object>> tableRenameFields = new HashMap<>();

                node.put("type", TABLE);
                node.put(TABLE_NAME, tpTable);
                node.put("name", tpTable);
                node.put("id", key);
                node.put(CONNECTION_ID, sourceConnectionId);
                nodes.add(node);

                // 增加 JS 处理器
                Map<String, Object> fields = (Map<String, Object>) contentMappingzValue.get(FIELDS);

                List<Map<String, Object>> renameOperations = new ArrayList<>();

                List<Map<String, Object>> deleteOperations = new ArrayList<>();
                contentDeleteOperations.put(key,deleteOperations);
                contentRenameOperations.put(key,renameOperations);
                for (String field : fields.keySet()) {
                    Map<String, Object> fieldMap = (Map<String, Object>) fields.get(field);
                    Map<String, Object> source = (Map<String, Object>) fieldMap.get(SOURCE);
                    Map<String, Object> target = (Map<String, Object>) fieldMap.get(TARGET);
                    Map<String, Object> newName = getNewNameMap(target, source);
                    tableRenameFields.put(source.get("name").toString(), newName);
                    Object isPk = source.get(IS_PRIMARY_KEY);
                    if (!(Boolean)target.get("included")) {
                        Map<String, Object> deleteOperation = getDeleteOperation(source.get("name").toString(),isPk);
                        deleteOperations.add(deleteOperation);
                        continue;
                    }

                    if (source.get("name").equals(target.get("name"))) {
                        continue;
                    }

                    Map<String, Object> renameOperation = getRenameOperation(source.get("name"), target.get("name"));
                    renameOperations.add(renameOperation);
                }

                renameFields.put(tpTable, tableRenameFields);

                String script = "";
                String declareScript = "";
                Map<String, Object> calculatedFields = (Map<String, Object>) contentMappingzValue.get("calculatedFields");
                for (String field : calculatedFields.keySet()) {
                    Map<String, Object> fieldMap = (Map<String, Object>) calculatedFields.get(field);
                    String newFieldName = (String) fieldMap.get("name");
                    String newFieldeExpression = (String) fieldMap.get("expression");
                    newFieldeExpression = newFieldeExpression.replace("columns[", "record[");
                    script += "    record[\"" + newFieldName + "\"] = " + newFieldeExpression + ";\n";
                }

                if (!script.equals("") || !declareScript.equals("")) {
                    script = "function process(record){" + script;
                    script += "    return record;\n";
                    script += "}";
                }
                String sourceId = (String) node.get("id");
                //add jsNode
                if (!script.equals("")) {
                    String jsId = addJSNode(tpTable, script, declareScript, nodes, sourceId, edges);
                    sourceId = jsId;
                }
                //add rename processor node
                sourceId = addRenameNode(tpTable, renameOperations, sourceId, nodes, edges);
                //add delete processor node
                if (!deleteOperations.isEmpty()) {
                    sourceId = addDeleteNode(tpTable, deleteOperations,  sourceId,nodes, edges);
                }

                // 记录映射
                sourceToJs.put((String) node.get("id"), sourceId);
                sourceNodes.add(sourceId);
            }


            List<Map<String, Object>> mergeProperties = new ArrayList<>();
            Map<String, Object> schema = (Map<String, Object>) rmProject.get("schema");
            Map<String, Object> full = (Map<String, Object>) schema.get("full");

            Map<String, Object> relationships = content.get(RELATIONSHIPS) == null ? new HashMap<>() : (Map<String, Object>) content.get(RELATIONSHIPS);
            Map<String, Object> relationshipsMapping = relationships.get(MAPPINGS) == null ? new HashMap<>() : (Map<String, Object>) relationships.get(MAPPINGS);
            String rootNodeId = "";
            String rootTableName = "";
            Map<String, Object> rootContentMappingValue = new HashMap<>();
            for (String key : contentMapping.keySet()) {
                Map<String, Object> contentMappingValue = (Map<String, Object>) contentMapping.get(key);
                if (!contentMappingValue.get(COLLECTION_ID).equals(task.get("id"))) {
                    continue;
                }
                Map<String, Object> setting = (Map<String, Object>) contentMappingValue.get("settings");
                if (setting == null) {
                    continue;
                }
                String type = (String) setting.get("type");
                if (type == null) {
                    continue;
                }
                if (type.equals("NEW_DOCUMENT")) {
                    rootNodeId = key;
                    rootContentMappingValue = contentMappingValue;
                    rootTableName = ((String) rootContentMappingValue.get(TABLE)).split("\\.")[((String) rootContentMappingValue.get(TABLE)).split("\\.").length - 1];
                    break;
                }
            }

            if (rootNodeId == null || rootNodeId.equals("")) {
                continue;
            }

            // 增加主从合并节点
            String mergeNodeId = UUID.randomUUID().toString().toLowerCase();
            Map<String, Object> mergeNode = new HashMap<>();
            mergeNode.put("type", "merge_table_processor");
            mergeNode.put("name", "merge");
            mergeNode.put("id", mergeNodeId);
            mergeNode.put(CATALOG, PROCESSOR);
            mergeNode.put("mergeMode", "main_table_first");
            mergeNode.put("isTransformed", false);
            Map<String, Object> rootProperties = new HashMap<>();
            rootProperties.put(TARGET_PATH, "");
            rootProperties.put("id", sourceToJs.get(rootNodeId));
            rootProperties.put(RM_ID_KEY, rootNodeId);
            rootProperties.put("mergeType", "updateOrInsert");
            tpTable = ((String) ((Map<String, Object>) contentMapping.get(rootNodeId)).get(TABLE)).split("\\.")[((String) ((Map<String, Object>) contentMapping.get(rootNodeId)).get(TABLE)).split("\\.").length - 1];
            rootProperties.put(TABLE_NAME, tpTable);
            genProperties(rootProperties, contentMapping, relationshipsMapping, full, sourceToJs, renameFields,contentDeleteOperations,contentRenameOperations);
            mergeProperties.add(rootProperties);
            mergeNode.put("mergeProperties", mergeProperties);
            Boolean needMergeNode = true;
            List<Object> children = (List<Object>) rootProperties.get(CHILDREN);
            if (children == null || children.isEmpty()) {
                needMergeNode = false;
            }


            if (needMergeNode) {
                nodes.add(mergeNode);
                for (String sourceId : sourceNodes) {
                    Map<String, Object> edge = new HashMap<>();
                    edge.put(SOURCE, sourceId);
                    edge.put(TARGET, mergeNodeId);
                    edges.add(edge);
                }
                contentDeleteOperations.forEach((k, v) -> {
                    Map<String, Object> contentMappingzValue = (Map<String, Object>) contentMapping.get(k);
                    String table = contentMappingzValue.get(TABLE).toString();
                    String finalTable = table.split("\\.")[table.split("\\.").length - 1];
                    v.removeIf((delOp) -> {
                        boolean flag = Boolean.TRUE.equals(delOp.get("isPk"));
                        if(flag){
                            Map<String, Map<String, Object>> map = renameFields.get(finalTable);
                            String name = delOp.get(FIELD).toString();
                            Map<String, Object> renameOperation = getRenameOperation(name, map.get(name).get(TARGET).toString());
                            contentRenameOperations.get(k).add(renameOperation);
                        }
                        return flag;
                    });

                });

            }


            // 把目标节点加进去
            Map<String, Object> targetNode = new HashMap<>();
            targetNode.put("type", TABLE);
            targetNode.put("existDataProcessMode", "keepData");
            targetNode.put("name", targetModelName);
            targetNode.put("id", task.get("id"));
            targetNode.put(CONNECTION_ID, targetConnectionId);
            targetNode.put(TABLE_NAME, targetModelName);
            List<String> updateConditionFields = new ArrayList<>();
            Map<String, Map<String, Object>> rootRenameFields = renameFields.get(rootTableName);
            Map<String, Object> rootFields = (Map<String, Object>) rootContentMappingValue.get(FIELDS);
            for (String field : rootFields.keySet()) {
                Map<String, Object> fieldMap = (Map<String, Object>) rootFields.get(field);
                Map<String, Object> source = (Map<String, Object>) fieldMap.get(SOURCE);
                Boolean isPrimaryKey = (Boolean) source.get(IS_PRIMARY_KEY);
                if (isPrimaryKey != null && isPrimaryKey) {
                    updateConditionFields.add(rootRenameFields.get(field).get(TARGET).toString());
                }
            }

            targetNode.put("updateConditionFields", updateConditionFields);
            nodes.add(targetNode);
            Map<String, Object> edge = new HashMap<>();
            if (needMergeNode) {
                edge.put(SOURCE, mergeNodeId);
            } else {
                edge.put(SOURCE, sourceNodes.get(0));
            }
            edge.put(TARGET, targetNode.get("id"));
            edges.add(edge);

            dag.put("nodes", nodes);
            dag.put("edges", edges);
            task.put("dag", dag);
            parsedTpTasks.put((String) task.get("id"), JsonUtil.toJson(task));
        }
        return parsedTpTasks;
    }

    protected String addJSNode(String tpTable, String script, String declareScript, List<Map<String, Object>> nodes, String sourceId, List<Map<String, Object>> edges) {
        String jsId = UUID.randomUUID().toString().toLowerCase();
        Map<String, Object> jsNode = new HashMap<>();
        jsNode.put("type", "js_processor");
        jsNode.put("name", tpTable);
        jsNode.put("id", jsId);
        jsNode.put("jsType", 1);
        jsNode.put(PROCESSOR_THREAD_NUM, 1);
        jsNode.put(CATALOG, PROCESSOR);
        jsNode.put(ELEMENT_TYEP, "Node");
        jsNode.put("script", script);
        jsNode.put("declareScript", declareScript);
        nodes.add(jsNode);
        Map<String, Object> edge = new HashMap<>();
        edge.put(SOURCE, sourceId);
        edge.put(TARGET, jsId);
        edges.add(edge);
        return jsId;
    }

    protected String addRenameNode(String tpTable,  List<Map<String, Object>> renameOperations, String sourceId,List<Map<String, Object>> nodes, List<Map<String, Object>> edges) {
        Map<String, Object> renameNode = new HashMap<>();
        String renameId = UUID.randomUUID().toString().toLowerCase();
        renameNode.put("id", renameId);
        renameNode.put(CATALOG, PROCESSOR);
        renameNode.put(ELEMENT_TYEP, "Node");
        renameNode.put("fieldsNameTransform", "");
        renameNode.put("isTransformed", false);
        renameNode.put("name", "Rename " + tpTable);
        renameNode.put(PROCESSOR_THREAD_NUM, 1);
        renameNode.put("type", "field_rename_processor");
        nodes.add(renameNode);
        renameNode.put("operations", renameOperations);
        Map<String, Object> edge = new HashMap<>();
        edge.put(SOURCE, sourceId);
        edge.put(TARGET, renameId);
        edges.add(edge);
        sourceId = renameId;
        return sourceId;
    }

    protected String addDeleteNode(String tpTable, List<Map<String, Object>> deleteOperations,  String sourceId,List<Map<String, Object>> nodes, List<Map<String, Object>> edges) {
        Map<String, Object> deleteNode = new HashMap<>();
        String deleteId = UUID.randomUUID().toString().toLowerCase();
        deleteNode.put("id", deleteId);
        deleteNode.put(CATALOG, PROCESSOR);
        deleteNode.put("deleteAllFields", false);
        deleteNode.put(ELEMENT_TYEP, "Node");
        deleteNode.put("name", "Delete " + tpTable);
        deleteNode.put("type", "field_add_del_processor");
        deleteNode.put(PROCESSOR_THREAD_NUM, 1);
        deleteNode.put("operations", deleteOperations);
        nodes.add(deleteNode);
        Map<String, Object> edge = new HashMap<>();
        edge.put(SOURCE, sourceId);
        edge.put(TARGET, deleteId);
        edges.add(edge);
        sourceId = deleteId;
        return sourceId;
    }

    protected Map<String, Object> getDeleteOperation(Object deleteFieldName, Object isPrimaryKey) {
        Map<String, Object> deleteOperation = new HashMap<>();
        deleteOperation.put("id", UUID.randomUUID().toString().toLowerCase());
        deleteOperation.put(FIELD, deleteFieldName);
        deleteOperation.put("op", "REMOVE");
        deleteOperation.put("operand", "true");
        deleteOperation.put("label", deleteFieldName);
        deleteOperation.put("isPk",isPrimaryKey);
        return deleteOperation;
    }

    protected Map<String, Object> getRenameOperation(Object source, Object target) {
        Map<String, Object> fieldRenameOperation = new HashMap<>();
        fieldRenameOperation.put("id", UUID.randomUUID().toString().toLowerCase());
        fieldRenameOperation.put(FIELD, source);
        fieldRenameOperation.put("op", "RENAME");
        fieldRenameOperation.put("operand", target);
        return fieldRenameOperation;
    }

    protected Map<String, Object> getNewNameMap(Map<String, Object> target, Map<String, Object> source) {
        Map<String, Object> newName = new HashMap<>();
        newName.put(TARGET, target.get("name").toString());
        newName.put(IS_PRIMARY_KEY, (Boolean) source.get(IS_PRIMARY_KEY));
        return newName;
    }

    public void importRmProject(MultipartFile multipartFile, UserDetail user, boolean cover, List<String> tags, String source, String sink) throws IOException {
        String json = new String(multipartFile.getBytes());
        Map<String, String> tasks = parseTaskFromRm(json, source, sink, user);
        if (tasks == null) {
            return;
        }
        List<TaskDto> tpTasks = new ArrayList<>();
        for (String key: tasks.keySet()) {
            TaskDto tpTask = JsonUtil.parseJsonUseJackson(tasks.get(key), TaskDto.class);
            tpTask.setTransformTaskId(new ObjectId().toHexString());
            tpTasks.add(tpTask);
        }
        batchImport(tpTasks, user, cover, tags, new HashMap<>(), new HashMap<>());
        checkJsProcessorTestRun(user, tpTasks);
    }

    public void checkJsProcessorTestRun(UserDetail user, List<TaskDto> tpTasks) {
        for (TaskDto task: tpTasks) {
            DAG dag = task.getDag();
            List<Node> nodes = dag.getNodes();
            for (Node node : nodes) {
                if (!node.getType().equals("js_processor")) {
                    continue;
                }
                TestRunDto testRunDto = new TestRunDto();
                testRunDto.setTaskId(task.getId().toHexString());
                testRunDto.setRows(1);
                testRunDto.setJsNodeId(node.getId());
                testRunDto.setScript(((ScriptProcessNode)node).getScript());
                testRunDto.setVersion(3L);
                testRunDto.setTableName(node.getName());
                taskNodeService.testRunJsNodeRPC(testRunDto, user, 1);
            }
        }
    }

    public void batchUpTask(MultipartFile multipartFile, UserDetail user, boolean cover, List<String> tags) {
        byte[] bytes;
        List<TaskUpAndLoadDto> taskUpAndLoadDtos;

        if (!Objects.requireNonNull(multipartFile.getOriginalFilename()).endsWith("json.gz")) {
            //不支持其他的格式文件
            throw new BizException(TASK_IMPORT_FORMAT_ERROR);
        }

        try {
            bytes = GZIPUtil.unGzip(multipartFile.getBytes());

            String json = new String(bytes, StandardCharsets.UTF_8);

            taskUpAndLoadDtos = JsonUtil.parseJsonUseJackson(json, new TypeReference<List<TaskUpAndLoadDto>>() {
            });
        } catch (Exception e) {
            //e.printStackTrace();
            //不支持其他的格式文件
            throw new BizException(TASK_IMPORT_FORMAT_ERROR);
        }

        if (taskUpAndLoadDtos == null) {
            //不支持其他的格式文件
            throw new BizException(TASK_IMPORT_FORMAT_ERROR);
        }

        List<MetadataInstancesDto> metadataInstancess = new ArrayList<>();
        List<TaskDto> tasks = new ArrayList<>();
        List<DataSourceConnectionDto> connections = new ArrayList<>();
        Set<ObjectId> connectionIds = new HashSet<>();
        List<CustomNodeDto> customNodeDtos = new ArrayList<>();
        for (TaskUpAndLoadDto taskUpAndLoadDto : taskUpAndLoadDtos) {
            try {
                String dtoJson = taskUpAndLoadDto.getJson();
                if (StringUtils.isBlank(taskUpAndLoadDto.getJson())) {
                    continue;
                }
                if (METADATA_INSTANCES.equals(taskUpAndLoadDto.getCollectionName())) {
                    metadataInstancess.add(JsonUtil.parseJsonUseJackson(dtoJson, MetadataInstancesDto.class));
                } else if ("Task".equals(taskUpAndLoadDto.getCollectionName())) {
                    tasks.add(JsonUtil.parseJsonUseJackson(dtoJson, TaskDto.class));
                } else if ("Connections".equals(taskUpAndLoadDto.getCollectionName())) {
                    DataSourceConnectionDto connectionDto = JsonUtil.parseJsonUseJackson(dtoJson, DataSourceConnectionDto.class);
                    if (connectionDto != null) {
                        if (!connectionIds.contains(connectionDto.getId())) {
                            connections.add(connectionDto);
                            if (connectionDto.getId() != null) {
                                connectionIds.add(connectionDto.getId());
                            }
                        }
                    }
                } else if ("CustomNodeTemps".equals(taskUpAndLoadDto.getCollectionName())) {
                    customNodeDtos.add(JsonUtil.parseJsonUseJackson(dtoJson, CustomNodeDto.class));
                }
            } catch (Exception e) {
                log.error("error", e);
            }
        }
        batchUpChecker.checkDataSourceConnection(connections, user);
        Map<String, CustomNodeDto> customNodeMap = new HashMap<>();
        Map<String, DataSourceConnectionDto> conMap = new HashMap<>();
        Map<String, MetadataInstancesDto> metaMap = new HashMap<>();
        try {
            agentGroupService.importAgentInfo(tasks, user);
            customNodeMap = customNodeService.batchImport(customNodeDtos, user, cover);
            conMap = dataSourceService.batchImport(connections, user, cover);
            metaMap = metadataInstancesService.batchImport(metadataInstancess, user, cover, conMap);
        } catch (Exception e) {
            log.error("metadataInstancesService.batchImport error", e);
        }
        try {
            batchImport(tasks, user, cover, tags, conMap, metaMap);
        } catch (Exception e) {
            log.error("tasks.batchImport error", e);
        }
    }

    /**
     *
     * @param taskDtos
     * @param user
     * @param cover
     * @param tags
     * @param conMap 为后续多租户的调整做准备
     * @param metaMap 为后续多租户的调整做准备
     */
    public void batchImport(List<TaskDto> taskDtos, UserDetail user, boolean cover, List<String> tags, Map<String, DataSourceConnectionDto> conMap, Map<String, MetadataInstancesDto> metaMap) {

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
            Query query = new Query(Criteria.where("_id").is(taskDto.getId()).and(IS_DELETED).ne(true));
            query.fields().include("_id", USER_ID);
            TaskDto one = findOne(query, user);

            taskDto.setListtags(null);
            taskDto.setStatus(TaskDto.STATUS_EDIT);
            taskDto.setTaskRecordId(new ObjectId().toHexString()); // 导入后不读旧指标数据

            Map<String, Object> attrs = taskDto.getAttrs();
            if (attrs != null) {
                attrs.remove(EDGE_MILESTONES);
                attrs.remove(SYNC_PROGRESS);
            }

            if (one == null) {
                TaskDto one1 = findOne(new Query(Criteria.where("_id").is(taskDto.getId()).and(IS_DELETED).ne(true)));
                if (one1 != null) {
                    taskDto.setId(null);
                    taskDto.getDag().getNodes().forEach(node -> {
                        if(node instanceof DatabaseNode){
                            DatabaseNode databaseNode = (DatabaseNode) node;
                            if(conMap.containsKey(databaseNode.getConnectionId())){
                                DataSourceConnectionDto dataSourceCon = conMap.get(databaseNode.getConnectionId());
                                databaseNode.setConnectionId(dataSourceCon.getId().toString());
                            }
                        }
                    });
                }
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
                    if (taskDto.getId() == null) {
                        taskDto.setId(new ObjectId());
                    }
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
                repository.getMongoOperations().updateFirst(new Query(Criteria.where("_id").is(taskDto.getId())), Update.update(STATUS, TaskDto.STATUS_EDIT), TaskEntity.class);
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
            List<Map<String, Map<String, Object>>> orList = (List) where.remove("or");
            for (Map<String, Map<String, Object>> orMap : orList) {
                orMap.forEach((key, value) -> {
                    if (value.containsKey(REGEX)) {
                        Object queryStr = value.get(REGEX);
                        Criteria orCriteria = Criteria.where(key).regex(queryStr.toString());
                        criteriaList.add(orCriteria);
                    } else if (value.containsKey("$eq")) {
                        Object queryStr = value.get("$eq");
                        Criteria orCriteria = Criteria.where(key).is(queryStr);
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
            instanceQuery.addCriteria(Criteria.where("source._id").is(connectionId).and("meta_type").is(TABLE).and(IS_DELETED).ne("true"));
            metadataInstancesList = metadataInstancesService.findAll(instanceQuery);

        } else {
            List<String> qualifiedNameList = transformerItemList.stream().map(MetadataTransformerItemDto::getSinkQulifiedName).distinct().collect(Collectors.toList());
            Query instantiatedQuery = new Query();
            instantiatedQuery.addCriteria(Criteria.where(QUALIFIED_NAME).in(qualifiedNameList));

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
    public UpdateResult renewNotSendMq(TaskDto taskDto, UserDetail user) {
        log.info("renew task, task name = {}, username = {}", taskDto.getName(), user.getUsername());


        Update set = resetUpdate();
        set.unset("temp")
                .unset("milestones")
                .unset(TM_CURRENT_TIME)
                .set("agentTags", null)
                .set("scheduleTimes", null)
                .set("scheduleTime", null)
                .set("messages", null)
                .set("errorEvents", null);


        if (taskDto.getAttrs() != null) {
            taskDto.getAttrs().remove(SYNC_PROGRESS);
            taskDto.getAttrs().remove(EDGE_MILESTONES);
            taskDto.getAttrs().remove("milestone");
            taskDto.getAttrs().remove("nodeMilestones");
            taskDto.getAttrs().remove(TaskDto.ATTRS_USED_SHARE_CACHE);
            taskDto.getAttrs().remove(TaskDto.ATTRS_SKIP_ERROR_EVENT);
            taskDto.getAttrs().remove("SNAPSHOT_ORDER_LIST");
            AutoInspectUtil.removeProgress(taskDto.getAttrs());

            set.set(ATTRS, taskDto.getAttrs());
        }


        //清空当前子任务的所有的node运行信息TaskRuntimeInfo
        List<Node> nodes = taskDto.getDag().getNodes();

        beforeSave(taskDto, user);
        set.unset("tempDag").set("isEdit", true);
        Criteria criteriaTask = Criteria.where("_id").is(taskDto.getId()).and(STATUS).is(TaskDto.STATUS_RENEWING);
        UpdateResult updateResult = update(new Query(criteriaTask), set, user);

        if (updateResult.getMatchedCount() > 0) {
            if (nodes != null) {

                List<String> nodeIds = nodes.stream().map(Node::getId).collect(Collectors.toList());
                Criteria criteria = Criteria.where(TASK_ID).is(taskDto.getId().toHexString())
                        .and("type").is(TaskSnapshotProgress.ProgressType.EDGE_PROGRESS.name())
                        .orOperator(Criteria.where("srcNodeId").in(nodeIds),
                                Criteria.where("tgtNodeId").in(nodeIds));
                Query query = new Query(criteria);

                snapshotEdgeProgressService.deleteAll(query);

                Criteria criteria1 = Criteria.where(TASK_ID).is(taskDto.getId().toHexString())
                        .and("type").is(TaskSnapshotProgress.ProgressType.TASK_PROGRESS.name());
                Query query1 = new Query(criteria1);

                snapshotEdgeProgressService.deleteAll(query1);
            }
        }
        return updateResult;
    }

    protected void sendRenewMq(TaskDto taskDto, UserDetail user, String opType) {
        DataSyncMq mq = new DataSyncMq();
        mq.setTaskId(taskDto.getId().toHexString());
        mq.setOpType(opType);
        mq.setType(MessageType.DATA_SYNC.getType());
        Map<String, Object> data;
        String json = JsonUtil.toJsonUseJackson(mq);
        data = JsonUtil.parseJsonUseJackson(json, Map.class);
        MessageQueueDto queueDto = new MessageQueueDto();
        queueDto.setReceiver(taskDto.getAgentId());
        queueDto.setData(data);
        queueDto.setType("pipe");

        log.debug("build stop task websocket context, processId = {}, userId = {}, queueDto = {}", taskDto.getAgentId(), user.getUserId(), queueDto);
        messageQueueService.sendMessage(queueDto);

    }

    @NotNull
    private static Update resetUpdate() {
        Update update = new Update()
                .unset(START_TIME)
                .unset("stopTime")
                .unset(STOP_RETRY_TIMES)
                .unset(CURRENT_EVENT_TIMESTAMP)
                .unset("snapshotDoneAt")
                .unset(SCHEDULE_DATE)
                .unset(STOPED_DATE)
                .unset("functionRetryEx")
                .unset("taskRetryStatus")
                .unset(FUNCTION_RETRY_STATUS);
        return update;
    }

    protected boolean findProcessNodeListWithGroup(TaskDto taskDto, List<String> accessNodeProcessIdList, UserDetail user) {
        try {
            List<String> list = agentGroupService.getProcessNodeListWithGroup(taskDto, user);
            if (CollectionUtils.isNotEmpty(list)) {
                accessNodeProcessIdList.addAll(list);
            }
        } catch (BizException e) {
            if ("group.agent.not.available".equals(e.getErrorCode())) {
                return true;
            }
            throw e;
        }
        return false;
    }

    public boolean findAgent(TaskDto taskDto, UserDetail user) {
        boolean noAgent = false;
        List<Worker> availableAgent = workerService.findAvailableAgent(user);
        List<String> accessNodeProcessIdList = Lists.newArrayList();
        if (findProcessNodeListWithGroup(taskDto, accessNodeProcessIdList, user)) {
            return true;
        }
        if (AccessNodeTypeEnum.isManually(taskDto.getAccessNodeType())
                && CollectionUtils.isNotEmpty(accessNodeProcessIdList)) {

            List<String> processIds = availableAgent.stream().map(Worker::getProcessId).collect(Collectors.toList());
            String agentId = null;
            for (String p : accessNodeProcessIdList) {
                if (processIds.contains(p)) {
                    agentId = p;
                    break;
                }
            }
            if (StringUtils.isBlank(agentId)) {
                noAgent = true;
                //任务指定的agent已经停用，当前操作不给用。
                // throw new BizException("Agent.DesignatedAgentNotAvailable");
            }

            taskDto.setAgentId(agentId);

        } else {
            if (CollectionUtils.isNotEmpty(availableAgent)) {
                Worker worker = availableAgent.get(0);
                taskDto.setAgentId(worker.getProcessId());
            } else {
                noAgent = true;
                taskDto.setAgentId(null);
            }
        }
        return noAgent;
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
            if (null == attrs){
                attrs = new HashMap<>();
            }
            return (String) attrs.get(CONNECTION_NAME);
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
        TaskDto taskDto = checkExistById(id, user);
        try {
            start(taskDto, user, "11");
        } catch (Exception e) {
            monitoringLogsService.startTaskErrorLog(taskDto, user, e, Level.ERROR);
            throw e;
        }
    }

    public void start(ObjectId id, UserDetail user, boolean system) {
        start(id, user);
    }

    public void start(TaskDto taskDto, UserDetail user, String startFlag, boolean system) {
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
    public void start(TaskDto taskDto, UserDetail user, String startFlag) {

        if (taskDto.getShareCdcEnable() && !TaskDto.SYNC_TYPE_LOG_COLLECTOR.equals(taskDto.getSyncType())) {
            //如果是共享挖掘任务给一个队列，避免混乱
            lockControlService.logCollectorStartQueue(user);
        }


//        if (TaskDto.LDP_TYPE_FDM.equals(taskDto.getLdpType())) {
//            //如果是共享挖掘任务给一个队列，避免混乱
//            lockControlService.fdmStartQueue(user);
//        }

        Update update = Update.update("lastStartDate", System.currentTimeMillis());
        if (StringUtils.isBlank(taskDto.getTaskRecordId())) {
            String taskRecordId = new ObjectId().toHexString();
            update.set(TASK_RECORD_ID, taskRecordId);
            taskDto.setTaskRecordId(taskRecordId);

            TaskEntity taskSnapshot = new TaskEntity();
            BeanUtil.copyProperties(taskDto, taskSnapshot);
            disruptorService.sendMessage(DisruptorTopicEnum.CREATE_RECORD, new TaskRecord(taskDto.getTaskRecordId(), taskDto.getId().toHexString(), taskSnapshot, user.getUserId(), DateUtil.date()));
        }
        if (Objects.isNull(taskDto.getStartTime())) {
            DateTime date = DateUtil.date();
            update.set(START_TIME, date);
            taskDto.setStartTime(date);
        }
        update(Query.query(Criteria.where("_id").is(taskDto.getId().toHexString())), update);

        checkDagAgentConflict(taskDto, user, false);
        checkEngineStatus(taskDto, user);
        if (!taskDto.getShareCache()) {
                Map<String, List<Message>> validateMessage = taskDto.getDag().validate();
                if (!validateMessage.isEmpty()) {
                    throw new BizException(TASK_LIST_WARN_MESSAGE, validateMessage);
            }
        }
        //日志挖掘
        if (startFlag.charAt(0) == '1') {
            logCollectorService.logCollector(user, taskDto);
        }

        //打点任务，这个标识主要是防止任务跟子任务重复执行的
        if (startFlag.charAt(1) == '1') {
            logCollectorService.startConnHeartbeat(user, taskDto);
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
                    throw new BizException("SystemError", "Wait transformed schema timeout");
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
        ReentrantLock lock = scheduleLockMap.computeIfAbsent(user.getUserId(), k -> new ReentrantLock());
        lock.lock();

        try {
            if (settingsService.isCloud()) {
                CalculationEngineVo calculationEngineVo = taskScheduleService.cloudTaskLimitNum(taskDto, user, true);
                int runningNum = subCronOrPlanNum(taskDto, calculationEngineVo.getRunningNum());
                if (runningNum >= calculationEngineVo.getTaskLimit()) {
                    throw new BizException("Task.ScheduleLimit");
                }
            }
            StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.START, user);
            if (stateMachineResult.isFail()) {
                //如果更新失败，则表示可能为并发启动操作，本次不做处理
                log.info("concurrent start operations, this operation don‘t effective, task name = {}", taskDto.getName());
                return;
            }
            Query query = new Query(Criteria.where("id").is(taskDto.getId()).and(STATUS).is(taskDto.getStatus()));
            //需要将重启标识清除
            Update set = Update.update("isEdit", false)
                    .set(RESTART_FLAG, false)
                    .unset(FUNCTION_RETRY_STATUS)
                    .set(RESTART_FLAG, false)
                    .set(STOP_RETRY_TIMES, 0);
            update(query, set, user);
            taskScheduleService.scheduling(taskDto, user);
        } finally {
            lock.unlock();
            if (!lock.isLocked()) {
                scheduleLockMap.remove(user.getUserId());
            }
        }
    }

    /**
     * @see DataFlowEvent#SCHEDULE_SUCCESS
     * @param dto
     * @param status
     * @param userDetail
     */
    public void updateTaskRecordStatus(TaskDto dto, String status, UserDetail userDetail) {

        //对于重置是异步的，并且在并发情况下可能存在多条运行记录，如果在这里过滤掉重置状态。可以调整创建运行记录的时间点避免
        if (TaskDto.STATUS_RENEWING.equals(status)) {
            return;
        }

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
                    .userId(dto.getUserId())
                    .taskDto(dto)
                    .userDetail(userDetail)
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

    public void pause(ObjectId id, UserDetail user, boolean force, boolean restart, boolean system) {
        pause(id, user, force, restart);
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
        Field field = new Field();
        field.put(STATUS, true);
        TaskDto statusTask = findById(taskDto.getId(), field);
        taskDto.setStatus(statusTask.getStatus());
        if ((TaskDto.STATUS_STOP.equals(taskDto.getStatus()) || TaskDto.STATUS_STOPPING.equals(taskDto.getStatus())) && restart) {
            Update update = Update.update(RESTART_FLAG, true).set("restartUserId", user.getUserId());
            Query query = new Query(Criteria.where("_id").is(taskDto.getId()));
            update(query, update, user);
            return;
        }

        Update stopUpdate = Update.update(STOPED_DATE, System.currentTimeMillis());
        if (CollectionUtils.isNotEmpty(taskDto.getLdpNewTables())) {
            stopUpdate.set("ldpNewTables", taskDto.getLdpNewTables());
        }
        updateById(taskDto.getId(), stopUpdate, user);

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
            update.set(RESTART_FLAG, true).set("restartUserId", user.getUserId());
            Query query1 = new Query(Criteria.where("_id").is(taskDto.getId()));
            update(query1, update, user);
        }

        Update update = Update.update(STOP_RETRY_TIMES, 0);
        updateById(taskDto.getId(), update, user);


        sendStoppingMsg(taskDto.getId().toHexString(), taskDto.getAgentId(), user, force);
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
        TaskDto taskDto = checkExistById(id, user, "_id", STATUS, "name", TASK_RECORD_ID, START_TIME, SCHEDULE_DATE);
        //将子任务状态改成运行中
        if (!TaskDto.STATUS_WAIT_RUN.equals(taskDto.getStatus())) {
            log.info("concurrent runError operations, this operation don‘t effective, task name = {}", taskDto.getName());
            return null;
        }

        FunctionUtils.ignoreAnyError(() -> {
            String template = "Engine takeover task successful, cost {0}ms.";
            String msg = MessageFormat.format(template, System.currentTimeMillis() - taskDto.getScheduleDate());
            monitoringLogsService.startTaskErrorLog(taskDto, user, msg, Level.INFO);
        });

        Query query1 = new Query(Criteria.where("_id").is(taskDto.getId()));
        Update update = Update.update(SCHEDULE_DATE, null);

        StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.RUNNING, user);
        if (stateMachineResult.isFail()) {
            log.info("concurrent running operations, this operation don‘t effective, task name = {}", taskDto.getName());
            return null;
        }

        update(query1, update, user);
        return id.toHexString();
    }

    /**
     * 收到任务运行失败的消息
     * @see DataFlowEvent#ERROR
     * @param id
     */
    public String runError(ObjectId id, UserDetail user, String errMsg, String errStack) {
        //判断任务是否存在。
        TaskDto taskDto = checkExistById(id, user, "_id", STATUS, "name", TASK_RECORD_ID);

        //将子任务状态更新成错误.
        StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.ERROR, user);
        if (stateMachineResult.isFail()) {
            log.info("concurrent runError operations, this operation don‘t effective, task name = {}", taskDto.getName());
            return null;
        }

        return id.toHexString();

    }

    /**
     * 收到子任务运行完成的消息
     * @see DataFlowEvent#COMPLETED
     * @param id
     */
    public String complete(ObjectId id, UserDetail user) {
        //判断子任务是否存在
        TaskDto taskDto = checkExistById(id, user, "_id", STATUS, "name", TASK_RECORD_ID);

        StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.COMPLETED, user);
        if (stateMachineResult.isFail()) {
            log.info("concurrent complete operations, this operation don‘t effective, task name = {}", taskDto.getName());
            return null;
        }

        return id.toHexString();
    }

    /**
     * 收到子任务已经停止的消息
     * @see DataFlowEvent#STOPPED
     * @param id
     */
    public String stopped(ObjectId id, UserDetail user) {
        //判断子任务是否存在。
        TaskDto taskDto = checkExistById(id, user, "dag", "name", STATUS, "_id", TASK_RECORD_ID, AGENT_ID, STOPED_DATE, RESTART_FLAG);

        StateMachineResult stateMachineResult = stateMachineService.executeAboutTask(taskDto, DataFlowEvent.STOPPED, user);

        if (stateMachineResult.isFail()) {
            log.info("concurrent stopped operations, this operation don‘t effective, task name = {}", taskDto.getName());
            return null;
        } else {
            FunctionUtils.ignoreAnyError(() -> {
                String template = "Task has been stopped, total cost {0}ms.";
                String msg = MessageFormat.format(template, System.currentTimeMillis() - taskDto.getStopedDate());
                monitoringLogsService.startTaskErrorLog(taskDto, user, msg, Level.INFO);
            });

            Update update = Update.update(STOP_RETRY_TIMES, 0).unset(STOPED_DATE)
                    .unset(FUNCTION_RETRY_STATUS);
            updateById(id, update, user);

            logCollectorService.endConnHeartbeat(user, taskDto); // 尝试停止心跳任务
        }


        //对于需要重启的任务，直接拉起来。
        FunctionUtils.ignoreAnyError(() -> {
            if (taskDto.getResetFlag() != null && taskDto.getResetFlag()) {
                start(id, user);
            }
        });
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
        Criteria criteria = Criteria.where("_id").is(objectId).and(DAG_NODES).elemMatch(Criteria.where("id").is(nodeId));
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
        TaskDto TaskDto = checkExistById(taskId, user, ATTRS);
        clear(srcNode, tgtNode, user, TaskDto);

    }

    private void clear(String srcNode, String tgtNode, UserDetail user, TaskDto TaskDto) {
        Map<String, Object> attrs = TaskDto.getAttrs();
        Object syncProgress = attrs.get(SYNC_PROGRESS);
        if (syncProgress == null) {
            return;
        }

        Map syncProgressMap = (Map) syncProgress;
        List<String> key = Lists.newArrayList(srcNode, tgtNode);

        syncProgressMap.remove(JsonUtil.toJsonUseJackson(key));

        Update update = Update.update(ATTRS, attrs);
        //不需要刷新主任状态， 所以调用super, 本来中重新的自带刷新主任务状态
        super.updateById(TaskDto.getId(), update, user);
    }

    public void increaseBacktracking(ObjectId taskId, String srcNode, String tgtNode, TaskDto.SyncPoint point, UserDetail user) {
        TaskDto taskDto = checkExistById(taskId, user, "parentId", ATTRS, "dag", SYNC_POINTS);
        clear(srcNode, tgtNode, user, taskDto);


        //更新主任务中的syncPoints时间点
        DAG dag = taskDto.getDag();
        Node node = dag.getNode(tgtNode);
        if (node instanceof DataParentNode) {
            List<TaskDto.SyncPoint> syncPoints = taskDto.getSyncPoints();
            if (CollectionUtils.isEmpty(syncPoints)) {
                syncPoints = new ArrayList<>();
            }

            boolean exist = false;
            TaskDto.SyncPoint syncPoint = new TaskDto.SyncPoint();
            for (TaskDto.SyncPoint item : syncPoints) {
                if (node.getId().equals(item.getNodeId())) {
                    syncPoint = item;
                    exist = true;
                    break;
                }
            }
            syncPoint.setPointType(point.getPointType());
            syncPoint.setDateTime(point.getDateTime());
            syncPoint.setTimeZone(point.getTimeZone());
            syncPoint.setNodeId(node.getId());
            syncPoint.setNodeName(node.getName());
            syncPoint.setConnectionId(((DataParentNode<?>) node).getConnectionId());

            if (exist) {
                Criteria criteriaPoint = Criteria.where("_id").is(taskDto.getId()).and(SYNC_POINTS)
                        .elemMatch(Criteria.where("nodeId").is(node.getId()));
                Update update = Update.update("syncPoints.$", syncPoint);
                //更新内嵌文档
                update(new Query(criteriaPoint), update);
            } else {
                syncPoints.add(syncPoint);
                Criteria criteriaPoint = Criteria.where("_id").is(taskDto.getId());
                Update update = Update.update(SYNC_POINTS, syncPoints);
                update(new Query(criteriaPoint), update);
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
        Criteria migrateCriteria = Criteria.where(STATUS).is(TaskDto.STATUS_WAIT_START)
                .and(PLAN_START_DATE_FLAG).is(true)
                .and("crontabScheduleMsg").is(null)
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
							UserDetail userDetail = finalUserMap.get(taskDto.getUserId());
							try {
								start(taskDto, userDetail, "11");
								//启动过后，应该更新掉这个自动启动计划
								Update unset = new Update().unset(PLAN_START_DATE_FLAG).unset("planStartDate");
								updateById(taskDto.getId(), unset, finalUserMap.get(taskDto.getUserId()));
							} catch (Exception e) {
								log.warn("Start plan migrate task Failed: {}", e.getMessage(), e);
								stateMachineService.executeAboutTask(taskDto, DataFlowEvent.ERROR, userDetail);
								if (e instanceof TapCodeException) {
									monitoringLogsService.startTaskErrorStackTrace(taskDto, userDetail, e, Level.ERROR);
								} else {
									monitoringLogsService.startTaskErrorStackTrace(taskDto, userDetail, new BizException("Task.PlanStart.Failed", e), Level.ERROR);
								}
							}
						}
        }
    }

    public void startPlanCronTask() {
        Criteria migrateCriteria = Criteria.where(CRONTAB_EXPRESSION_FLAG).is(true)
                .and("type").in(ParentTaskDto.TYPE_INITIAL_SYNC, ParentTaskDto.TYPE_INITIAL_SYNC_CDC)
                .and("crontabExpression").exists(true)
                .and(IS_DELETED).is(false)
                .andOperator(Criteria.where(STATUS).nin(TaskDto.STATUS_EDIT,TaskDto.STATUS_STOPPING,
                        TaskDto.STATUS_RENEWING,TaskDto.STATUS_DELETING,TaskDto.STATUS_SCHEDULING,
                        TaskDto.STATUS_DELETE_FAILED));
        Query taskQuery = new Query(migrateCriteria);
        List<TaskDto> taskList = findAll(taskQuery);
        if (CollectionUtils.isNotEmpty(taskList)) {
            taskList = taskList.stream().filter(t -> Objects.nonNull(t.getTransformed()) && t.getTransformed())
                    .collect(Collectors.toList());
            log.info("Total cron task:{}", taskList.size());
            int error = 0;
            int success = 0;
            for (TaskDto taskDto : taskList) {
                try {
                    scheduleService.executeTask(taskDto);
                    success = success + 1;
                } catch (Exception e) {
                    log.error("Cron task error name:{},id:{}, msg:{}", taskDto.getName(), taskDto.getId(), e.getMessage());
                    error = error + 1;
                }
            }
            log.info("Success sum:{},error sum:{}", success, error);
        }
    }

    public TaskDto findByCacheName(String cacheName, UserDetail user) {
        Criteria taskCriteria = Criteria.where(DAG_NODES).elemMatch(Criteria.where(CATALOG).is("memCache").and("cacheName").is(cacheName));
        Query query = new Query(taskCriteria);

        return findOne(query, user);
    }

    public void updateDag(TaskDto taskDto, UserDetail user, boolean saveHistory) {
        TaskDto oldTask = checkExistById(taskDto.getId(), user);
        taskUpdateDagService.updateDag(taskDto, oldTask, user, saveHistory);
    }

    public TaskDto findByVersionTime(String id, Long time) {
        Criteria criteria = Criteria.where(TASK_ID).is(id);
        criteria.and(TM_CURRENT_TIME).is(time);

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
        Criteria criteria = Criteria.where(TASK_ID).is(taskId);
        criteria.and(TM_CURRENT_TIME).gt(time);

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
            throw new BizException(TASK_NOT_FOUND, "The task does not exist");
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

    public Chart6Vo chart6(UserDetail user) {
        Criteria criteria = Criteria.where(IS_DELETED).ne(true).and(SYNC_TYPE).in(TaskDto.SYNC_TYPE_SYNC, TaskDto.SYNC_TYPE_MIGRATE);
        Query query = new Query(criteria);
        query.fields().include("_id");
        List<TaskDto> allDto = findAllDto(query, user);
        List<String> ids = allDto.stream().map(a->a.getId().toHexString()).collect(Collectors.toList());

        List<MeasurementEntity>  allMeasurements = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(ids)) {
            ids.stream().forEach(id -> {
                MeasurementEntity measurement = measurementServiceV2.findLastMinuteByTaskId(id);
                if (measurement != null) {
                    allMeasurements.add(measurement);
                }
            });
        }

        BigInteger output = BigInteger.ZERO;
        BigInteger input = BigInteger.ZERO;
        BigInteger insert = BigInteger.ZERO;
        BigInteger update = BigInteger.ZERO;
        BigInteger delete = BigInteger.ZERO;

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
                    BigInteger inputInsertTotal = NumberUtil.parseDataTotal(vs.get("inputInsertTotal"));
                    BigInteger inputOthersTotal = NumberUtil.parseDataTotal(vs.get("inputOthersTotal"));
                    BigInteger inputDdlTotal = NumberUtil.parseDataTotal(vs.get("inputDdlTotal"));
                    BigInteger inputUpdateTotal = NumberUtil.parseDataTotal(vs.get("inputUpdateTotal"));
                    BigInteger inputDeleteTotal = NumberUtil.parseDataTotal(vs.get("inputDeleteTotal"));

                    BigInteger outputInsertTotal = NumberUtil.parseDataTotal(vs.get("outputInsertTotal"));
                    BigInteger outputOthersTotal = NumberUtil.parseDataTotal(vs.get("outputOthersTotal"));
                    BigInteger outputDdlTotal = NumberUtil.parseDataTotal(vs.get("outputDdlTotal"));
                    BigInteger outputUpdateTotal = NumberUtil.parseDataTotal(vs.get("outputUpdateTotal"));
                    BigInteger outputDeleteTotal = NumberUtil.parseDataTotal(vs.get("outputDeleteTotal"));
                    output = output.add(outputInsertTotal);
                    output = output.add(outputOthersTotal);
                    output = output.add(outputDdlTotal);
                    output = output.add(outputUpdateTotal);
                    output = output.add(outputDeleteTotal);

                    input = input.add(inputInsertTotal);
                    input = input.add(inputOthersTotal);
                    input = input.add(inputDdlTotal);
                    input = input.add(inputUpdateTotal);
                    input = input.add(inputDeleteTotal);

                    insert = insert.add(inputInsertTotal);
                    update = update.add(inputUpdateTotal);
                    delete = delete.add(inputDeleteTotal);

                }
            }
        }


        Chart6Vo chart6Vo = Chart6Vo.builder().outputTotal(output).inputTotal(input)
                .insertedTotal(insert).updatedTotal(update).deletedTotal(delete)
                .build();
        return chart6Vo;
    }

    public void stopTaskIfNeedByAgentId(String agentId, UserDetail userDetail) {
        Query query = Query.query(Criteria.where(AGENT_ID).is(agentId).and(STATUS).is(TaskDto.STATUS_STOPPING));
        query.fields().include("_id");
        List<TaskDto> needStopTasks = findAllDto(query, userDetail);
        for (TaskDto needStopTask : needStopTasks) {
            stopped(needStopTask.getId(), userDetail);
        }
    }


    public List<TaskDto> getTaskStatsByTableNameOrConnectionId(String connectionId, String tableName, UserDetail userDetail) {
        if (StringUtils.isBlank(connectionId)) {
            throw new BizException(ILLEGAL_ARGUMENT, CONNECTION_ID);
        }
        Criteria criteria = new Criteria();
        // tableName 不为空根据表查询。否则根据连接查询
        criteria.and(DAG_NODES_CONNECTION_ID).is(connectionId).and(IS_DELETED).ne(true);
        if (StringUtils.isNotBlank(tableName)) {
            criteria.orOperator(new Criteria().and("dag.nodes.tableName").is(tableName),
                    new Criteria().and("dag.nodes.syncObjects.objectNames").is(tableName)
            );
        }
        Query query = Query.query(criteria);
        return findAllDto(query,userDetail);
    }

    public TableStatusInfoDto getTableStatus(String connectionId, String tableName, UserDetail userDetail) {
        if (StringUtils.isBlank(connectionId)) {
            throw new BizException(ILLEGAL_ARGUMENT, CONNECTION_ID);
        }
        if (StringUtils.isBlank(tableName)) {
            throw new BizException(ILLEGAL_ARGUMENT, TABLE_NAME);
        }
        TableStatusInfoDto tableStatusInfoDto = new TableStatusInfoDto();
        Criteria criteria = new Criteria();
        // tableName 不为空根据表查询。否则根据连接查询
        criteria.and(DAG_NODES_CONNECTION_ID).is(connectionId).and(IS_DELETED).ne(true);
        criteria.orOperator(new Criteria().and("dag.nodes.tableName").is(tableName),
                new Criteria().and("dag.nodes.tableNames").in(tableName),
                new Criteria().and("dag.nodes.syncObjects.objectNames").in(tableName));
        Query query = Query.query(criteria);
        List<TaskDto> list = findAll(query);
        String taskSuccessStatus = "";
        String taskErrorStatus = "";
        String taskEditStatus = "";
        boolean exist = false;
        String tableStatus="";
        String taskId="";
        for (TaskDto taskDto : list) {
            if (judgeTargetNode(taskDto, tableName)) {
                exist = true;
                if (TaskDto.STATUS_RUNNING.equals(taskDto.getStatus())
                        || TaskDto.STATUS_COMPLETE.equals(taskDto.getStatus())) {
                    taskSuccessStatus =TableStatusEnum.STATUS_NORMAL.getValue();
                    taskId= String.valueOf(taskDto.getId());
                    break;
                } else {
                    if (StringUtils.equalsAny(taskDto.getStatus(), TaskDto.STATUS_EDIT, TaskDto.STATUS_WAIT_START, TaskDto.STATUS_WAIT_RUN,
                            TaskDto.STATUS_SCHEDULING)) {
                        taskEditStatus = TableStatusEnum.STATUS_DRAFT.getValue();
                    } else {
                        taskErrorStatus = TableStatusEnum.STATUS_ERROR.getValue();
                    }
                }
            }
        }
        if (!exist) {
            tableStatus=TableStatusEnum.STATUS_DRAFT.getValue();
            tableStatusInfoDto.setStatus(tableStatus);
            return tableStatusInfoDto;
        }
        if (StringUtils.isNotEmpty(taskSuccessStatus)) {
            if(judgeTargetInspect(connectionId, tableName, userDetail)){
                tableStatus=  TableStatusEnum.STATUS_NORMAL.getValue();
                measurementServiceV2.queryTableMeasurement(taskId,tableStatusInfoDto);
            }else {
                tableStatus = TableStatusEnum.STATUS_ERROR.getValue();
            }
            tableStatusInfoDto.setStatus(tableStatus);
            return tableStatusInfoDto;
        }
        tableStatus =  StringUtils.isNotEmpty(taskErrorStatus) ? taskErrorStatus : taskEditStatus;
        tableStatusInfoDto.setStatus(tableStatus);
        return tableStatusInfoDto;
    }


    public boolean judgeTargetInspect(String connectionId, String tableName, UserDetail userDetail) {
        Criteria criteriaInspect = new Criteria();
        // tableName 不为空根据表查询。否则根据连接查询
        criteriaInspect.and("stats.target.connectionId").is(connectionId);
        criteriaInspect.and("stats.target.table").is(tableName);
        Query queryInspect = Query.query(criteriaInspect);
        queryInspect.with(Sort.by(CREATE_TIME).descending());
        InspectResultDto inspectResultDto = inspectResultService.findOne(queryInspect,userDetail);
        if (inspectResultDto == null) {
            return true;
        }
        List<Stats> statsList = inspectResultDto.getStats();
        if (CollectionUtils.isNotEmpty(statsList)) {
            for (Stats stats : statsList) {
                Source target = stats.getTarget();
                if (connectionId.equals(target.getConnectionId()) && tableName.equals(target.getTable())) {
                    if (StringUtils.equalsAny(stats.getStatus(), InspectStatusEnum.FAILED.name(),
                            InspectStatusEnum.ERROR.name())) {
                        return false;
                    } else if (StringUtils.equalsAny(stats.getStatus(), InspectStatusEnum.DONE.name(),
                            InspectStatusEnum.PASSED.name())) {
                        return InspectResultEnum.PASSED.name().equals(stats.getResult());
                    } else {
                        return true;
                    }
                }
            }

        }
        return false;
    }


    public boolean judgeTargetNode(TaskDto taskDto, String tableName) {
        List<Edge> edges = taskDto.getDag().getEdges();
        if (CollectionUtils.isNotEmpty(edges)) {
            for (Edge edge : edges) {
                String target = edge.getTarget();
                List<Node> nodeList = taskDto.getDag().getNodes();
                if (CollectionUtils.isNotEmpty(nodeList)) {
                    for (Node node : nodeList) {
                        if (node instanceof DatabaseNode) {
                            boolean tableNameExist = false;
                            DatabaseNode nodeTemp = (DatabaseNode) node;
                            if (nodeTemp.getSyncObjects() != null) {
                                for (SyncObjects syncObjects : nodeTemp.getSyncObjects()) {
                                    if (syncObjects.getObjectNames().contains(tableName)) {
                                        tableNameExist = true;
                                        break;
                                    }
                                }
                            }
                            if (((CollectionUtils.isNotEmpty(nodeTemp.getTableNames()) && nodeTemp.getTableNames().contains(tableName))
                                    || tableNameExist)
                                    && node.getId().equals(target)) {
                                return true;
                            }
                        } else if (node instanceof TableNode) {
                            TableNode tableNode = (TableNode) node;
                            if (StringUtils.isNotEmpty(tableNode.getTableName()) &&
                                    tableNode.getTableName().equals(tableName)
                                    && node.getId().equals(target)) {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        return false;
    }


    public List<TaskDto> findHeartbeatByConnectionId(String connectionId, String... includeFields) {
        Query query = Query.query(Criteria.where(DAG_NODES_CONNECTION_ID).is(connectionId)
                .and(SYNC_TYPE).is(TaskDto.SYNC_TYPE_CONN_HEARTBEAT)
                .and(IS_DELETED).is(false)
        );
        if (null != includeFields && includeFields.length > 0) {
            query.fields().include(includeFields);
        }

        return findAll(query);
    }

    public TaskDto findHeartbeatByTaskId(String taskId, String... includeFields) {
        Query query = Query.query(Criteria.where(ConnHeartbeatUtils.TASK_RELATION_FIELD).is(taskId)
                .and(SYNC_TYPE).is(TaskDto.SYNC_TYPE_CONN_HEARTBEAT)
                .and(IS_DELETED).is(false)
        );
        if (null != includeFields && includeFields.length > 0) {
            query.fields().include(includeFields);
        }

        return findOne(query);
    }

    public int deleteHeartbeatByConnId(UserDetail user, String connId) {
        int deleteSize = 0;
        List<TaskDto> heartbeatTasks = findHeartbeatByConnectionId(connId, "_id", STATUS, IS_DELETED);
        if (null != heartbeatTasks) {
            TaskDto statusDto;
            for (TaskDto dto : heartbeatTasks) {
                statusDto = dto;
                do {
                    if (TaskDto.STATUS_RUNNING.equals(statusDto.getStatus())) {
                        pause(statusDto.getId(), user, false);
                    } else if (!TaskDto.STATUS_STOPPING.equals(statusDto.getStatus())) {
                        break;
                    }
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        throw new RuntimeException("Delete heartbeat task failed");
                    }
                    statusDto = findByTaskId(dto.getId(), STATUS);
                } while (null != statusDto);

                remove(dto.getId(), user);
                deleteSize++;
            }
        }
        return deleteSize;
    }


    public int runningTaskNum(String processId, UserDetail userDetail) {
        long workNum = count(Query.query(Criteria.where(AGENT_ID).is(processId)
                .and(IS_DELETED).ne(true).and(SYNC_TYPE).in(TaskDto.SYNC_TYPE_SYNC, TaskDto.SYNC_TYPE_MIGRATE)
                .and(STATUS).nin(TaskDto.STATUS_DELETE_FAILED,TaskDto.STATUS_DELETING)
                        .orOperator(Criteria.where(STATUS).in(TaskDto.STATUS_RUNNING, TaskDto.STATUS_SCHEDULING, TaskDto.STATUS_WAIT_RUN),
                                Criteria.where(PLAN_START_DATE_FLAG).is(true),
                                Criteria.where(CRONTAB_EXPRESSION_FLAG).is(true)
                        )), userDetail);
        return (int) workNum;
    }

    @Override
    public TaskEntity convertToEntity(Class entityClass, BaseDto dto, String... ignoreProperties) {
        if (entityClass == null || dto == null)
            return null;
        try {
            TaskEntity entity = new TaskEntity();
            BeanUtils.copyProperties(dto, entity, AGENT_ID, START_TIME, "lastStartDate", "shareCdcStop", "shareCdcStopMessage");
            return entity;
        } catch (Exception e) {
            log.error("Convert entity " + entityClass + " failed. {}", ThrowableUtils.getStackTraceByPn(e));
        }
        return null;
    }

    @Override
    public <T extends BaseDto> T convertToDto(TaskEntity entity, Class<T> dtoClass, String... ignoreProperties) {
        T dto = super.convertToDto(entity, dtoClass, "shareCdcStopMessage");
        try {
            if (dto instanceof TaskDto) {
                TaskDto taskDto = (TaskDto) dto;
                if (null != entity.getShareCdcStop() && entity.getShareCdcStop() && StringUtils.isNotBlank(entity.getShareCdcStopMessage())) {
                    taskDto.setShareCdcStopMessage(MessageUtil.getMessage(entity.getShareCdcStopMessage()));
                }
            }
        } catch (Exception e) {
            log.error("Convert task entity to dto failed, try to use super method: {}", e.getMessage(), e);
            return super.convertToDto(entity, dtoClass, ignoreProperties);
        }
        return dto;
    }
    public int findRunningTasksByAgentId(String processId){
        if (StringUtils.isBlank(processId.trim())) throw new IllegalArgumentException("process id can not be empty");
        Query query = Query.query(Criteria.where(AGENT_ID).is(processId).and(STATUS).is("running"));
        List<TaskDto> runningTasks = findAll(query);
        return runningTasks.size();
    }

    public int runningTaskNum(UserDetail userDetail) {
        long workNum = count(Query.query(Criteria.where(IS_DELETED).ne(true)
                .and(SYNC_TYPE).in(TaskDto.SYNC_TYPE_SYNC, TaskDto.SYNC_TYPE_MIGRATE)
                .and(STATUS).nin(TaskDto.STATUS_DELETE_FAILED,TaskDto.STATUS_DELETING)
                .orOperator(Criteria.where(STATUS).in(TaskDto.STATUS_RUNNING, TaskDto.STATUS_SCHEDULING, TaskDto.STATUS_WAIT_RUN),
                        Criteria.where(PLAN_START_DATE_FLAG).is(true),
                        Criteria.where(CRONTAB_EXPRESSION_FLAG).is(true)
                )), userDetail);
        return (int) workNum;
    }

    public boolean checkCloudTaskLimit(ObjectId taskId,UserDetail user,boolean checkCurrentTask){
        if (settingsService.isCloud()) {
            TaskDto task = findByTaskId(taskId,"id",AGENT_ID,"agentTags");
            CalculationEngineVo calculationEngineVo = workerService.calculationEngine(task, user, null);
            int runningNum;
            if(checkCurrentTask){
                runningNum  = subCronOrPlanNum(task, calculationEngineVo.getRunningNum());
            }else{
                runningNum = calculationEngineVo.getRunningNum();
            }
            if (runningNum >= calculationEngineVo.getTaskLimit()) {
                return false;
            }
        }
        return true;
    }

    protected void checkUnwindProcess(DAG dag){
        if(CollectionUtils.isEmpty(dag.getNodes()))return;
        AtomicBoolean check = new AtomicBoolean(false);
        dag.getNodes().forEach(node -> {
            if(node instanceof UnwindProcessNode) check.set(true);
        });
        if(check.get()){
            dag.getNodes().forEach(node -> {
                if(node instanceof TableNode && ((TableNode)node).getDmlPolicy() != null){
                    ((TableNode)node).getDmlPolicy().setInsertPolicy(DmlPolicyEnum.just_insert);
                }
            });
        }
    }
}
