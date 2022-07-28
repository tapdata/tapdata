package com.tapdata.tm.task.service;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.lang.Assert;
import cn.hutool.core.map.MapUtil;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Maps;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.CustomerJobLogs.CustomerJobLog;
import com.tapdata.tm.CustomerJobLogs.service.CustomerJobLogsService;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.base.dto.BaseDto;
import com.tapdata.tm.commons.dag.*;
import com.tapdata.tm.commons.dag.nodes.*;
import com.tapdata.tm.commons.dag.process.JoinProcessorNode;
import com.tapdata.tm.commons.dag.process.MergeTableNode;
import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
import com.tapdata.tm.commons.dag.vo.FieldInfo;
import com.tapdata.tm.commons.dag.vo.Operation;
import com.tapdata.tm.commons.dag.vo.SyncObjects;
import com.tapdata.tm.commons.dag.vo.TableFieldInfo;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.schema.MetadataTransformerDto;
import com.tapdata.tm.commons.schema.MetadataTransformerItemDto;
import com.tapdata.tm.commons.schema.*;
import com.tapdata.tm.commons.task.dto.*;
import com.tapdata.tm.commons.task.dto.migrate.MigrateTableDto;
import com.tapdata.tm.commons.util.CapitalizedEnum;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.file.service.FileService;
import com.tapdata.tm.inspect.constant.InspectResultEnum;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectService;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.monitor.entity.MeasurementEntity;
import com.tapdata.tm.monitor.service.MeasurementService;
import com.tapdata.tm.task.bean.FullSyncVO;
import com.tapdata.tm.task.bean.LogCollectorResult;
import com.tapdata.tm.task.bean.TaskUpAndLoadDto;
import com.tapdata.tm.task.constant.SubTaskEnum;
import com.tapdata.tm.task.constant.SubTaskOpStatusEnum;
import com.tapdata.tm.task.constant.SyncType;
import com.tapdata.tm.task.constant.TaskStatusEnum;
import com.tapdata.tm.task.entity.SubTaskEntity;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.param.SaveShareCacheParam;
import com.tapdata.tm.task.repository.TaskRepository;
import com.tapdata.tm.task.vo.ShareCacheDetailVo;
import com.tapdata.tm.task.vo.ShareCacheVo;
import com.tapdata.tm.task.vo.TaskDetailVo;
import com.tapdata.tm.transform.service.MetadataTransformerItemService;
import com.tapdata.tm.transform.service.MetadataTransformerService;
import com.tapdata.tm.utils.*;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @Author:
 * @Date: 2021/11/03
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class TaskService extends BaseService<TaskDto, TaskEntity, ObjectId, TaskRepository> {

    private SubTaskService subTaskService;
    private MessageService messageService;
    private SnapshotEdgeProgressService snapshotEdgeProgressService;
    private MeasurementService measurementService;
    private InspectService inspectService;
    private TaskRunHistoryService taskRunHistoryService;
    private TaskStartService taskStartService;
    private TransformSchemaAsyncService transformSchemaAsyncService;
    private TransformSchemaService transformSchemaService;
    private CustomerJobLogsService customerJobLogsService;
    private DataSourceService dataSourceService;
    private MetadataTransformerService transformerService;
    private MetadataInstancesService metadataInstancesService;
    private MetadataTransformerItemService metadataTransformerItemService;
    private FileService fileService1;
    private MongoTemplate mongoTemplate;

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
        log.debug("The save task is complete and the subtask will be processed, task name = {}", taskDto.getName());
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

        if (rename) {
            subTaskService.rename(taskDto.getId(), taskDto.getName());
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

        // 自动处理ddl
        taskDto.setIsOpenAutoDDL(getBoolValue(taskDto.getIsOpenAutoDDL(), true));

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
        boolean rename = false;
        if (StringUtils.isNotBlank(taskDto.getName()) && !taskDto.getName().equals(oldTaskDto.getName())) {
            checkTaskName(taskDto.getName(), user, taskDto.getId());
            rename = true;
        }

        //校验dag
        DAG dag = taskDto.getDag();
        if (dag != null) {
            if (TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
                if (CollectionUtils.isNotEmpty(dag.getSourceNode())) {
                    //supplier migrate tableSelectType=all tableNames and SyncObjects
                    DatabaseNode sourceNode = dag.getSourceNode().get(0);
                    if (Objects.nonNull(sourceNode) && CollectionUtils.isEmpty(sourceNode.getTableNames())
                            && StringUtils.equals("all", sourceNode.getMigrateTableSelectType())) {
                        String connectionId = sourceNode.getConnectionId();
                        List<MetadataInstancesDto> metaList = metadataInstancesService.findBySourceIdAndTableNameList(connectionId, null, user);
                        if (CollectionUtils.isNotEmpty(metaList)) {
                            List<String> collect = metaList.stream().map(MetadataInstancesDto::getOriginalName).collect(Collectors.toList());
                            sourceNode.setTableNames(collect);
                            Dag temp = new Dag(dag.getEdges(), dag.getNodes());
                            dag = DAG.build(temp);
                        }
                    }

                    // supplement migrate_field_rename_processor fieldMapping data
                    supplementMigrateFieldMapping(dag, user);

                    transformSchemaAsyncService.transformSchema(dag, user, taskDto.getId());
                }
            } else {
                transformSchemaService.transformSchema(dag, user, taskDto.getId());
            }
        }
        log.debug("check task dag complete, task id =- {}", taskDto.getId());

        String editVersion = buildEditVersion(taskDto);
        taskDto.setEditVersion(editVersion);

//        if (!TaskDto.STATUS_EDIT.equals(oldTaskDto.getStatus())) {
//            taskDto.setTemp(taskDto.getDag());
//            taskDto.setDag(oldTaskDto.getDag());
//        } else {
//            log.debug("update task and clear temp,  task dto = {}", taskDto);
//
//            TaskDto taskDto1 = saveAndClearTemp(taskDto, user);
//            if (rename) {
//                subTaskService.rename(taskDto1.getId(), taskDto1.getName());
//            }
//            return taskDto1;
//        }

        //更新任务
        log.debug("update task, task dto = {}", taskDto);
        TaskDto save = save(taskDto, user);
        if (rename) {
            subTaskService.rename(save.getId(), save.getName());
        }
        return save;

    }

    private void supplementMigrateFieldMapping(DAG dag, UserDetail userDetail) {
        dag.getNodes().forEach(node -> {
            if (node instanceof MigrateFieldRenameProcessorNode) {
                MigrateFieldRenameProcessorNode fieldNode = (MigrateFieldRenameProcessorNode) node;
                LinkedList<TableFieldInfo> fieldsMapping = fieldNode.getFieldsMapping();

                if (CollectionUtils.isNotEmpty(fieldsMapping)) {
                    List<String> tableNames = fieldsMapping.stream()
                            .map(TableFieldInfo::getOriginTableName)
                            .collect(Collectors.toList());
                    DatabaseNode sourceNode = dag.getNodes().stream()
                            .filter(n -> n instanceof DatabaseNode)
                            .map(n -> (DatabaseNode) n)
                            .collect(Collectors.toCollection(LinkedList::new))
                            .stream()
                            .filter(n -> dag.getSources().contains(n))
                            .findAny().orElse(new DatabaseNode());

                    List<MetadataInstancesDto> metaList = metadataInstancesService.findBySourceIdAndTableNameList(sourceNode.getConnectionId(), tableNames, userDetail);
                    Map<String, List<com.tapdata.tm.commons.schema.Field>> fieldMap = metaList.stream()
                            .collect(Collectors.toMap(MetadataInstancesDto::getQualifiedName, MetadataInstancesDto::getFields));
                    fieldsMapping.forEach(table -> {
                        Operation operation = table.getOperation();
                        LinkedList<FieldInfo> fields = table.getFields();

                        List<String> fieldNames = Lists.newArrayList();
                        if (CollectionUtils.isNotEmpty(fields)) {
                            fieldNames = fields.stream().map(FieldInfo::getSourceFieldName).collect(Collectors.toList());
                        }
                        List<com.tapdata.tm.commons.schema.Field> tableFields = fieldMap.get(table.getQualifiedName());
                        if (CollectionUtils.isNotEmpty(tableFields)) {
                            List<String> finalFieldNames = fieldNames;
                            tableFields.forEach(field -> {
                                String targetFieldName = field.getFieldName();
                                if (!finalFieldNames.contains(targetFieldName)) {
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
                                    FieldInfo fieldInfo =
                                            new FieldInfo(field.getFieldName(), targetFieldName, true, "system");
                                    fields.add(fieldInfo);
                                }
                            });
                        }
                    });
                }
            }
        });
    }


    public TaskDto updateShareCacheTask(String id, SaveShareCacheParam saveShareCacheParam, UserDetail user) {
        TaskDto taskDto = findById(MongoUtils.toObjectId(id));
        parseCacheToTaskDto(saveShareCacheParam, taskDto);
/*        taskDto.setType(ParentTaskDto.TYPE_CDC);
        List<Node> nodes= taskDto.getDag().getNodes();
        List<Edge> edges= taskDto.getDag().getEdges();
        if (CollectionUtils.isNotEmpty(nodes)){
            Node sourceNode=nodes.get(0);
            Node targetNode=nodes.get(1);
            Edge edge=new Edge();
            edge.setSource(sourceNode.getId());
            edge.setTarget(targetNode.getId());
            edges.add(edge);
        }*/

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
        taskDto = confirmById(taskDto, user, confirm, false);
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

        //saveInspect(existedTask, taskDto, user);
        return confirmById(taskDto, user, confirm, false);
    }

    public void checkTaskInspectFlag (TaskDto taskDto) {
        if (taskDto.isAutoInspect() && !taskDto.isCanOpenInspect()) {
            throw new BizException("Task.CanNotSupportInspect");
        }
    }

    private void saveInspect(TaskDto existedTask, TaskDto taskDto, UserDetail userDetail) {
        try {
            ObjectId taskId = taskDto.getId();
            if (isInspectPropertyChanged(existedTask, taskDto)) {
                inspectService.deleteByTaskId(taskId.toString());
                inspectService.saveInspect(taskDto, userDetail);
            }
        } catch (Exception e) {
            log.error("新建校验任务出错", e);
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

    public TaskDto confirmById(TaskDto taskDto, UserDetail user, boolean confirm, boolean importTask) {
        DAG dag = taskDto.getDag();
        // check task flow engine available
        dataSourceService.checkAccessNodeAvailable(taskDto.getAccessNodeType(), taskDto.getAccessNodeProcessIdList(), user);

        if (!taskDto.getShareCache()) {
            if (!importTask) {
                Map<String, List<Message>> validateMessage = dag.validate();
                if (!validateMessage.isEmpty()) {
                    throw new BizException("Task.ListWarnMessage", validateMessage);
                }
            }
        }

        taskDto = updateById(taskDto, user);

        List<SubTaskDto> subTaskDtos = subTaskService.findByTaskId(taskDto.getId());

        List<DAG> newDags = dag.split();
        CustomerJobLog customerJobLog = new CustomerJobLog(taskDto.getId().toString(), taskDto.getName());
        customerJobLog.setDataFlowType(CustomerJobLogsService.DataFlowType.sync.getV());
        customerJobLog.setJobInfos(printInfos(dag));
        customerJobLogsService.splittedJobs(customerJobLog, user);

        log.debug("check task dag complete, task id = {}", taskDto.getId());

        Boolean isOpenAutoDDL = taskDto.getIsOpenAutoDDL();

        if (CollectionUtils.isEmpty(subTaskDtos)) {
            //拆分子任务
            int index = 1;
            for (DAG subDag : newDags) {
                SubTaskDto subTaskDto = new SubTaskDto();
                subTaskDto.setParentId(taskDto.getId());
                subTaskDto.setName(getSubTaskName(taskDto, index, user));
                subTaskDto.setStatus(SubTaskDto.STATUS_EDIT);
                subTaskDto.setIsEdit(true);
                subTaskDto.setDag(subDag);
                subTaskDtos.add(subTaskDto);
                index++;

                //设置子任务isOpenAutoDDL 属性，与父任务保持一致
                subTaskDto.setIsOpenAutoDDL(isOpenAutoDDL);
            }

            //子任务入库
            List<SubTaskDto> subTaskSaves = subTaskService.save(subTaskDtos, user);
            log.debug("handle subtask is complete, will be save");


            taskDto = saveAndClearTemp(taskDto, user);

            Map<String, Object> attrs = taskDto.getAttrs();
            if (attrs == null) {
                attrs = new HashMap<>();
                taskDto.setAttrs(attrs);
            }
            SubTaskDto subTaskDto = subTaskSaves.get(0);
            if (subTaskDto != null) {
                attrs.put(LOG_COLLECTOR_SAVE_ID, subTaskSaves.get(0).getId().toHexString());
            }

            return taskDto;
        }


        //合并子任务
        List<DAG.SubTaskStatus> dags = dag.update(newDags, subTaskDtos);
        Map<ObjectId, SubTaskDto> subTaskDtoMap = subTaskDtos.stream().collect(Collectors.toMap(SubTaskDto::getId, s -> s));

        List<DAG.SubTaskStatus> deleteSubTasks = dags.stream().filter(s -> "delete".equals(s.getAction())).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(deleteSubTasks)) {
            for (DAG.SubTaskStatus deleteSubTask : deleteSubTasks) {
                SubTaskDto subTaskDto = subTaskDtoMap.get(deleteSubTask.getSubTaskId());
                if (SubTaskService.runningStatus.contains(subTaskDto.getStatus())) {
                    throw new BizException("Task.DeleteSubTaskIsRun");
                }
            }
        }

        Map<String, List<Message>> messageMap = new HashMap<>();
        if (!confirm) {
            if (CollectionUtils.isNotEmpty(deleteSubTasks)) {
                for (DAG.SubTaskStatus deleteSubTask : deleteSubTasks) {
                    SubTaskDto subTaskDto = subTaskDtoMap.get(deleteSubTask.getSubTaskId());
                    if (subTaskDto.getIsEdit() == null || subTaskDto.getIsEdit()) {
                        continue;
                    }
                    Message message = new Message();
                    message.setCode("Task.DeleteSubTask");
                    List<Message> messageList = new ArrayList<>();
                    messageList.add(message);
                    messageMap.put(deleteSubTask.getSubTaskId().toString(), messageList);
                }
            }


            List<DAG.SubTaskStatus> updateSubTasks = dags.stream().filter(s -> "update".equals(s.getAction())).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(updateSubTasks)) {
                for (DAG.SubTaskStatus updateSubtask : updateSubTasks) {
                    SubTaskDto subTaskDto = subTaskDtoMap.get(updateSubtask.getSubTaskId());
                    if (subTaskDto.getIsEdit() == null || subTaskDto.getIsEdit()) {
                        continue;
                    }
                    boolean canHotUpdate = subTaskService.canHotUpdate(subTaskDto.getDag(), updateSubtask.getDag());
                    if (!canHotUpdate) {
                        throw new BizException("Task.UpdateSubTask");
                    }
                }
            }

        }

        //推合并的子任务进行新增更新删除
        updateSubtask(taskDto, user, dags);

        //更新任务
        log.debug("update task, task dto = {}", taskDto);
        TaskDto taskDto1 = saveAndClearTemp(taskDto, user);
        if (messageMap.size() > 0) {
            //返回异常
            throw new BizException("Task.ListWarnMessage", messageMap);
        }

        return taskDto1;

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

    private TaskDto saveAndClearTemp(TaskDto taskDto, UserDetail user) {
        TaskEntity taskEntity = convertToEntity(TaskEntity.class, taskDto);
        Update update1 = repository.buildUpdateSet(taskEntity);
        update1.set("temp", null);
        updateById(taskDto.getId(), update1, user);
        flushStatus(taskDto, user);
        return findById(taskDto.getId());
    }

    /**
     * 删除任务
     *
     * @param id   任务id
     * @param user 用户
     */
    public void remove(ObjectId id, UserDetail user) {
        //查询任务是否存在。
        //查询任务状态是否为停止状态。
        TaskDto taskDto = checkExistById(id, user);

        String status = taskDto.getStatus();

        List<SubTaskDto> subTaskDtos = subTaskService.findByTaskId(id, user);

        if (CollectionUtils.isNotEmpty(subTaskDtos)) {
            for (SubTaskDto subTaskDto : subTaskDtos) {
                if (SubTaskService.runningStatus.contains(subTaskDto.getStatus())) {
                    log.warn("task status is not stop, task id = {}", taskDto.getId());
                    throw new BizException("Task.statusIsNotStop", "task status is not stop");
                }
                //删除子任务
                //subTaskService.renew(subTaskDto, user);
                subTaskService.deleteById(subTaskDto, user);
            }
        } else {
            if (!TaskDto.STATUS_EDIT.equals(status)) {
                log.warn("task status is not stop, task id = {}", taskDto.getId());
                throw new BizException("Task.statusIsNotStop", "task status is not stop");
            }
        }

        //将任务删除标识改成true
        update(new Query(Criteria.where("_id").is(id)), Update.update("is_deleted", true));

        //add message
        if (SyncType.MIGRATE.getValue().equals(taskDto.getSyncType())) {
            messageService.addMigration(taskDto.getName(), taskDto.getId().toString(), MsgTypeEnum.DELETED, Level.WARN, user);
        } else if (SyncType.SYNC.getValue().equals(taskDto.getSyncType())) {
            messageService.addSync(taskDto.getName(), taskDto.getId().toString(), MsgTypeEnum.DELETED, "", Level.WARN, user);
        }

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
            stop(id, user, true);
        } catch (Exception e) {
            log.error("停止异常，但是共享缓存仍然删除", e);
        }
        //将任务删除标识改成true
        update(new Query(Criteria.where("_id").is(id)), Update.update("is_deleted", true));
//        remove(id, user);
    }


    public void flushStatus(ObjectId id, UserDetail user) {
//        CompletableFuture.allOf(CompletableFuture.runAsync(() -> {
//            TaskDto taskDto = checkExistById(id, user);
//            flushStatus(taskDto, user);
//        }, completableFutureThreadPool));
        TaskDto taskDto = checkExistById(id, user);
        flushStatus(taskDto, user);
    }

    private void flushStatus(TaskDto taskDto, UserDetail user) {
        try {

            List<SubTaskDto> subTaskDtos = subTaskService.findByTaskId(taskDto.getId(), "status");
            //如果没有子任务。则任务状态为状态字段的值
            List<SubStatus> statuses = new ArrayList<>();
            if (CollectionUtils.isNotEmpty(subTaskDtos)) {
                statuses = subTaskDtos.stream().map(s -> new SubStatus(s.getId().toString(), s.getName(), null, s.getStatus())).collect(Collectors.toList());
            }
            Update update = Update.update("statuses", statuses);
            taskDto.setStatuses(statuses);
            //updateById(taskDto.getId(), update, user);

            if (subTaskDtos.size() == 1) {
                String status = subTaskDtos.get(0).getStatus();
                update.set("status", status);
                taskDto.setStatus(status);
                updateById(taskDto.getId(), update, user);
                return;
            }

            List<String> statues = subTaskDtos.stream().map(SubTaskDto::getStatus).collect(Collectors.toList());
            //如果所有的子任务都是编辑中，则主任务为编辑中
            for (String status : statues) {
                if (!SubTaskDto.STATUS_EDIT.equals(status)) {
                    break;
                }
                update.set("status", TaskDto.STATUS_EDIT);
                updateById(taskDto.getId(), update, user);
                return;
            }

            //存在子任务处于调度中，待运行，运行中，停止中，暂停中，则设置任务为运行中
            for (String status : statues) {
                if (SubTaskService.runningStatus.contains(status)) {
                    update.set("status", TaskDto.STATUS_RUNNING);
                    taskDto.setStatus(status);
                    updateById(taskDto.getId(), update, user);
                    return;
                }
            }

            //所有子任务处于已暂停，任务状态为已暂停
//        for (String status : statues) {
//            if (!SubTaskDto.STATUS_PAUSE.equals(status)) {
//                break;
//            }
//            update.set("status", TaskDto.STATUS_PAUSE);
//            updateById(id, update, user);
//            return;
//        }

            //所有的子任务都是已完成，错误，已停止，调度失败, 编辑中状态，任务的状态为已停止。
            for (String status : statues) {
                if (!SubTaskService.stopStatus.contains(status)) {
                    break;
                }
                update.set("status", TaskDto.STATUS_STOP);
                taskDto.setStatus(status);
                updateById(taskDto.getId(), update, user);
                return;
            }

            update.set("status", TaskDto.STATUS_EDIT);
            taskDto.setStatus(TaskDto.STATUS_EDIT);
            updateById(taskDto.getId(), update, user);
        } catch (Exception e) {
            log.warn("refresh task statuses error, task name = {}", taskDto == null ? "" : taskDto.getName());
        }
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

        //将任务id设置为null,状态改为编辑中
        taskDto.setId(null);

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
        //taskDto.setTemp(null);

        //创建新任务， 直接调用事务不会生效
        TaskService taskService = SpringContextHelper.getBean(TaskService.class);

        log.info("create new task, task = {}", taskDto);
        checkDagAgentConflict(taskDto, false);
        taskDto = taskService.confirmById(taskDto, user, true, true);
        //taskService.flushStatus(taskDto, user);
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
     * 启动任务
     *
     * @param id   id
     * @param user 用户
     */
    public void start(ObjectId id, UserDetail user) {
        TaskDto taskDto = checkExistById(id, user);
        start(taskDto, user);
    }


    public void start(TaskDto taskDto, UserDetail user) {
        DAG dag = taskDto.getDag();

        //校验dag
        Map<String, List<Message>> validateMessage = dag.validate();
        if (!validateMessage.isEmpty()) {
            throw new BizException("Task.ListWarnMessage", validateMessage);
        }


        //当任务状态是运行状态，则不允许运行
//        if (TaskDto.STATUS_RUNNING.equals(taskDto.getStatus())) {
//            log.warn("current status not allow to start, task = {}, status = {}", taskDto.getName(), taskDto.getStatus());
//            throw new BizException("Task.StartStatusInvalid");
//        }
        taskStartService.start0(taskDto, user);
    }

    /**
     * 暂停任务 暂停就是将所有的子任务停止下来，不清空中间状态
     *
     * @param id   id
     * @param user 用户
     */
    public void pause(ObjectId id, UserDetail user, boolean force) {
        //查询任务是否存在
        TaskDto taskDto = checkExistById(id, user);
        //暂停所有的子任务
        List<SubTaskDto> subTaskDtos = subTaskService.findByTaskId(id);

        log.debug("subtask size = {}, task name = {}", subTaskDtos.size(), taskDto.getName());

        for (SubTaskDto subTaskDto : subTaskDtos) {
            try {
                subTaskService.pause(subTaskDto, user, force);
            } catch (BizException e) {
                log.warn("pause sub task failed, e = {}", e.getErrorCode());
            }
        }
    }


    /**
     * 重置任务
     *
     * @param id   任务id
     * @param user 用户
     * @return
     */
    public TaskDto renew(ObjectId id, UserDetail user) {
        TaskDto taskDto = checkExistById(id, user);
        String status = taskDto.getStatus();

        //只有暂停或者停止状态可以重置
        if (!SubTaskOpStatusEnum.to_renew_status.v().contains(status)) {
            //需要停止的时候才可以操作
            log.info("The current status of the task does not allow resetting, task name = {}, status = {}", taskDto.getName(), status);
            throw new BizException("Task.statusIsNotStop");
        }

        log.debug("check task status complete, task name = {}", taskDto.getName());


        //清空子任务的运行信息 清空任务运行历史表 这里是
        List<SubTaskDto> subTaskDtoList = subTaskService.findByTaskId(id, user);
        if (CollectionUtils.isNotEmpty(subTaskDtoList)) {
            for (SubTaskDto subTaskDto : subTaskDtoList) {
                subTaskService.renew(subTaskDto, user);
                subTaskService.renewAgentMeasurement(subTaskDto.getId().toString());
            }
        }

        log.debug("renew subtask complete, task name = {}", taskDto.getName());

        //更新任务信息
        Update update = Update.update("status", TaskDto.STATUS_EDIT).unset("temp");
        updateById(id, update, user);

        CustomerJobLog customerJobLog = new CustomerJobLog(taskDto.getId().toString(), taskDto.getName());
        customerJobLog.setDataFlowType(CustomerJobLogsService.DataFlowType.sync.getV());
        customerJobLogsService.resetDataFlow(customerJobLog, user);
        return findById(id);


    }

    /**
     * 停止任务 将所有的子任务停止，并且清空所有的中间状态
     *
     * @param id   id
     * @param user 用户
     */
    public void stop(ObjectId id, UserDetail user, boolean force) {
        stop(id, user, force, false);
    }

    /**
     * 停止任务 将所有的子任务停止，并且清空所有的中间状态
     *
     * @param id      id
     * @param user    用户
     * @param restart 重启标识， 为true的时候需要重启
     */
    public void stop(ObjectId id, UserDetail user, boolean force, boolean restart) {
        //查询任务是否存在
        TaskDto taskDto = checkExistById(id, user);
        CustomerJobLog customerJobLog = new CustomerJobLog(taskDto.getId().toString(), taskDto.getName());
        customerJobLog.setDataFlowType(CustomerJobLogsService.DataFlowType.sync.getV());
        if (force) {
            customerJobLogsService.stopDataFlow(customerJobLog, user);
        } else {
            customerJobLogsService.forceStopDataFlow(customerJobLog, user);
        }
        //暂停所有的子任务
        List<SubTaskDto> subTaskDtos = subTaskService.findByTaskId(id);
        for (SubTaskDto subTaskDto : subTaskDtos) {
            try {
                subTaskService.pause(subTaskDto, user, force, restart);
            } catch (BizException e) {
                log.warn("pause sub task failed, e = {}", e.getErrorCode());
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
     * 获取子任务名称
     *
     * @param parentId 任务id
     * @param user     用户
     * @return
     */
    private String getSubTaskName(ObjectId parentId, UserDetail user) {
        Query query = new Query(Criteria.where("_id").is(parentId));
        query.fields().include("name");
        TaskDto taskDto = findOne(query);

        int subNameMaxIndex = getSubNameAddIndex(parentId, user);


        return getSubTaskName(taskDto, subNameMaxIndex, user);
    }

    /**
     * 获取当前子任务名称序号
     *
     * @param parentId 任务id
     * @param user     用户
     * @return
     */
    private int getSubNameAddIndex(ObjectId parentId, UserDetail user) {
        Query query1 = new Query(Criteria.where("parentId").is(parentId));
        query1.fields().include("name");
        List<SubTaskEntity> subTaskEntities = subTaskService.findAll(query1, user);
        int index = 1;
        if (CollectionUtils.isNotEmpty(subTaskEntities)) {
            Optional<Integer> maxOptional = subTaskEntities.stream().map(s -> {
                String name = s.getName();
                int start = name.indexOf("(");
                int end = name.indexOf(")");
                String nameIndex = name.substring(start + 1, end);
                return Integer.parseInt(nameIndex);
            }).max(Comparator.comparingInt(Integer::intValue));
            if (maxOptional.isPresent()) {
                index = maxOptional.get() + 1;
            }
        }

        return index;
    }

    /**
     * 获取子任务名称
     *
     * @param taskDto 任务信息
     * @param index   序号
     * @param user    用户
     * @return
     */
    private String getSubTaskName(TaskDto taskDto, int index, UserDetail user) {

        if (StringUtils.isEmpty(taskDto.getName())) {
            Query query = new Query(Criteria.where("_id").is(taskDto.getId()));
            query.fields().include("name");
            TaskDto one = findOne(query);
            if (one == null) {
                log.warn("create sub task name failed, task name not found, id = {}", taskDto.getId());
                throw new BizException("Task.NotFound", "create sub task name failed");
            }
            taskDto.setName(one.getName());
        }

        return taskDto.getName() + " (" + index + ")";
    }


    /**
     * 跟更新子任务
     *
     * @param taskDto 任务
     * @param user    用户
     * @param dags    操作的dags
     */
    private void updateSubtask(TaskDto taskDto, UserDetail user, List<DAG.SubTaskStatus> dags) {
        Map<String, List<DAG.SubTaskStatus>> actionMap = dags.stream().collect(Collectors.groupingBy(DAG.SubTaskStatus::getAction));

        Boolean isOpenAutoDDL = taskDto.getIsOpenAutoDDL();

        actionMap.forEach((k, v) -> {
            if ("create".equals(k)) {
                List<SubTaskDto> subTaskDtos = new ArrayList<>();

                int subNameMaxIndex = getSubNameAddIndex(taskDto.getId(), user);
                for (DAG.SubTaskStatus subTaskStatus : v) {
                    SubTaskDto subTaskDto = new SubTaskDto();
                    subTaskDto.setParentId(taskDto.getId());
                    subTaskDto.setName(getSubTaskName(taskDto, subNameMaxIndex, user));
                    subTaskDto.setStatus(SubTaskDto.STATUS_EDIT);
                    subTaskDto.setDag(subTaskStatus.getDag());
                    subTaskDto.setIsEdit(true);

                    subTaskDto.setIsOpenAutoDDL(isOpenAutoDDL);
                    subTaskDtos.add(subTaskDto);
                    subNameMaxIndex++;
                }

                log.debug("need add subtask size = {}", subTaskDtos.size());
                //子任务入库
                subTaskService.save(subTaskDtos, user);
            } else if ("update".equals(k)) {
                List<SubTaskDto> subTaskDtos = v.stream().map(s -> {
                    SubTaskDto subTaskDto = new SubTaskDto();
                    subTaskDto.setId(s.getSubTaskId());
                    subTaskDto.setDag(s.getDag());

                    subTaskDto.setIsOpenAutoDDL(isOpenAutoDDL);

                    return subTaskDto;
                }).collect(Collectors.toList());
                log.debug("need add subtask size = {}", subTaskDtos.size());

                //如果task的重要属性修改了。则子任务都是需要重新启动的
                TaskDto oldTaskDto = findById(taskDto.getId());

                subTaskService.update(subTaskDtos, user, !taskDto.equals(oldTaskDto));
            } else if ("delete".equals(k)) {
                log.debug("need add subtask size = {}", v.size());
                for (DAG.SubTaskStatus subTaskStatus : v) {
                    subTaskService.deleteById(subTaskStatus.getSubTaskId(), user);
                }
            }
        });
    }


    public void batchStart(List<ObjectId> taskIds, UserDetail user) {
        List<TaskDto> taskDtos = findAllTasksByIds(taskIds.stream().map(ObjectId::toHexString).collect(Collectors.toList()));
        for (TaskDto task : taskDtos) {
            checkDagAgentConflict(task, false);

            try {
                boolean noPass = taskStartService.taskStartCheckLog(task, user);
                if (!noPass) {
                    start(task, user);
                }

            } catch (Exception e) {
                log.warn("start task exception, task id = {}, e = {}", task.getId(), e);
            }
        }
    }

    public void batchStop(List<ObjectId> taskIds, UserDetail user) {
        for (ObjectId taskId : taskIds) {
            try {
                stop(taskId, user, false);
            } catch (Exception e) {
                log.warn("stop task exception, task id = {}, e = {}", taskId, e);
            }
        }
    }

    public void batchDelete(List<ObjectId> taskIds, UserDetail user) {
        for (ObjectId taskId : taskIds) {
            try {
                remove(taskId, user);
                //todo  需不需要手动删除
                inspectService.deleteByTaskId(taskId.toString());
            } catch (Exception e) {

                log.warn("delete task exception, task id = {}, e = {}", taskId, e);
                if (e instanceof BizException) {
                    throw e;
                }
            }
        }
    }

    public void batchRenew(List<ObjectId> taskIds, UserDetail user) {
        for (ObjectId taskId : taskIds) {
            try {
                renew(taskId, user);
            } catch (Exception e) {
                log.warn("renew task exception, task id = {}, e = {}", taskId, e);
            }
        }
    }

    /**
     * 任务的状态就通过 statues 来判断，statues 为空的时候，任务是编辑中
     *
     * @param filter     optional, page query parameters
     * @param userDetail
     * @return
     */
    @Override
    public Page<TaskDto> find(Filter filter, UserDetail userDetail) {

        Where where = filter.getWhere();
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
            where.put("is_deleted", false);
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
/*
            Criteria parentId = Criteria.where("parentId").in(taskIds);
            Query query1 = new Query(parentId);
            query1.fields().include("_id", "parentId");
*/
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

            //添加运行时间
            List<ObjectId> taskObjectIds = items.stream().map(BaseDto::getId).collect(Collectors.toList());
            Criteria parentIdCriteria = Criteria.where("parentId").in(taskObjectIds);
            Query query1 = new Query(parentIdCriteria);
            query1.fields().include("parentId", "startTime");
            List<SubTaskDto> subTaskDtos = subTaskService.findAllDto(query1, userDetail);
            Map<ObjectId, List<SubTaskDto>> subMap = subTaskDtos.stream().collect(Collectors.groupingBy(SubTaskDto::getParentId));

            for (TaskDto item : items) {
                if (item != null) {
                    List<SubTaskDto> subTaskDtos1 = subMap.get(item.getId());
                    if (CollectionUtils.isNotEmpty(subTaskDtos1)) {
                        Optional<Date> max = subTaskDtos1.stream().map(ParentSubTaskDto::getStartTime).filter(Objects::nonNull).max(Comparator.comparingLong(Date::getTime));
                        max.ifPresent(item::setStartTime);
                    }

                }
            }
            //查询任务列表所属的所有的子任启动时间
            //根据任务id分组得到所有的子任务组，并且取分组中的最后启动的时间


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
        String statusParam = "";
        if (null != where.get("status") && where.get("status") instanceof String) {
            statusParam = (String) where.remove("status");
        }


        Criteria criteria = Criteria.where("is_deleted").ne(true).and("user_id").is(userDetail.getUserId());
        Criteria orToCriteria = parseOrToCriteria(where);

        Criteria statusesCriteria = new Criteria();

        if ("edit".equals(statusParam)) {
            Criteria editCriteria = Criteria.where("status").is(TaskStatusEnum.STATUS_EDIT.getValue());
            Criteria notEmptyStatus = Criteria.where("statuses").is(new ArrayList());
            statusesCriteria = new Criteria().andOperator(notEmptyStatus, editCriteria);
        } else if ("ready".equals(statusParam)) {
            Criteria editCriteria = Criteria.where("status").is(TaskStatusEnum.STATUS_EDIT.getValue());
            Criteria notEmptyStatus = Criteria.where("statuses").ne(new ArrayList());
            statusesCriteria = new Criteria().andOperator(editCriteria, notEmptyStatus);
        } else if (StringUtils.isNotEmpty(statusParam)) {
            criteria.and("statuses.status").is(statusParam);
        }

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

        criteria.andOperator(orToCriteria, statusesCriteria);
        query.addCriteria(criteria);

        TmPageable tmPageable = new TmPageable();
        Integer page = (filter.getSkip() / filter.getLimit()) + 1;
        tmPageable.setPage(page);
        tmPageable.setSize(filter.getLimit());

        String order = filter.getOrder() == null ? "createTime DESC" : String.valueOf(filter.getOrder());
        if (order.contains("ASC")) {
            tmPageable.setSort(Sort.by("createTime").ascending());
        } else {
            tmPageable.setSort(Sort.by("createTime").descending());
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
        Where where = filter.getWhere();
        String status = "";
        if (null != where.get("status")) {
            //已经不通过status 来查找了
            status = (String) where.remove("status");
        }
        Criteria criteria = Criteria.where("is_deleted").ne(true).and("user_id").is(userDetail.getUserId());

        Criteria orToCriteria = parseOrToCriteria(filter.getWhere());

        //处理状态
        List<String> statues = new ArrayList<>();
        Criteria statusesCriteria = new Criteria();
        switch (status) {
            case "running":
                statues.addAll(SubTaskService.runningStatus);
                criteria.and("statuses.status").in(statues);
                break;
            case "not_running":
                //子任务的状态是空数组   所以默认父级就显示未运行
                statues.addAll(SubTaskService.stopStatus);
                Criteria notRunningCri = Criteria.where("statuses.status").in(statues);
                Criteria emptyStatus = Criteria.where("statuses").is(new ArrayList());
                List<Criteria> statusCriList = new ArrayList<>();

                statusCriList.add(notRunningCri);
                statusCriList.add(emptyStatus);
                statusesCriteria = new Criteria().orOperator(statusCriList);
                break;
            case "error":
                statues.add(SubTaskDto.STATUS_ERROR);
                criteria.and("statuses.status").in(statues);
                break;
            case "edit":
                statues.add(SubTaskDto.STATUS_EDIT);
                criteria.and("statuses.status").in(statues);
                break;
            default:
                break;
        }

        criteria.andOperator(orToCriteria, statusesCriteria);

        Query query = new Query();
        parseWhereCondition(filter.getWhere(), query);
        parseFieldCondition(filter, query);

        query.addCriteria(criteria);

        TmPageable tmPageable = new TmPageable();
        Integer page = (filter.getSkip() / filter.getLimit()) + 1;
        tmPageable.setPage(page);
        tmPageable.setSize(filter.getLimit());

        String order = filter.getOrder() == null ? "createTime DESC" : String.valueOf(filter.getOrder());
        if (order.contains("ASC")) {
            tmPageable.setSort(Sort.by("createTime").ascending());
        } else {
            tmPageable.setSort(Sort.by("createTime").descending());
        }

        Long total = repository.getMongoOperations().count(query, TaskEntity.class);
        List<TaskEntity> taskEntityList = repository.getMongoOperations().find(query.with(tmPageable), TaskEntity.class);

        List<TaskDto> taskDtoList = com.tapdata.tm.utils.BeanUtil.deepCloneList(taskEntityList, TaskDto.class);

        Page result = new Page();
        result.setItems(taskDtoList);
        result.setTotal(total);

        return result;
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
    @Transactional
    public TaskDto createShareCacheTask(SaveShareCacheParam saveShareCacheParam, UserDetail user) {
        TaskDto taskDto = new TaskDto();

        parseCacheToTaskDto(saveShareCacheParam, taskDto);
        taskDto = confirmById(taskDto, user, true);
        //新建完成马上调度
        List<ObjectId> taskIds = Arrays.asList(taskDto.getId());
        batchStart(taskIds, user);
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
       /* if (where != null && where.get("status") != null) {
            String status = (String) where.get("status");
            if (StringUtils.isNotBlank(status)) {
                List<String> statues = new ArrayList<>();
                switch (status) {
                    case "running":
                        statues.addAll(SubTaskService.runningStatus);
                        break;
                    case "not_running":
                        statues.addAll(SubTaskService.stopStatus);
                        statues.add(SubTaskDto.STATUS_EDIT);
                        break;
                    case "error":
                        statues.add(SubTaskDto.STATUS_ERROR);
                        break;
                    default:
                        break;
                }

                if (CollectionUtils.isNotEmpty(statues)) {
                    Map<String, List<String>> statusMap = new HashMap<>();
                    statusMap.put("$in", statues);
                    where.put("statuses.status", statusMap);
                }
            }
            where.remove("status");
        }*/

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

                    MeasurementEntity measurementEntity = measurementService.findByTaskIdAndNodeId(taskDto.getId().toString(), sourceNode.getId());
                    if (null != measurementEntity && null != measurementEntity.getStatistics() && null != measurementEntity.getStatistics().get("cdcTime")) {
                        Map<String, Number> statistics = measurementEntity.getStatistics();
                        //cdc 值为integer or long 需要处理，cdcTime   也可能为0 需要处理
                        Number cdcTime = statistics.get("cdcTime");
                        Date cdcTimeDate = null;
                        if (cdcTime instanceof Integer) {
                            cdcTimeDate = new Date(cdcTime.longValue());
                        } else if (cdcTime instanceof Long) {
                            cdcTimeDate = new Date((Long) cdcTime);
                        }
                        shareCacheVo.setCacheTimeAt(cdcTimeDate);
                    }
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
                .and("is_deleted").is(false)
                .and("shareCache").ne(true)
                .andOperator(Criteria.where("status").exists(true), Criteria.where("status").ne(null), Criteria.where("syncType").exists(true));

        Query query = Query.query(criteria);
        query.fields().include("syncType", "status", "statuses");
        //把任务都查询出来
        List<TaskDto> taskDtoList = findAll(query);
        Map<String, List<TaskDto>> syncTypeToTaskList = taskDtoList.stream().collect(Collectors.groupingBy(TaskDto::getSyncType));


        resultChart.put("chart1", getDataCopyChart(syncTypeToTaskList));
//        resultChart.put("chart2", dataCopy);
        resultChart.put("chart3", getDataDevChart(syncTypeToTaskList));
//        resultChart.put("chart4", dataDev);
        resultChart.put("chart5", inspectService.inspectPreview(user));
        resultChart.put("chart6", measurementService.getTransmitTotal(user));
        return resultChart;
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
    private Map<String, Object> getDataCopyChart( Map<String, List<TaskDto>> syncTypeToTaskList) {
        Map<String, Object> dataCopyPreview = new HashMap();

//        Query query = Query.query(Criteria.where("is_deleted").ne(true).and("shareCache").ne(true).and("syncType").is(SyncType.MIGRATE.getValue()));
//        List<TaskDto> migrateList = findAllDto(query, userDetail);

        List<TaskDto> migrateList =  syncTypeToTaskList.getOrDefault(SyncType.MIGRATE.getValue(), Collections.emptyList());
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
     * 统计的是子任务 Task中的statuses
     *
     * @param syncTypeToTaskList
     * @return
     */
    private Map<String, Object> getDataDevChart(Map<String, List<TaskDto>> syncTypeToTaskList) {
        Map<String, Object> dataCopyPreview = new HashMap();

        List<TaskDto> synList = syncTypeToTaskList.getOrDefault(SyncType.SYNC.getValue(), Collections.emptyList());

        Map<String, Long> statusToCount = new HashMap<>();
        if (CollectionUtils.isNotEmpty(synList)) {
            for (int i = 0; i < synList.size(); i++) {
                TaskDto taskDto = synList.get(i);
                List<SubStatus> subStatusList = taskDto.getStatuses();
                if (CollectionUtils.isNotEmpty(subStatusList)) {
                    for (SubStatus subStatus : subStatusList) {
                        MapUtils.increase(statusToCount, subStatus.getStatus());
                    }
                } else {
                    //Statuses 为空 就认为是编辑中
                    MapUtils.increase(statusToCount, SubTaskEnum.STATUS_EDIT.getValue());
                }
            }
        }

        //数据复制概览
        List<Map> dataCopyPreviewItems = new ArrayList();
        List<String> allStatus = SubTaskEnum.getAllStatus();
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
     * 全量开始时间，参考 SubTask 的milestone  code 是 READ_SNAPSHOT 的start
     * 增量开始时间，参考 SubTask 的milestone  code 是 READ_CDC_EVENT 的start
     * 任务完成时间  取 ParentSubTaskDto 的 finishTime
     * 增量最大滞后时间:  AgentStatistics  replicateLag
     * 任务开始时间： SubTask startTime
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

        Criteria criteria = Criteria.where("parentId").is(MongoUtils.toObjectId(id));
        List<SubTaskDto> subTaskDtos = subTaskService.findAll(Query.query(criteria));
        SubTaskDto subTaskDto = null;
        if (CollectionUtils.isNotEmpty(subTaskDtos)) {
            subTaskDto = subTaskDtos.get(0);
            String subTaskId = subTaskDto.getId().toString();

            String type = taskDto.getType();
            if ("initial_sync".equals(type)) {
                //设置全量开始时间
                Date initStartTime = getMillstoneTime(subTaskDto, "READ_SNAPSHOT", "initial_sync");
                taskDetailVo.setInitStartTime(initStartTime);
            } else if ("cdc".equals(type)) {
                //增量开始时间
                Date cdcStartTime = getMillstoneTime(subTaskDto, "READ_CDC_EVENT", "cdc");
                taskDetailVo.setCdcStartTime(cdcStartTime);

                // 增量所处时间点
                Date eventTime = getEventTime(subTaskId);
                taskDetailVo.setEventTime(eventTime);

                //增量最大滞后时间
                taskDetailVo.setCdcDelayTime(getCdcDelayTime(subTaskId));

            } else if ("initial_sync+cdc".equals(type)) {
                //全量开始时间
                Date initStartTime = getMillstoneTime(subTaskDto, "READ_SNAPSHOT", "initial_sync");
                taskDetailVo.setInitStartTime(initStartTime);

                //增量开始时间
                Date cdcStartTime = getMillstoneTime(subTaskDto, "READ_CDC_EVENT", "cdc");
                taskDetailVo.setCdcStartTime(cdcStartTime);

                // 增量所处时间点
                Date eventTime = getEventTime(subTaskId);
                taskDetailVo.setEventTime(eventTime);

                //增量最大滞后时间
                taskDetailVo.setCdcDelayTime(getCdcDelayTime(subTaskId));
            }

            // 总时长  开始时间和结束时间都有才行
            taskDetailVo.setTaskLastHour(getLastHour(subTaskId));
            taskDetailVo.setStartTime(subTaskDto.getStartTime());
            //任务完成时间
            taskDetailVo.setTaskFinishTime(subTaskDto.getFinishTime());
            taskDetailVo.setType(taskDto.getType());
        }
        return taskDetailVo;
    }

    /**
     * 获取全量开始时间
     *
     * @param subTaskId
     * @return-
     */
  /*  private FullSyncVO getFullVo(String subTaskId) {
        FullSyncVO fullSyncVO = new FullSyncVO();
        try {
            if (null != snapshotEdgeProgressService.syncOverview(subTaskId)) {
                fullSyncVO = snapshotEdgeProgressService.syncOverview(subTaskId);
            }
        } catch (Exception e) {
            log.error("获取 fullSyncVO 出错， subTaskId：{}", subTaskId);
        }
        return fullSyncVO;
    }*/

    /**
     * 获取任务总时长
     *
     * @param subTaskId
     * @return
     */
    private Long getLastHour(String subTaskId) {
        Long taskLastHour = null;
        try {
            FullSyncVO fullSyncVO = snapshotEdgeProgressService.syncOverview(subTaskId);
            if (null != fullSyncVO) {
                if (null != fullSyncVO.getStartTs() && null != fullSyncVO.getEndTs()) {
                    taskLastHour = DateUtil.between(fullSyncVO.getStartTs(), fullSyncVO.getEndTs(), DateUnit.MS);
                }
            }
        } catch (Exception e) {
            log.error("获取 fullSyncVO 出错， subTaskId：{}", subTaskId);
        }
        return taskLastHour;
    }


    /**
     * 获取增量开始时间
     *
     * @param subTaskDto
     * @return
     */
    private Date getMillstoneTime(SubTaskDto subTaskDto, String code, String group) {
        Date millstoneTime = null;
        Optional<Milestone> optionalMilestone = Optional.empty();
        List<Milestone> milestones = subTaskDto.getMilestones();
        if (null != milestones) {
            optionalMilestone = milestones.stream().filter(s -> (code.equals(s.getCode()) && (group).equals(s.getGroup()))).findFirst();
            if (optionalMilestone.isPresent() && null != optionalMilestone.get().getStart() && optionalMilestone.get().getStart() > 0) {
                millstoneTime = new Date(optionalMilestone.get().getStart());
            }

        }
        return millstoneTime;
    }

    /**
     * 增量延迟时间
     *
     * @return
     */
    private Long getCdcDelayTime(String subTaskId) {
        Long cdcDelayTime = null;
        MeasurementEntity measurementEntity = measurementService.findBySubTaskId(subTaskId);
        if (null != measurementEntity && null != measurementEntity.getStatistics() && null != measurementEntity.getStatistics().get("replicateLag")) {
            Number cdcDelayTimeNumber = (Number) measurementEntity.getStatistics().get("replicateLag");
            cdcDelayTime = cdcDelayTimeNumber.longValue();
        }
        return cdcDelayTime;
    }


    /**
     * 获取增量所处时间点
     *
     * @return
     */
    private Date getEventTime(String subTaskId) {
        Date eventTime = null;
        MeasurementEntity measurementEntity = measurementService.findBySubTaskId(subTaskId);
        if (null != measurementEntity) {
            Number cdcTimestamp = measurementEntity.getStatistics().getOrDefault("cdcTime", 0L);
            Long cdcMillSeconds = cdcTimestamp.longValue();
            if (cdcMillSeconds > 0) {
                eventTime = new Date(cdcMillSeconds);
            }
        }
        return eventTime;
    }


    public Boolean checkRun(String taskId, UserDetail user) {
        List<SubTaskDto> subTaskDtos = subTaskService.findByTaskId(MongoUtils.toObjectId(taskId), user, "status");
        if (CollectionUtils.isNotEmpty(subTaskDtos)) {
            for (SubTaskDto subTaskDto : subTaskDtos) {
                if (!SubTaskDto.STATUS_EDIT.equals(subTaskDto.getStatus())) {
                    return false;
                }
            }
        }

        return true;
    }

    public TransformerWsMessageDto findTransformParam(String taskId, UserDetail user) {
        TaskDto taskDto = checkExistById(MongoUtils.toObjectId(taskId), user);
        return transformSchemaService.getTransformParam(taskDto, user);
    }

    public void updateDag(TaskDto taskDto, UserDetail user) {
        checkExistById(taskDto.getId(), user, "_id");
        Criteria criteria = Criteria.where("_id").is(taskDto.getId());
        repository.update(new Query(criteria), Update.update("dag", taskDto.getDag()), user);
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
        for (String taskId : taskIds) {
            TaskDto taskDto = findById(MongoUtils.toObjectId(taskId), user);
            if (taskDto != null) {
                taskDto.setCreateUser(null);
                taskDto.setCustomId(null);
                taskDto.setLastUpdBy(null);
                taskDto.setUserId(null);
                taskDto.setStatus(TaskDto.STATUS_EDIT);
                taskDto.setStatuses(new ArrayList<>());
                jsonList.add(new TaskUpAndLoadDto("Task", JsonUtil.toJsonUseJackson(taskDto)));
                DAG dag = taskDto.getDag();
                List<Node> nodes = dag.getNodes();
                if (CollectionUtils.isNotEmpty(nodes)) {
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
                }
            }
        }
        String json = JsonUtil.toJsonUseJackson(jsonList);
        fileService1.viewImg1(json, response);
    }


    public void batchUpTask(MultipartFile multipartFile, UserDetail user, boolean cover) {
        byte[] bytes = new byte[0];
        try {
            bytes = GZIPUtil.unGzip(multipartFile.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
        String json = new String(bytes, StandardCharsets.UTF_8);

        List<TaskUpAndLoadDto> taskUpAndLoadDtos = JsonUtil.parseJsonUseJackson(json, new TypeReference<List<TaskUpAndLoadDto>>() {
        });
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

            }
        }

        metadataInstancesService.batchImport(metadataInstancess, user, cover);
        dataSourceService.batchImport(connections, user, cover);

        batchImport(tasks, user, cover);
    }

    public void batchImport(List<TaskDto> taskDtos, UserDetail user, boolean cover) {
        for (TaskDto taskDto : taskDtos) {
            Query query = new Query(Criteria.where("_id").is(taskDto.getId()).and("is_deleted").ne(true));
            query.fields().include("id");
            TaskDto one = findOne(query);
            if (one == null || cover) {
                ObjectId objectId = null;
                if (one != null) {
                    objectId = one.getId();
                }

                while (checkTaskNameNotError(taskDto.getName(), user, objectId)) {
                    taskDto.setName(taskDto.getName() + "_import");
                }

                if (one == null) {
                    taskDto.setId(null);
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
                checkDagAgentConflict(taskDto, false);
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
        return findAll(query);
    }

    public void updateStatus(ObjectId taskId, String status) {
        Query query = Query.query(Criteria.where("_id").is(taskId));
        Update update = Update.update("status", status);
        update(query, update);
    }
}
