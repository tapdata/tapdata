//package com.tapdata.tm.task.service;
//
//import cn.hutool.core.bean.BeanUtil;
//import com.mongodb.client.result.UpdateResult;
//import com.tapdata.manager.common.utils.JsonUtil;
//import com.tapdata.tm.CustomerJobLogs.CustomerJobLog;
//import com.tapdata.tm.CustomerJobLogs.service.CustomerJobLogsService;
//import com.tapdata.tm.base.dto.Field;
//import com.tapdata.tm.base.dto.Filter;
//import com.tapdata.tm.base.dto.Page;
//import com.tapdata.tm.base.dto.Where;
//import com.tapdata.tm.base.exception.BizException;
//import com.tapdata.tm.base.service.BaseService;
//import com.tapdata.tm.commons.dag.*;
//import com.tapdata.tm.commons.dag.logCollector.HazelCastImdgNode;
//import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
//import com.tapdata.tm.commons.dag.nodes.DataParentNode;
//import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
//import com.tapdata.tm.commons.dag.nodes.TableNode;
//import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
//import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
//import com.tapdata.tm.commons.schema.MetadataInstancesDto;
//import com.tapdata.tm.commons.task.dto.*;
//import com.tapdata.tm.commons.task.dto.progress.SubTaskSnapshotProgress;
//import com.tapdata.tm.commons.util.ConnHeartbeatUtils;
//import com.tapdata.tm.commons.util.CreateTypeEnum;
//import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
//import com.tapdata.tm.config.security.UserDetail;
//import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
//import com.tapdata.tm.ds.service.impl.DataSourceService;
//import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
//import com.tapdata.tm.messagequeue.service.MessageQueueService;
//import com.tapdata.tm.metadatainstance.service.MetaDataHistoryService;
//import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
//import com.tapdata.tm.monitor.entity.AgentStatDto;
//import com.tapdata.tm.monitor.service.MeasurementService;
//import com.tapdata.tm.task.bean.FullSyncVO;
//import com.tapdata.tm.task.bean.IncreaseSyncVO;
//import com.tapdata.tm.task.bean.RunTimeInfo;
//import com.tapdata.tm.task.constant.SubTaskOpStatusEnum;
//import com.tapdata.tm.task.entity.SubTaskEntity;
//import com.tapdata.tm.task.repository.SubTaskRepository;
//import com.tapdata.tm.task.vo.SubTaskDetailVo;
//import com.tapdata.tm.user.service.UserService;
//import com.tapdata.tm.utils.Lists;
//import com.tapdata.tm.utils.MongoUtils;
//import com.tapdata.tm.utils.UUIDUtil;
//import com.tapdata.tm.worker.dto.WorkerDto;
//import com.tapdata.tm.worker.entity.Worker;
//import com.tapdata.tm.worker.service.WorkerService;
//import com.tapdata.tm.ws.enums.MessageType;
//import lombok.NonNull;
//import lombok.Setter;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.collections4.CollectionUtils;
//import org.apache.commons.lang3.StringUtils;
//import org.bson.Document;
//import org.bson.types.ObjectId;
//import org.springframework.beans.BeanUtils;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.data.mongodb.core.BulkOperations;
//import org.springframework.data.mongodb.core.MongoTemplate;
//import org.springframework.data.mongodb.core.query.Criteria;
//import org.springframework.data.mongodb.core.query.Query;
//import org.springframework.data.mongodb.core.query.Update;
//import org.springframework.scheduling.annotation.Async;
//import org.springframework.stereotype.Service;
//import org.springframework.transaction.annotation.Transactional;
//
//import java.util.*;
//import java.util.function.Function;
//import java.util.stream.Collectors;
//
///**
// * @Author:
// * @Date: 2021/11/03
// * @Description:
// */
//@Service
//@Slf4j
//@Setter(onMethod_ = {@Autowired})
//public class SubTaskService extends BaseService<SubTaskDto, SubTaskEntity, ObjectId, SubTaskRepository> {
//
//    private TaskService taskService;
//    private MeasurementService measurementService;
//    private SnapshotEdgeProgressService snapshotEdgeProgressService;
//    private TaskNodeRuntimeInfoService taskNodeRuntimeInfoService;
//    private TaskDatabaseRuntimeInfoService taskDatabaseRuntimeInfoService;
//    private TaskSnapshotService taskSnapshotService;
//    private TaskRunHistoryService taskRunHistoryService;
//    private WorkerService workerService;
//    private MessageQueueService messageQueueService;
//    private CustomerJobLogsService customerJobLogsService;
//    private UserService userService;
//    private MetadataInstancesService metadataInstancesService;
//    private DataSourceService dataSourceService;
//
//    private DataSourceDefinitionService dataSourceDefinitionService;
//
//    private MetaDataHistoryService historyService;
//    /**
//     * 非停止状态
//     */
//    public static Set<String> stopStatus = new HashSet<>();
//    /**
//     * 停止状态
//     */
//    public static Set<String> runningStatus = new HashSet<>();
//
////    @Autowired
////    private StateMachineService stateMachineService;
//
//    static {
//
//        runningStatus.add(SubTaskDto.STATUS_SCHEDULING);
//        runningStatus.add(SubTaskDto.STATUS_WAIT_RUN);
//        runningStatus.add(SubTaskDto.STATUS_RUNNING);
//        runningStatus.add(SubTaskDto.STATUS_STOPPING);
//
//        stopStatus.add(SubTaskDto.STATUS_SCHEDULE_FAILED);
//        stopStatus.add(SubTaskDto.STATUS_COMPLETE);
//        stopStatus.add(SubTaskDto.STATUS_STOP);
//    }
//
//    public SubTaskService(@NonNull SubTaskRepository repository, TaskNodeRuntimeInfoService taskNodeRuntimeInfoService,
//                          TaskSnapshotService taskSnapshotService, TaskRunHistoryService taskRunHistoryService,
//                          TaskDatabaseRuntimeInfoService taskDatabaseRuntimeInfoService,
//                          CustomerJobLogsService customerJobLogsService,
//                          WorkerService workerService, MessageQueueService messageQueueService) {
//        super(repository, SubTaskDto.class, SubTaskEntity.class);
//        this.taskNodeRuntimeInfoService = taskNodeRuntimeInfoService;
//        this.taskSnapshotService = taskSnapshotService;
//        this.taskRunHistoryService = taskRunHistoryService;
//        this.workerService = workerService;
//        this.messageQueueService = messageQueueService;
//        this.taskDatabaseRuntimeInfoService = taskDatabaseRuntimeInfoService;
//        this.customerJobLogsService = customerJobLogsService;
//    }
//
//    protected void beforeSave(SubTaskDto subTask, UserDetail user) {
//        DAG dag = subTask.getDag();
//        if (dag == null) {
//            return;
//        }
//        List<Node> nodes = dag.getNodes();
//        if (CollectionUtils.isEmpty(nodes)) {
//            return;
//        }
//
//        for (Node node : nodes) {
//            node.setSchema(null);
//            node.setOutputSchema(null);
//        }
//
//    }
//
//    /**
//     * 根据任务id查询子任务列表
//     *
//     * @param taskId
//     * @return
//     */
//    public List<SubTaskDto> findByTaskId(ObjectId taskId, UserDetail user, String... fields) {
//        Criteria criteria = Criteria.where("parentId").is(taskId);
//        Query query = new Query(criteria);
//        if (fields != null && fields.length > 0) {
//            query.fields().include(fields);
//        }
//
//        if (user != null) {
//            return findAllDto(query, user);
//        }
//
//        return findAll(query);
//    }
//
//    public List<SubTaskDto> findByTaskId(ObjectId taskId, String... fields) {
//        return findByTaskId(taskId, null, fields);
//    }
//
//    /**
//     * 根据任务id查询子任务列表
//     *
//     * @param taskId
//     * @return
//     */
//    public List<SubTaskDto> findByTaskId(String taskId) {
//        Criteria criteria = Criteria.where("parentId").is(taskId);
//        Query query = new Query(criteria);
//        return findAll(query);
//    }
//
//
//    /**
//     * 重置子任务之后，情况指标观察数据
//     *
//     * @param subTaskId
//     */
//    public void renewAgentMeasurement(String subTaskId) {
//        //所有的任务重置操作，都会进这里
//        //根据subTaskId 把指标数据都删掉
//        measurementService.deleteSubTaskMeasurement(subTaskId);
//    }
//
//
//    /**
//     * 重置子任务
//     * 清空子任务的运行中间状态信息
//     *
//     * @param
//     */
//    public void renew(ObjectId id, UserDetail user) {
//        SubTaskDto subTaskDto = checkExistById(id);
//        renew(subTaskDto, user);
//        renewAgentMeasurement(id.toString());
//    }
//
//    public void renew(SubTaskDto subTaskDto, UserDetail user) {
//        sendRenewMq(subTaskDto, user, DataSyncMq.OP_TYPE_RESET);
//        renewNotSendMq(subTaskDto, user);
//    }
//
//    public void renewNotSendMq(SubTaskDto subTaskDto, UserDetail user) {
//        log.info("renew subtask, subtask name = {}, username = {}", subTaskDto.getName(), user.getUsername());
//
//        Update set = Update.update("agentId", null).set("agentTags", null).set("scheduleTimes", null)
//                .set("scheduleTime", null)
//                .unset("milestones").unset("tmCurrentTime").set("messages", null).set("status", SubTaskDto.STATUS_EDIT);
//
//
//        if (subTaskDto.getAttrs() != null) {
//            subTaskDto.getAttrs().remove("syncProgress");
//            subTaskDto.getAttrs().remove("edgeMilestones");
//
//            set.set("attrs", subTaskDto.getAttrs());
//        }
//
//        //updateById(subTaskDto.getId(), set, user);
//
//        //清空当前子任务的所有的node运行信息TaskRuntimeInfo
//        List<Node> nodes = subTaskDto.getDag().getNodes();
//        if (nodes != null) {
//
//            List<String> nodeIds = nodes.stream().map(Node::getId).collect(Collectors.toList());
//            Criteria criteria = Criteria.where("subTaskId").is(subTaskDto.getId().toHexString())
//                    .and("type").is(SubTaskSnapshotProgress.ProgressType.EDGE_PROGRESS.name())
//                    .orOperator(Criteria.where("srcNodeId").in(nodeIds),
//                            Criteria.where("tgtNodeId").in(nodeIds));
//            Query query = new Query(criteria);
//
//            snapshotEdgeProgressService.deleteAll(query);
//
//            Criteria criteria1 = Criteria.where("subTaskId").is(subTaskDto.getId().toHexString())
//                    .and("type").is(SubTaskSnapshotProgress.ProgressType.SUB_TASK_PROGRESS.name());
//            Query query1 = new Query(criteria1);
//
//            snapshotEdgeProgressService.deleteAll(query1);
////            taskNodeRuntimeInfoService.deleteAll(query);
////            taskDatabaseRuntimeInfoService.deleteAll(query);
//        }
//
//        //重置的时候需要将子任务的temp更新到子任务实体中
//        subTaskDto.setDag(subTaskDto.getTempDag());
//        beforeSave(subTaskDto, user);
//        set.unset("tempDag").set("isEdit", true).set("status", SubTaskDto.STATUS_EDIT);
//        Update update = new Update();
//        taskService.update(new Query(Criteria.where("_id").is(subTaskDto.getParentId())), update.unset("temp"));
//        updateById(subTaskDto.getId(), set, user);
//
//        CustomerJobLog customerJobLog = new CustomerJobLog(subTaskDto.getId().toString(), subTaskDto.getName());
//        customerJobLog.setDataFlowType(CustomerJobLogsService.DataFlowType.sync.getV());
//        customerJobLogsService.resetJob(customerJobLog, user);
//        resetFlag(subTaskDto.getId(), user, "resetFlag");
//    }
//
//    private void sendRenewMq(SubTaskDto subTaskDto, UserDetail user, String opType) {
//        if (checkPdkSubTask(subTaskDto, user)) {
//
//            DataSyncMq mq = new DataSyncMq();
//            mq.setTaskId(subTaskDto.getId().toHexString());
//            mq.setOpType(opType);
//            mq.setType(MessageType.DATA_SYNC.getType());
//
//
//            Map<String, Object> data;
//            String json = JsonUtil.toJsonUseJackson(mq);
//            data = JsonUtil.parseJsonUseJackson(json, Map.class);
//
//            if (subTaskDto.getParentTask() == null) {
//                TaskDto parentTask = taskService.findById(subTaskDto.getParentId(), user);
//                subTaskDto.setParentTask(parentTask);
//            }
//            TaskDto taskDto = subTaskDto.getParentTask();
//            if (StringUtils.equals(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), taskDto.getAccessNodeType())
//                    && CollectionUtils.isNotEmpty(taskDto.getAccessNodeProcessIdList())) {
//                subTaskDto.setAgentId(taskDto.getAccessNodeProcessIdList().get(0));
//            } else {
//                List<Worker> availableAgent = workerService.findAvailableAgent(user);
//                if (CollectionUtils.isNotEmpty(availableAgent)) {
//                    Worker worker = availableAgent.get(0);
//                    subTaskDto.setAgentId(worker.getProcessId());
//                } else {
//                    subTaskDto.setAgentId(null);
//                }
//            }
//
//            MessageQueueDto queueDto = new MessageQueueDto();
//            queueDto.setReceiver(subTaskDto.getAgentId());
//            queueDto.setData(data);
//            queueDto.setType("pipe");
//
//            log.debug("build stop subtask websocket context, processId = {}, userId = {}, queueDto = {}", subTaskDto.getAgentId(), user.getUserId(), queueDto);
//            messageQueueService.sendMessage(queueDto);
//
//            //检查是否完成重置，设置8秒的超时时间
//            boolean checkFlag = false;
//            for (int i = 0; i < 16; i++) {
//                checkFlag = DataSyncMq.OP_TYPE_RESET.equals(opType) ? checkResetFlag(subTaskDto.getId(), user) : checkDeleteFlag(subTaskDto.getId(), user);
//                if (checkFlag) {
//                    break;
//                }
//                try {
//                    Thread.sleep(500L);
//                } catch (InterruptedException e) {
//                    throw new BizException("SystemError");
//                }
//            }
//
//            if (!checkFlag) {
//                log.info((DataSyncMq.OP_TYPE_RESET.equals(opType) ? "reset" : "delete") + "Task reset timeout.");
//                throw new BizException(DataSyncMq.OP_TYPE_RESET.equals(opType) ? "Task.ResetTimeout" : "Task.DeleteTimeout");
//            }
//        }
//    }
//
//    public boolean deleteById(SubTaskDto subTaskDto, UserDetail user) {
//        //如果子任务在运行中，将任务停止，再删除（在这之前，应该提示用户这个风险）
//        if (subTaskDto == null) {
//            return true;
//        }
//
//        sendRenewMq(subTaskDto, user, DataSyncMq.OP_TYPE_DELETE);
//
//        renewNotSendMq(subTaskDto, user);
//
//        if (runningStatus.contains(subTaskDto.getStatus())) {
//            log.warn("SubTask is run, can not delete it");
//            throw new BizException("Task.DeleteSubTaskIsRun");
//        }
//
//        //TODO 删除当前模块的模型推演
//        resetFlag(subTaskDto.getId(), user, "deleteFlag");
//        return super.deleteById(subTaskDto.getId(), user);
//    }
//
//    public void update(List<SubTaskDto> newSubTasks, UserDetail user, boolean restart) {
//        List<ObjectId> ids = newSubTasks.stream().map(SubTaskDto::getId).collect(Collectors.toList());
//        Criteria criteria = Criteria.where("_id").in(ids);
//        Query query = new Query(criteria);
//        query.fields().include("_id", "dag");
//        List<SubTaskDto> subTaskDtos = findAllDto(query, user);
//        Map<ObjectId, SubTaskDto> subTaskDtoMap = subTaskDtos.stream().collect(Collectors.toMap(SubTaskDto::getId, s -> s));
//
//        for (SubTaskDto newSubTask : newSubTasks) {
//            SubTaskDto oldSubTask = subTaskDtoMap.get(newSubTask.getId());
//            if (oldSubTask == null) {
//                throw new BizException("SystemError");
//            }
//
//            //判断更新的力度是否需要
//            boolean canHotUpdate = canHotUpdate(oldSubTask.getDag(), newSubTask.getDag());
//            if (oldSubTask.getIsEdit() != null && !oldSubTask.getIsEdit() && !canHotUpdate) {
//                //throw new BizException("Task.HotUpdateFailed", "This modification is not allowed while the task is running");
//                //保存备份，并且提醒用户需要重置之后才能生效新的改动
//                newSubTask.setTempDag(newSubTask.getDag());
//                newSubTask.setDag(oldSubTask.getDag());
//                //TODO 发送消息提醒用户需要重置该子任务才可以修改生效, 这里可以考虑一起批量存
//                super.save(newSubTask, user);
//                continue;
//            }
//
//            if (stopStatus.contains(oldSubTask.getStatus())) {
//                newSubTask.setStatus(SubTaskDto.STATUS_EDIT);
//
//            }
//
//            beforeSave(newSubTask, user);
//            Update update = repository.buildUpdateSet(convertToEntity(SubTaskEntity.class, newSubTask));
//            update.set("tempDag", null);
//            updateById(newSubTask.getId(), update, user);
//
//            if (runningStatus.contains(oldSubTask.getStatus())) {
//                restart = restart ? restart : !compareDag(newSubTask.getDag(), oldSubTask.getDag());
//                if (restart) {
//                    restart(newSubTask.getId(), user);
//                }
//            }
//        }
//
//
//        //TODO 还有一只设置，可以让正在运行中的任务直接进行复杂的修改，之后重启任务、不确定这个是系统级别的设置，还是任务级别的设置
//    }
//
//    private boolean compareDag(DAG dag, DAG old) {
//        List<Node> nodes = dag.getNodes();
//        List<Node> oldNodes = old.getNodes();
//        if (nodes.size() != oldNodes.size()) {
//            return false;
//        }
//
//        Map<String, Node> oldMap = oldNodes.stream().collect(Collectors.toMap(Node::getId, n -> n));
//        for (Node node : nodes) {
//            Node oldNode = oldMap.get(node.getId());
//            if (oldNode == null) {
//                return false;
//            }
//
//            if (!node.equals(oldNode)) {
//                return false;
//            }
//        }
//        return true;
//    }
//
//
//    @Override
//    public SubTaskDetailVo findById(ObjectId id, Field field, UserDetail userDetail) {
//        SubTaskDto subTaskDto = super.findById(id, field, userDetail);
//        subTaskDto = getSubTaskDto(userDetail, subTaskDto);
//
//        SubTaskDetailVo subTaskDetailVo = BeanUtil.copyProperties(subTaskDto, SubTaskDetailVo.class);
//        FullSyncVO fullSyncVO = snapshotEdgeProgressService.syncOverview(id.toString());
//        if (null != fullSyncVO) {
//            subTaskDetailVo.setStartTime(fullSyncVO.getStartTs());
//        }
//        subTaskDetailVo.setCreator(StringUtils.isNotBlank(userDetail.getUsername()) ? userDetail.getUsername() : userDetail.getEmail());
//        return subTaskDetailVo;
//    }
//
//    private SubTaskDto getSubTaskDto(UserDetail userDetail, SubTaskDto subTaskDto) {
//        if (subTaskDto == null) {
//            return null;
//        }
//        if (subTaskDto.getParentId() != null) {
//            Query query = new Query(Criteria.where("_id").is(subTaskDto.getParentId()));
//            query.fields().exclude("dag");
//            TaskDto taskDto = taskService.findOne(query, userDetail);
//            subTaskDto.setParentTask(taskDto);
//
//            //TODO 这段代码会报错，后续修改下在上，目前flowengin会自行查询
//            //TODO 将子任务中的所有的子节点的数据源设置进去
////            List<Node> nodes = subTaskDto.getDag().getNodes();
////            if (CollectionUtils.isNotEmpty(nodes)) {
////                Set<Node> nodeSet = nodes.stream().filter(Node::isDataNode).collect(Collectors.toSet());
////                List<ObjectId> connectionIds = nodeSet.stream().map(n -> MongoUtils.toObjectId(((TableNode) n).getConnectionId())).collect(Collectors.toList());
////                Criteria criteria = Criteria.where("_id").in(connectionIds);
////                List<DataSourceConnectionDto> allDto = dataSourceService.findAllDto(new Query(criteria), userDetail);
////                Map<ObjectId, DataSourceConnectionDto> connectionDtoMap = allDto.stream().collect(Collectors.toMap(DataSourceConnectionDto::getId, d -> d));
////                for (Node node : nodeSet) {
////                    TableNode tableNode = (TableNode) node;
////
////                    if (tableNode.getConnectionId() != null) {
////                        DataSourceConnectionDto dataSourceConnectionDto = connectionDtoMap.get(MongoUtils.toObjectId(tableNode.getConnectionId()));
////                        if (dataSourceConnectionDto != null) {
////                            com.tapdata.tm.commons.schema.DataSourceConnectionDto target = new com.tapdata.tm.commons.schema.DataSourceConnectionDto();
////                            BeanUtils.copyProperties(dataSourceConnectionDto, target);
////                            tableNode.setConnectionDto(target);
////                        }
////                    }
////                }
////            }
//        }
//
//        return subTaskDto;
//    }
//
//
//    public SubTaskDto findOne(Filter filter, UserDetail userDetail) {
//        Query query = repository.filterToQuery(filter);
//        SubTaskDto subTaskDto = findOne(query, userDetail);
//        return getSubTaskDto(userDetail, subTaskDto);
//    }
//
//    /**
//     * Paging query
//     *
//     * @param filter optional, page query parameters
//     * @return the Page of current page, include page data and total size.
//     */
//    public Page<SubTaskDto> find(Filter filter, UserDetail userDetail) {
//        Page<SubTaskDto> page = super.find(filter, userDetail);
//        List<SubTaskDto> items = page.getItems();
//        if (CollectionUtils.isNotEmpty(items)) {
//            for (SubTaskDto item : items) {
//                getSubTaskDto(userDetail, item);
//            }
//        }
//
//        return page;
//    }
//
//    /**
//     * 启动子任务
//     *
//     * @param id
//     */
//    public void start(ObjectId id, UserDetail user) {
//        start(id, user, "11");
//    }
//
//
//    /**
//     * 启动子任务
//     *
//     * @param id
//     * @param id
//     */
//    @Async
//    public void start(ObjectId id, UserDetail user, String startFlag) {
//        SubTaskDto subTaskDto = checkExistById(id);
//        TaskDto taskDto = taskService.findById(subTaskDto.getParentId(), user);
//        taskService.checkDagAgentConflict(taskDto, false);
//        subTaskDto.setParentTask(taskDto);
//        start(subTaskDto, user, startFlag);
//    }
//
//    /**
//     * 状态机启动子任务之前执行
//     *
//     * @param subTaskDto
//     * @param startFlag  字符串开关，
//     *                   第一位 是否需要共享挖掘处理， 1 是   0 否
//     *                   第二位 是否开启打点任务      1 是   0 否
//     */
//    private void start(SubTaskDto subTaskDto, UserDetail user, String startFlag) {
//
//        TaskDto parentTask = subTaskDto.getParentTask();
//
//        //日志挖掘
//        if (startFlag.charAt(0) == '1') {
//            logCollector(user, parentTask);
//        }
//
//        //打点任务，这个标识主要是防止任务跟子任务重复执行的
//        if (startFlag.charAt(1) == '1') {
//            startConnHeartbeat(user, parentTask);
//        }
//
//        //模型推演,如果模型已经存在，则需要推演
//        DAG dag = subTaskDto.getDag();
////        Map<String, List<Message>> schemaErrorMap = transformSchemaService.transformSchemaSync(dag, user, subTaskDto.getParentId());
////        if (!schemaErrorMap.isEmpty()) {
////            throw new BizException("Task.ListWarnMessage", schemaErrorMap);
////        }
//
//        //校验当前状态是否允许启动。
//        if (!SubTaskOpStatusEnum.to_start_status.v().contains(subTaskDto.getStatus())) {
//            log.warn("subTask current status not allow to start, subTask = {}, status = {}", subTaskDto.getName(), subTaskDto.getStatus());
//            throw new BizException("Task.StartStatusInvalid");
//        }
//
//
//        run(subTaskDto, user);
//
//    }
//
//    public void run(SubTaskDto subTaskDto, UserDetail user) {
//        //将子任务的状态改成启动
//        DAG dag = subTaskDto.getDag();
//        Query query = new Query(Criteria.where("id").is(subTaskDto.getId()).and("status").is(subTaskDto.getStatus()));
//        //需要将重启标识清除
//        UpdateResult update = update(query, Update.update("status", SubTaskDto.STATUS_SCHEDULING).set("isEdit", false).set("restartFlag", false), user);
//        if (update.getModifiedCount() == 0) {
//            //如果更新失败，则表示可能为并发启动操作，本次不做处理
//            log.info("concurrent start operations, this operation don‘t effective, subtask name = {}", subTaskDto.getName());
//            return;
//        }
//
//        TaskDto taskDto = subTaskDto.getParentTask();
//        if (StringUtils.equals(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), taskDto.getAccessNodeType())
//                && CollectionUtils.isNotEmpty(taskDto.getAccessNodeProcessIdList())) {
//            subTaskDto.setAgentId(taskDto.getAccessNodeProcessIdList().get(0));
//        } else {
//            subTaskDto.setAgentId(null);
//        }
//
//        workerService.scheduleTaskToEngine(subTaskDto, user, "SubTask", subTaskDto.getName());
//        CustomerJobLog customerJobLog = new CustomerJobLog(subTaskDto.getId().toString(), subTaskDto.getName());
//        customerJobLog.setDataFlowType(CustomerJobLogsService.DataFlowType.sync.getV());
//        if (StringUtils.isBlank(subTaskDto.getAgentId())) {
//            log.warn("No available agent found, task name = {}", subTaskDto.getName());
//            Query query1 = new Query(Criteria.where("_id").is(subTaskDto.getId()).and("status").is(SubTaskDto.STATUS_SCHEDULING));
//            update(query1, Update.update("status", SubTaskDto.STATUS_SCHEDULE_FAILED), user);
//            customerJobLogsService.noAvailableAgents(customerJobLog, user);
//            throw new BizException("Task.AgentNotFound");
//        }
//
//        WorkerDto workerDto = workerService.findOne(new Query(Criteria.where("processId").is(subTaskDto.getAgentId())));
//        customerJobLog.setAgentHost(workerDto.getHostname());
//        customerJobLogsService.assignAgent(customerJobLog, user);
//
//        //调度完成之后，改成待运行状态
//        Query query1 = new Query(Criteria.where("_id").is(subTaskDto.getId()).and("status").is(SubTaskDto.STATUS_SCHEDULING));
//        UpdateResult update1 = update(query1, Update.update("status", SubTaskDto.STATUS_WAIT_RUN).set("agentId", subTaskDto.getAgentId()), user);
//        if (update1.getModifiedCount() == 0) {
//            log.info("concurrent start operations, this operation don‘t effective, subtask name = {}", subTaskDto.getName());
//            return;
//        }
//        customerJobLog.setJobName(subTaskDto.getName());
//        customerJobLog.setJobInfos(TaskService.printInfos(dag));
//        customerJobLogsService.startJob(customerJobLog, user);
//        //发送websocket消息，提醒flowengin启动
//        DataSyncMq dataSyncMq = new DataSyncMq();
//        dataSyncMq.setTaskId(subTaskDto.getId().toHexString());
//        dataSyncMq.setOpType(DataSyncMq.OP_TYPE_START);
//        dataSyncMq.setType(MessageType.DATA_SYNC.getType());
//
//        Map<String, Object> data;
//        String json = JsonUtil.toJsonUseJackson(dataSyncMq);
//        data = JsonUtil.parseJsonUseJackson(json, Map.class);
//        MessageQueueDto queueDto = new MessageQueueDto();
//        queueDto.setReceiver(subTaskDto.getAgentId());
//        queueDto.setData(data);
//        queueDto.setType("pipe");
//
//        log.debug("build start subTask websocket context, processId = {}, userId = {}, queueDto = {}", subTaskDto.getAgentId(), user.getUserId(), queueDto);
//        messageQueueService.sendMessage(queueDto);
//
//        //创建任务执行历史记录（任务快照表)
//        //插入任务运行历史记录（TaskRunHistory）
//        createTaskSnapshot(subTaskDto, user, TaskRunHistoryDto.ACTION_RUN);
//    }
//
//
//    /**
//     * 暂停子任务
//     *
//     * @param id
//     */
//    public void pause(ObjectId id, UserDetail user, boolean force) {
//        SubTaskDto subTaskDto = checkExistById(id);
//        pause(subTaskDto, user, force);
//    }
//
//    /**
//     * 暂停子任务  将子任务停止，不清空中间状态
//     *
//     * @param subTaskDto 子任务
//     * @param user       用户
//     * @param force      是否强制停止
//     */
//    @Transactional
//    @Async
//    public void pause(SubTaskDto subTaskDto, UserDetail user, boolean force) {
//        pause(subTaskDto, user, force, false);
//    }
//
//    /**
//     * 暂停子任务  将子任务停止，不清空中间状态
//     *
//     * @param subTaskDto 子任务
//     * @param user       用户
//     * @param force      是否强制停止
//     */
//    //@Transactional
//    @Async
//    public void pause(SubTaskDto subTaskDto, UserDetail user, boolean force, boolean restart) {
//        //任务暂停的子任务状态只能是运行中
//        if (!SubTaskOpStatusEnum.to_stop_status.v().contains(subTaskDto.getStatus()) && !restart) {
//            log.warn("subTask current status not allow to pause, subTask = {}, status = {}", subTaskDto.getName(), subTaskDto.getStatus());
//            throw new BizException("Task.PauseStatusInvalid");
//        }
//
//        //重启的特殊处理，共享挖掘的比较多
//        if (SubTaskDto.STATUS_STOP.equals(subTaskDto.getStatus()) && restart) {
//            Update update = Update.update("restartFlag", true).set("restartUserId", user.getUserId());
//            Query query = new Query(Criteria.where("_id").is(subTaskDto.getId()));
//            update(query, update, user);
//            return;
//        }
//
//
//        String pauseStatus = SubTaskDto.STATUS_STOPPING;
//        if (force) {
//            pauseStatus = SubTaskDto.STATUS_STOP;
//        }
//
//        //将状态改为暂停中，给flowengin发送暂停消息，在回调的消息中将任务改为已暂停
//        Update update = Update.update("status", pauseStatus);
//        if (restart) {
//            update.set("restartFlag", true).set("restartUserId", user.getUserId());
//        }
//
//        Query query1 = new Query(Criteria.where("_id").is(subTaskDto.getId()).and("status").is(subTaskDto.getStatus()));
//        UpdateResult update1 = update(query1, update, user);
//        if (update1.getModifiedCount() == 0) {
//            //没有更新成功，说明可能是并发操作导致
//            log.info("concurrent pause operations, this operation don‘t effective, subtask name = {}", subTaskDto.getName());
//            return;
//        }
//        CustomerJobLog customerJobLog = new CustomerJobLog(subTaskDto.getId().toString(), subTaskDto.getName());
//        customerJobLog.setDataFlowType(CustomerJobLogsService.DataFlowType.sync.getV());
//        customerJobLog.setJobName(subTaskDto.getName());
//        if (force) {
//            customerJobLogsService.forceStopJob(customerJobLog, user);
//        } else {
//            customerJobLogsService.stopJob(customerJobLog, user);
//        }
//
//        DataSyncMq dataSyncMq = new DataSyncMq();
//        dataSyncMq.setTaskId(subTaskDto.getId().toHexString());
//        dataSyncMq.setForce(force);
//        dataSyncMq.setOpType(DataSyncMq.OP_TYPE_STOP);
//        dataSyncMq.setType(MessageType.DATA_SYNC.getType());
//
//        Map<String, Object> data;
//        String json = JsonUtil.toJsonUseJackson(dataSyncMq);
//        data = JsonUtil.parseJsonUseJackson(json, Map.class);
//        MessageQueueDto queueDto = new MessageQueueDto();
//        queueDto.setReceiver(subTaskDto.getAgentId());
//        queueDto.setData(data);
//        queueDto.setType("pipe");
//
//        log.debug("build stop subtask websocket context, processId = {}, userId = {}, queueDto = {}", subTaskDto.getAgentId(), user.getUserId(), queueDto);
//        messageQueueService.sendMessage(queueDto);
//
//        //创建任务执行历史记录（任务快照表)
//        //插入任务运行历史记录（TaskRunHistory）
//        createTaskSnapshot(subTaskDto, user, TaskRunHistoryDto.ACTION_STOP);
//    }
//
//
//    /**
//     * 收到子任务已经运行的消息
//     *
//     * @param id
//     */
//    public String running(ObjectId id, UserDetail user) {
//        //判断子任务是否存在
//        SubTaskDto subTaskDto = checkExistById(id, "_id", "status", "name");
//        //将子任务状态改成运行中
//        if (!SubTaskDto.STATUS_WAIT_RUN.equals(subTaskDto.getStatus())) {
//            log.info("concurrent runError operations, this operation don‘t effective, subtask name = {}", subTaskDto.getName());
//            return null;
//        }
//        Query query1 = new Query(Criteria.where("_id").is(subTaskDto.getId()).and("status").is(SubTaskDto.STATUS_WAIT_RUN));
//        UpdateResult update1 = update(query1, Update.update("status", SubTaskDto.STATUS_RUNNING).set("startTime", new Date()), user);
//        if (update1.getModifiedCount() == 0) {
//            log.info("concurrent running operations, this operation don‘t effective, subtask name = {}", subTaskDto.getName());
//            return null;
//        } else {
//            return id.toHexString();
//        }
//    }
//
//    /**
//     * 收到子任务运行失败的消息
//     *
//     * @param id
//     */
//    public String runError(ObjectId id, UserDetail user, String errMsg, String errStack) {
//        //判断子任务是否存在。
//        SubTaskDto subTaskDto = checkExistById(id, "_id", "status", "name");
//
//        if (!SubTaskOpStatusEnum.to_error_status.v().contains(subTaskDto.getStatus())) {
//            log.info("concurrent runError operations, this operation don‘t effective, subtask name = {}", subTaskDto.getName());
//            return null;
//        }
//        //将子任务状态更新成错误.
//        Query query1 = new Query(Criteria.where("_id").is(subTaskDto.getId()).and("status").in(SubTaskOpStatusEnum.to_error_status.v()));
//        UpdateResult update1 = update(query1, Update.update("status", SubTaskDto.STATUS_ERROR), user);
//        if (update1.getModifiedCount() == 0) {
//            log.info("concurrent runError operations, this operation don‘t effective, subtask name = {}", subTaskDto.getName());
//            return null;
//        } else {
//            return id.toHexString();
//        }
//
//    }
//
//    /**
//     * 收到子任务运行完成的消息
//     *
//     * @param id
//     */
//    public String complete(ObjectId id, UserDetail user) {
//        //判断子任务是否存在
//        SubTaskDto subTaskDto = checkExistById(id, "_id", "status", "name");
//        if (!SubTaskOpStatusEnum.to_complete_status.v().contains(subTaskDto.getStatus())) {
//            log.info("concurrent complete operations, this operation don‘t effective, subtask name = {}", subTaskDto.getName());
//            return null;
//        }
//        //将子任务状态更新成为已完成
//        Query query1 = new Query(Criteria.where("_id").is(subTaskDto.getId()).and("status").in(SubTaskOpStatusEnum.to_complete_status.v()));
//        UpdateResult update1 = update(query1, Update.update("status", SubTaskDto.STATUS_COMPLETE), user);
//        if (update1.getModifiedCount() == 0) {
//            log.info("concurrent complete operations, this operation don‘t effective, subtask name = {}", subTaskDto.getName());
//            return null;
//        } else {
//            return id.toHexString();
//        }
//    }
//
//    /**
//     * 收到子任务已经停止的消息
//     *
//     * @param id
//     */
//    public String stopped(ObjectId id, UserDetail user) {
//        //判断子任务是否存在。
//        SubTaskDto subTaskDto = checkExistById(id, "dag", "name", "status", "_id", "parentId");
//
//
//        //如果子任务状态为停止中，则将任务更新为已停止，并且清空所有运行信息
//        if (!SubTaskDto.STATUS_STOPPING.equals(subTaskDto.getStatus())) {
//            log.info("concurrent stopped operations, this operation don‘t effective, subtask name = {}", subTaskDto.getName());
//            return null;
//        }
//
//        Query query1 = new Query(Criteria.where("_id").is(subTaskDto.getId()).and("status").is(SubTaskDto.STATUS_STOPPING));
//
//        //endConnHeartbeat(user, subTaskDto);
//
//        UpdateResult update1 = update(query1, Update.update("status", SubTaskDto.STATUS_STOP), user);
//        if (update1.getModifiedCount() == 0) {
//            log.info("concurrent stopped operations, this operation don‘t effective, subtask name = {}", subTaskDto.getName());
//            return null;
//        } else {
//            return id.toHexString();
//        }
//    }
//
//
//    public void restart(ObjectId id, UserDetail user) {
//        SubTaskDto subTaskDto = checkExistById(id);
//
//
//        //重启之前改成待运行状态
//        updateById(subTaskDto.getId(), Update.update("status", SubTaskDto.STATUS_WAIT_RUN), user);
//
//        pause(subTaskDto, user, false, true);
//
//        //创建任务执行历史记录（任务快照表)
//        //插入任务运行历史记录（TaskRunHistory）
//        createTaskSnapshot(subTaskDto, user, TaskRunHistoryDto.ACTION_STOP);
//    }
//
//    public void restarted(ObjectId id, UserDetail user) {
//
//    }
//
//
//    /**
//     * 判断当前子任务的修改是否可以不暂停修改
//     * 目前只有在尾结点添加东西的时候才可以不停止原有子任务。
//     *
//     * @param old 修改前的dag
//     * @param cur 修改后的dag
//     * @return
//     */
//    public boolean canHotUpdate(DAG old, DAG cur) {
//        //如果当前节点数量连线数量应该大于等于老的节点数量
//        List<Node> curNodes = cur.getNodes();
//        List<Node> oldNodes = old.getNodes();
//
//        List<Edge> curEdges = cur.getEdges();
//        List<Edge> oldEdges = old.getEdges();
//
//        //不管新老节点或者连线为空，都直接报false
//        if (CollectionUtils.isEmpty(curNodes) || CollectionUtils.isEmpty(oldNodes)
//                || CollectionUtils.isEmpty(curEdges) || CollectionUtils.isEmpty(oldEdges)) {
//            return false;
//        }
//
//        if (curNodes.size() < oldNodes.size() || curEdges.size() < oldEdges.size()) {
//            return false;
//        }
//
//        //老的节点在当前更新中必须都存在。
//        //老的连线在当前更新中必须存在。
//        //之前存在的处理节点跟当前对应的处理节点参数必须一致
//        Map<String, Node> oldNodeMap = oldNodes.stream().collect(Collectors.toMap(Node::getId, n -> n));
//
//        for (Edge oldEdge : oldEdges) {
//            boolean eq = false;
//            for (Edge curEdge : curEdges) {
//                if (curEdge.equals(oldEdge)) {
//                    eq = true;
//                    break;
//                }
//            }
//            if (!eq) {
//                return false;
//            }
//        }
//
//
//        //新增连线的输出不能是老节点。
//        List<Edge> edges = curEdges.stream().filter(e -> !oldEdges.contains(e)).collect(Collectors.toList());
//        for (Edge curEdge : edges) {
//            if (oldNodeMap.get(curEdge.getTarget()) != null) {
//                return false;
//            }
//        }
//
//        return true;
//    }
//
//
//    private void createTaskSnapshot(SubTaskDto subTaskDto, UserDetail user, String action) {
//
//        long time = System.currentTimeMillis() - 300000L;
//        Date date = new Date(time);
//        Criteria criteriaHistory = Criteria.where("taskId").is(subTaskDto.getParentId()).and("createTime").gt(date);
//
//
//        long count = taskSnapshotService.count(new Query(criteriaHistory), user);
//        if (count == 0) {
//            //控制一下，五分钟同一个任务只能记录一条快照，如果是启动总任务，多个子任务同时启动，也只会记录一条记录
//            TaskDto taskDto = taskService.findById(subTaskDto.getParentId());
//            List<SubTaskDto> subTaskDtos = findByTaskId(subTaskDto.getParentId());
//            Criteria criteria = Criteria.where("taskId").is(subTaskDto.getParentId());
//            Query query = new Query(criteria);
//            TaskNodeRuntimeInfoDto taskNodeRuntimeInfoDto = taskNodeRuntimeInfoService.findOne(query);
//            TaskDatabaseRuntimeInfoDto taskDatabaseRuntimeInfoDto = taskDatabaseRuntimeInfoService.findOne(query);
//
//            TaskSnapshotsDto taskSnapshotDto = new TaskSnapshotsDto();
//            taskSnapshotDto.setTaskId(subTaskDto.getParentId());
//            taskSnapshotDto.setSnapshot(taskDto);
//            taskSnapshotDto.setSubtasks(subTaskDtos);
//            taskSnapshotDto.setNodeRunTimeInfo(taskNodeRuntimeInfoDto);
//            taskSnapshotDto.setDatabaseRunNodeTimeInfo(taskDatabaseRuntimeInfoDto);
//
//            taskSnapshotService.save(taskSnapshotDto, user);
//
//        }
//        TaskRunHistoryDto taskRunHistoryDto = new TaskRunHistoryDto();
//        taskRunHistoryDto.setTaskId(subTaskDto.getParentId());
//        taskRunHistoryDto.setSubTaskId(subTaskDto.getId());
//        taskRunHistoryDto.setSubTaskName(subTaskDto.getName());
//        taskRunHistoryDto.setAction(action);
//        taskRunHistoryService.save(taskRunHistoryDto, user);
//    }
//
//
//    public SubTaskDto checkExistById(ObjectId id) {
//        SubTaskDto subTaskDto = findById(id);
//        if (subTaskDto == null) {
//            throw new BizException("Task.subTaskNotFound");
//        }
//        return subTaskDto;
//    }
//
//    public SubTaskDto checkExistById(ObjectId id, String... fields) {
//        Field field = null;
//        if (fields != null && fields.length != 0) {
//            field = new Field();
//            for (String s : fields) {
//                field.put(s, true);
//            }
//        }
//
//        SubTaskDto subTaskDto = null;
//        if (field == null) {
//            subTaskDto = findById(id);
//        } else {
//            subTaskDto = findById(id, field);
//        }
//        if (subTaskDto == null) {
//            throw new BizException("Task.subTaskNotFound");
//        }
//        return subTaskDto;
//    }
//
//    /**
//     * 里程碑信息， 结构迁移信息， 全量同步信息，增量同步信息
//     * 里程碑信息为子任务表中的里程碑信息， 结构迁移与全量同步保存在节点运行中间状态表中。 增量同步信息保存在
//     *
//     * @param subId   子任务id
//     * @param endTime 前一次查询到的数据的结束时间， 本次查询应该为查询结束时间之后的数据， 为空则查询全部
//     */
//    public RunTimeInfo runtimeInfo(ObjectId subId, Long endTime, UserDetail user) {
//        log.debug("query subtask runtime info, subtask id = {}, endTime = {}, user = {}", subId, endTime, user);
//
//        //查询子任务是否存在
//        SubTaskDto subTaskDto = findById(subId, user);
//        if (subTaskDto == null) {
//            return null;
//        }
//        //查询所有的里程碑信息
//        List<Milestone> milestones = new ArrayList<>();
//        if (CollectionUtils.isNotEmpty(subTaskDto.getMilestones())) {
//            milestones.addAll(subTaskDto.getMilestones());
//        }
//        RunTimeInfo runTimeInfo = new RunTimeInfo();
//        runTimeInfo.setMilestones(milestones);
//
//        log.debug("runtime info ={}", runTimeInfo);
//        return runTimeInfo;
//    }
//
//    public UpdateResult update(Query query, Update update, UserDetail userDetail) {
//        UpdateResult update1 = repository.update(query, update, userDetail);
//
//        try {
//            Document queryObject = query.getQueryObject();
//            Object id = queryObject.get("id");
//            if (id == null) {
//                id = queryObject.get("_id");
//            }
//
//            if (id != null) {
//
//                flushStatus((ObjectId) id, userDetail);
//            } else {
//                query.fields().include("_id");
//                SubTaskDto one = findOne(query, userDetail);
//                flushStatus(one.getId(), userDetail);
//            }
//        } catch (Exception e) {
//            log.info("flush task status list failed");
//        }
//        return update1;
//    }
//
//    public UpdateResult updateById(ObjectId id, Update update, UserDetail userDetail) {
//        UpdateResult updateResult = super.updateById(id, update, userDetail);
//        flushStatus(id, userDetail);
//        return updateResult;
//    }
//
//    public void flushStatus(ObjectId id, UserDetail userDetail) {
//        Criteria criteria = Criteria.where("_id").is(id);
//        Query query = new Query(criteria);
//        query.fields().include("parentId");
//        SubTaskDto one = findOne(query);
//        flushStatusParentId(one.getParentId(), userDetail);
//    }
//
//    private void flushStatusParentId(ObjectId id, UserDetail userDetail) {
//        taskService.flushStatus(id, userDetail);
//    }
//
//
//    public long updateByWhere(Where where, Document doc, UserDetail user, String reqBody) {
//        long count = super.updateByWhere(where, doc, user);
//
//        if (count > 0 && reqBody.contains("\"status\"")) {
//            Criteria criteria = Criteria.where("_id").is(where.get("_id"));
//            Query query = new Query(criteria);
//            query.fields().include("parentId");
//            SubTaskDto subTaskDto = findOne(query, user);
//            if (subTaskDto != null) {
//                flushStatusParentId(subTaskDto.getParentId(), user);
//            }
//        }
//        return count;
//    }
//
//
//    /**
//     * 日志挖掘
//     *
//     * @param user
//     * @param oldTaskDto
//     */
//    public void logCollector(UserDetail user, TaskDto oldTaskDto) {
//
//        if (!oldTaskDto.getShareCdcEnable()) {
//            //任务没有开启共享挖掘
//            return;
//        }
//
//        //获取DAG所有的源节点并分组
//        DAG dag = oldTaskDto.getDag();
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
//        List<Node> allNodes = dag.getNodes();
//        List<Node> targets = dag.getTargets();
//        Set<String> sinkIds = targets.stream().map(Node::getId).collect(Collectors.toSet());
//
//        Map<ObjectId, List<Node>> group = allNodes.stream().
//                filter(Node::isDataNode)
//                .filter(n -> !sinkIds.contains(n.getId()))
//                .collect(Collectors.groupingBy(s -> MongoUtils.toObjectId(((DataParentNode<?>) s).getConnectionId())));
//        //查询获取所有源的数据源连接
//        Criteria criteria = Criteria.where("_id").in(group.keySet());
//        Query query = new Query(criteria);
//        query.fields().include("_id", "shareCdcEnable", "shareCdcTTL", "uniqueName", "database_type", "name");
//        List<DataSourceConnectionDto> dataSourceDtos = dataSourceService.findAllDto(query, user);
//
//        //根据数据源连接
//        Set<String> sourceUniqSet = new HashSet<>();
//        List<DataSourceConnectionDto> _dataSourceDtos = new ArrayList<>();
//        List<DataSourceConnectionDto> createLogCollects = new ArrayList<>();
//        for (DataSourceConnectionDto dataSourceDto : dataSourceDtos) {
//            if (dataSourceDto.getShareCdcEnable() == null || !dataSourceDto.getShareCdcEnable()) {
//                continue;
//            }
//
//            String uniqueName = dataSourceDto.getUniqueName();
//
//            if (sourceUniqSet.contains(uniqueName)) {
//                _dataSourceDtos.add(dataSourceDto);
//            }
//
//            if (StringUtils.isBlank(uniqueName) || !sourceUniqSet.contains(uniqueName)) {
//                createLogCollects.add(dataSourceDto);
//                sourceUniqSet.add(uniqueName);
//            }
//        }
//
//        _dataSourceDtos.addAll(createLogCollects);
//        dataSourceDtos = _dataSourceDtos;
//
//        Map<String, List<DataSourceConnectionDto>> datasourceMap = dataSourceDtos.stream().collect(Collectors.groupingBy(d -> StringUtils.isBlank(d.getUniqueName()) ? d.getId().toHexString() : d.getUniqueName()));
//
//
//        //不同类型数据源的id缓存
//        Map<String, List<DataSourceConnectionDto>> dataSourceCacheByType = new HashMap<>();
//
//        //数据源id对应创建的挖掘任务id
//        Map<String, String> newLogCollectorMap = new HashMap<>();
//
//        datasourceMap.forEach((k, v) -> {
//
//
//            //获取需要日志挖掘的表名
//            List<String> tableNames = new ArrayList<>();
//            for (DataSourceConnectionDto d : v) {
//                List<Node> nodes = group.get(d.getId());
//                for (Node node : nodes) {
//                    if (node instanceof TableNode) {
//
//                        tableNames.add(((TableNode) node).getTableName());
//
//                    } else if (node instanceof DatabaseNode) {
//                        tableNames = ((DatabaseNode) node).getSourceNodeTableNames();
//                    }
//                }
//            }
//
//            //查询是否存在相同的日志挖掘任务，存在，并且表也存在，则不处理
//            //根据unique name查询，或者根据id查询
//            DataSourceConnectionDto dataSource = v.get(0);
//            List<String> ids = new ArrayList<>();
//
//            //如果没有uniqname,则唯一键采用的id，所以不会存在相似的数据源
//            if (StringUtils.isBlank(dataSource.getUniqueName())) {
//                ids.add(dataSource.getId().toHexString());
//
//            } else {
//
//                List<DataSourceConnectionDto> cache = dataSourceCacheByType.get(dataSource.getDatabase_type());
//                if (CollectionUtils.isEmpty(cache)) {
//                    Criteria criteria1 = Criteria.where("database_type").is(dataSource.getDatabase_type());
//                    Query query1 = new Query(criteria1);
//                    query1.fields().include("_id", "uniqueName");
//                    cache = dataSourceService.findAllDto(query1, user);
//                    dataSourceCacheByType.put(dataSource.getDatabase_type(), cache);
//
//                }
//
//
//                ids = cache.stream().filter(c -> dataSource.getUniqueName().equals(c.getUniqueName())).map(d -> d.getId().toHexString()).collect(Collectors.toList());
//            }
//
//
//            Criteria criteria1 = Criteria.where("is_deleted").is(false).and("dag.nodes").elemMatch(Criteria.where("type").is("logCollector").and("connectionIds").elemMatch(Criteria.where("$in").is(ids)));
//            Query query1 = new Query(criteria1);
//            query1.fields().include("dag", "statuses");
//            List<String> connectionIds = v.stream().map(d -> d.getId().toHexString()).collect(Collectors.toList());
//            TaskDto oldLogCollectorTask = taskService.findOne(query1, user);
//            if (oldLogCollectorTask != null) {
//                List<Node> sources1 = oldLogCollectorTask.getDag().getSources();
//                LogCollectorNode logCollectorNode = (LogCollectorNode) sources1.get(0);
//                List<String> oldTableNames = logCollectorNode.getTableNames();
//                List<SubTaskDto> id1s = findByTaskId(oldLogCollectorTask.getId(), user, "_id");
//                SubTaskDto subTaskDto = id1s.get(0);
//                for (String id : ids) {
//                    newLogCollectorMap.put(id, subTaskDto.getId().toHexString());
//                }
//
//                List<String> oldConnectionIds = logCollectorNode.getConnectionIds();
//
//                boolean updateConnectionId = false;
//                for (String connectionId : connectionIds) {
//                    if (!oldConnectionIds.contains(connectionId)) {
//                        oldConnectionIds.add(connectionId);
//                        updateConnectionId = true;
//                    }
//                }
//
//                if (CollectionUtils.isNotEmpty(oldTableNames) && oldTableNames.containsAll(tableNames)) {
//                    //检查状态，如果状态不是启动的，需要启动起来
//                    List<SubStatus> statuses = oldLogCollectorTask.getStatuses();
//                    if (updateConnectionId) {
//                        taskService.confirmById(oldLogCollectorTask, user, true);
//                    }
//                    if (CollectionUtils.isNotEmpty(statuses)) {
//                        SubStatus subStatus = statuses.get(0);
//                        if (SubTaskDto.STATUS_RUNNING.equals(subStatus.getStatus())) {
//                            return;
//                        }
//                    }
//
//
//                    taskService.start(oldLogCollectorTask.getId(), user);
//                    return;
//                }
//
//                tableNames.addAll(oldTableNames);
//                tableNames = tableNames.stream().distinct().collect(Collectors.toList());
//                logCollectorNode.setTableNames(tableNames);
//                taskService.confirmById(oldLogCollectorTask, user, true);
//                updateLogCollectorMap(oldTaskDto.getId(), newLogCollectorMap, user);
//                //这个stop是异步的， 需要重启，重启的逻辑是通过定时任务跑的
//                taskService.stop(oldLogCollectorTask.getId(), user, false, true);
//                return;
//            }
//
//
//            LogCollectorNode logCollectorNode = new LogCollectorNode();
//            logCollectorNode.setId(UUIDUtil.getUUID());
//            logCollectorNode.setConnectionIds(connectionIds);
//            logCollectorNode.setDatabaseType(v.get(0).getDatabase_type());
//            logCollectorNode.setName(UUIDUtil.getUUID());
//
//
//            logCollectorNode.setTableNames(tableNames);
//            logCollectorNode.setSelectType(LogCollectorNode.SELECT_TYPE_RESERVATION);
//
//            HazelCastImdgNode hazelCastImdgNode = new HazelCastImdgNode();
//            hazelCastImdgNode.setId(UUIDUtil.getUUID());
//            hazelCastImdgNode.setName(hazelCastImdgNode.getId());
//
//            List<Node> nodes = Lists.newArrayList(logCollectorNode, hazelCastImdgNode);
//
//            Edge edge = new Edge(logCollectorNode.getId(), hazelCastImdgNode.getId());
//            List<Edge> edges = Lists.newArrayList(edge);
//            Dag dag1 = new Dag(edges, nodes);
//            DAG build = DAG.build(dag1);
//            TaskDto taskDto = new TaskDto();
//            taskDto.setName("来自" + dataSource.getName() + "的共享挖掘任务");
//            taskDto.setDag(build);
//            taskDto.setType("cdc");
//            taskDto.setSyncType("logCollector");
//            taskDto = taskService.create(taskDto, user);
//            taskDto = taskService.confirmById(taskDto, user, true);
//
//            //保存新增挖掘任务id到子任务中
//            for (String id : ids) {
//                Map<String, Object> attrs = taskDto.getAttrs();
//                if (attrs != null) {
//                    newLogCollectorMap.put(id, (String) attrs.get(TaskService.LOG_COLLECTOR_SAVE_ID));
//                }
//            }
//
//            taskService.start(taskDto.getId(), user);
//        });
//
//        updateLogCollectorMap(oldTaskDto.getId(), newLogCollectorMap, user);
//    }
//
//    /**
//     * 根据同步任务启动连接心跳打点任务
//     * <pre>
//     * 启动条件：
//     *   - 同步任务启动时检查，并启动连接对应打点任务
//     *   - 同步任务类型为：迁移、同步
//     *   - 同步任务包含增量同步（全量+增量、增量）
//     *   - 同步任务源连接非 dummy 节点，并支持增量同步
//     * 打点任务逻辑：
//     *   - 增量任务
//     *   - 一个打点任务对应一个连接的心跳数据生成
//     *   - 源为 dummy 节点，mode=ConnHeartbeat
//     *   - 目标为任务源节点
//     * </pre>
//     *
//     * @param user       操作用户信息
//     * @param oldTaskDto 启动任务
//     */
//    public void startConnHeartbeat(UserDetail user, TaskDto oldTaskDto) {
//        if (!ConnHeartbeatUtils.checkTask(oldTaskDto.getType(), oldTaskDto.getSyncType())) return;
//        log.info("start connection heartbeat: {}({})", oldTaskDto.getId(), oldTaskDto.getName());
//
//        String subTaskId;
//        DataSourceConnectionDto heartbeatConnection = null;
//        List<DataSourceConnectionDto> dataSourceDtos;
//        Set<String> joinConnectionIdSet = new HashSet<>();
//        Map<String, List<DataSourceConnectionDto>> dataSourceCacheByType = new HashMap<>(); //不同类型数据源的id缓存
//        List<SubTaskDto> subTaskDtos = findByTaskId(oldTaskDto.getId(), user, "_id", "dag");
//        for (SubTaskDto subTaskDto : subTaskDtos) {
//            subTaskId = subTaskDto.getId().toHexString();
//            dataSourceDtos = getConnectionByDag(user, subTaskDto.getDag());
//            for (DataSourceConnectionDto dataSource : dataSourceDtos) {
//                String dataSourceId = dataSource.getId().toHexString();
//                if (!ConnHeartbeatUtils.checkConnection(dataSource.getDatabase_type(), dataSource.getCapabilities()) || joinConnectionIdSet.contains(dataSourceId))
//                    continue;
//
//                //如果连接已经有心跳任务，将连接任务编号添加到 heartbeatTasks，并尝试启动任务
//                List<String> connectionIds = getConnectionIds(user, dataSourceCacheByType, dataSource);
//                TaskDto oldConnHeartbeatTask = queryTask(user, connectionIds);
//                if (oldConnHeartbeatTask != null) {
//                    HashSet<String> heartbeatTasks = Optional.ofNullable(oldConnHeartbeatTask.getHeartbeatTasks()).orElseGet(HashSet::new);
//                    heartbeatTasks.add(subTaskId);
//                    taskService.update(new Query(Criteria.where("_id").is(oldConnHeartbeatTask.getId())), Update.update(ConnHeartbeatUtils.TASK_RELATION_FIELD, heartbeatTasks), user);
//                    if (!SubTaskDto.STATUS_RUNNING.equals(oldConnHeartbeatTask.getStatus())) {
//                        taskService.start(oldConnHeartbeatTask.getId(), user);
//                    }
//                    joinConnectionIdSet.add(dataSourceId);
//                    continue;
//                }
//
//                //循环中只需要获取一次dummy源跟打点模型表
//                if (heartbeatConnection == null) {
//                    boolean addDummy = false;
//                    //获取打点的Dummy数据源
//                    Query query2 = new Query(Criteria.where("database_type").is(ConnHeartbeatUtils.PDK_NAME)
//                            .and("createType").is(CreateTypeEnum.System)
//                            .and("config.mode").is(ConnHeartbeatUtils.MODE)
//                    );
//                    heartbeatConnection = dataSourceService.findOne(query2, user);
//                    if (heartbeatConnection == null) {
//                        Query query3 = new Query(Criteria.where("pdkId").is(ConnHeartbeatUtils.PDK_ID));
//                        query3.fields().include("pdkHash", "type");
//                        DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.findOne(query3);
//
//                        heartbeatConnection = new DataSourceConnectionDto();
//                        heartbeatConnection.setName(ConnHeartbeatUtils.CONNECTION_NAME);
//                        heartbeatConnection.setConfig(Optional.of(new LinkedHashMap<String, Object>()).map(m -> {
//                            m.put("mode", ConnHeartbeatUtils.MODE);
//                            m.put("connId", dataSourceId);
//                            return m;
//                        }).get());
//                        heartbeatConnection.setConnection_type("source");
//                        heartbeatConnection.setPdkType("pdk");
//                        heartbeatConnection.setRetry(0);
//                        heartbeatConnection.setStatus("testing");
//                        heartbeatConnection.setShareCdcEnable(false);
//                        heartbeatConnection.setDatabase_type(definitionDto.getType());
//                        heartbeatConnection.setPdkHash(definitionDto.getPdkHash());
//                        heartbeatConnection.setCreateType(CreateTypeEnum.System);
//                        heartbeatConnection = dataSourceService.add(heartbeatConnection, user);
//                        dataSourceService.sendTestConnection(heartbeatConnection, true, true, user); //添加后没加载模型，手动加载一次
//                        addDummy = true;
//                    }
//
//                    String qualifiedName = MetaDataBuilderUtils.generateQualifiedName("table", heartbeatConnection, "heartbeatTable");
//                    MetadataInstancesDto metadata = metadataInstancesService.findByQualifiedNameNotDelete(qualifiedName, user, "_id");
//                    if (metadata == null) {
//                        if (!addDummy) {
//                            //新增数据源的时候 我自动加载模型
//                            dataSourceService.sendTestConnection(heartbeatConnection, true, true, user);
//                        }
//
//                        for (int i = 0; i < 8; i++) {
//                            if (metadataInstancesService.findByQualifiedNameNotDelete(qualifiedName, user, "_id") == null) {
//                                try {
//                                    Thread.sleep(500 * i);
//                                } catch (InterruptedException e) {
//                                    throw new BizException("SystemError");
//                                }
//                            }
//
//                        }
//                    }
//                }
//
//                TaskDto taskDto = ConnHeartbeatUtils.generateTask(subTaskId, dataSourceId, dataSource.getName(), dataSource.getDatabase_type(), heartbeatConnection.getId().toHexString(), heartbeatConnection.getDatabase_type());
//                taskDto = taskService.create(taskDto, user);
//                taskDto = taskService.confirmById(taskDto, user, true);
//                taskService.start(taskDto.getId(), user);
//                joinConnectionIdSet.add(dataSourceId);
//            }
//        }
//
//    }
//
//
//    private List<String> getConnectionIds(UserDetail user, Map<String, List<DataSourceConnectionDto>> dataSourceCacheByType, DataSourceConnectionDto dataSource) {
//        List<String> ids = new ArrayList<>();
//
//        //如果没有uniqname,则唯一键采用的id，所以不会存在相似的数据源
//        if (StringUtils.isBlank(dataSource.getUniqueName())) {
//            ids.add(dataSource.getId().toHexString());
//
//        } else {
//
//            List<DataSourceConnectionDto> cache = dataSourceCacheByType.get(dataSource.getDatabase_type());
//            if (CollectionUtils.isEmpty(cache)) {
//                Criteria criteria1 = Criteria.where("database_type").is(dataSource.getDatabase_type());
//                Query query1 = new Query(criteria1);
//                query1.fields().include("_id", "uniqueName");
//                cache = dataSourceService.findAllDto(query1, user);
//                dataSourceCacheByType.put(dataSource.getDatabase_type(), cache);
//
//            }
//
//
//            ids = cache.stream().filter(c -> dataSource.getUniqueName().equals(c.getUniqueName())).map(d -> d.getId().toHexString()).collect(Collectors.toList());
//        }
//        return ids;
//    }
//
//    public void endConnHeartbeat(UserDetail user, SubTaskDto subTaskDto) {
//        log.info("stop connection heartbeat: {}({})", subTaskDto.getId(), subTaskDto.getName());
//        TaskDto parentTask = subTaskDto.getParentTask();
//        parentTask = null != parentTask ? parentTask : taskService.findById(subTaskDto.getParentId());
//        if (null == parentTask) {
//            log.warn("end connections heartbeat not found parent task: {}({})", subTaskDto.getId(), subTaskDto.getName());
//            return;
//        }
//
//        if (!ConnHeartbeatUtils.checkTask(parentTask.getType(), parentTask.getSyncType())) return;
//
//        //不同类型数据源的id缓存
//        Map<String, List<DataSourceConnectionDto>> dataSourceCacheByType = new HashMap<>();
//        List<DataSourceConnectionDto> dataSourceDtos = getConnectionByDag(user, subTaskDto.getDag());
//        for (DataSourceConnectionDto dataSource : dataSourceDtos) {
//            //获取是否存在这些connectionIds作为源的任务。
//            List<String> connectionIds = getConnectionIds(user, dataSourceCacheByType, dataSource);
//            TaskDto oldConnHeartbeatTask = queryTask(user, connectionIds);
//            if (null == oldConnHeartbeatTask) {
//                log.warn("not found heartbeat task, connId: {}", dataSource.getId().toHexString());
//                continue;
//            }
//            HashSet<String> heartbeatTasks = oldConnHeartbeatTask.getHeartbeatTasks();
//            if (heartbeatTasks.remove(subTaskDto.getId().toHexString())) {
//                taskService.update(new Query(Criteria.where("_id").is(oldConnHeartbeatTask.getId())), Update.update(ConnHeartbeatUtils.TASK_RELATION_FIELD, heartbeatTasks), user);
//            }
//            if (heartbeatTasks.size() == 0) {
//                taskService.stop(oldConnHeartbeatTask.getId(), user, false);
//            }
//        }
//    }
//
//    private TaskDto queryTask(UserDetail user, List<String> ids) {
//        Criteria criteria1 = Criteria.where("is_deleted").is(false)
//                .and("syncType").is(TaskDto.SYNC_TYPE_CONN_HEARTBEAT)
//                .and("dag.nodes").elemMatch(Criteria.where("connectionId").in(ids));
//        Query query1 = new Query(criteria1);
//        query1.fields().include("dag", "status", ConnHeartbeatUtils.TASK_RELATION_FIELD);
//        TaskDto oldConnHeartbeatTask = taskService.findOne(query1, user);
//        return oldConnHeartbeatTask;
//    }
//
//    private List<DataSourceConnectionDto> getConnectionByDag(UserDetail user, DAG dag) {
//        List<Node> sources = dag.getSources();
//
//        Set<String> connectionIds = sources.stream().map(n -> ((DataParentNode) n).getConnectionId()).collect(Collectors.toSet());
//        //查询获取所有源的数据源连接
//        Criteria criteria = Criteria.where("_id").in(connectionIds);
//        Query query = new Query(criteria);
//        query.fields().include("_id", "uniqueName", "database_type", "name", "capabilities");
//        List<DataSourceConnectionDto> dataSourceDtos = dataSourceService.findAllDto(query, user);
//        return dataSourceDtos;
//    }
//
//    //由于调用这个方法的都是异步方法，不存在返回时长问题，所以这里直接使用sleep等待
//    private void delayCheckSubTaskStatus(ObjectId taskId, String subStatus, UserDetail user) {
//        Criteria criteria = Criteria.where("parentId").is(taskId);
//        Query query = new Query(criteria);
//        query.fields().include("status");
//        level1:
//        //如果16秒钟还没有等到想要的结果，就不再继续等待了
//        for (int i = 0; i < 100; i++) {
//            int ms = (1 << i) * 1000;
//            try {
//                Thread.sleep(ms);
//            } catch (InterruptedException e) {
//                log.warn("thread sleep interrupt exception, e = {}", e.getMessage());
//                return;
//            }
//
//            List<SubTaskDto> allDto = findAllDto(query, user);
//            for (SubTaskDto subTaskDto : allDto) {
//                if (!subStatus.equals(subTaskDto.getStatus())) {
//                    break;
//                }
//                break level1;
//            }
//        }
//    }
//
//
//    //将启动的挖掘任务id更新到任务中去
//    private void updateLogCollectorMap(ObjectId taskId, Map<String, String> newLogCollectorMap, UserDetail user) {
//        List<SubTaskDto> subTaskDtos = findByTaskId(taskId, "dag", "_id");
//        if (CollectionUtils.isEmpty(subTaskDtos)) {
//            return;
//        }
//
//        if (newLogCollectorMap == null || newLogCollectorMap.isEmpty()) {
//
//            return;
//        }
//
//        for (SubTaskDto subTaskDto : subTaskDtos) {
//            DAG dag = subTaskDto.getDag();
//            List<Node> sources = dag.getSources();
//            Map<String, String> shareCdcTaskId = subTaskDto.getShareCdcTaskId();
//            if (shareCdcTaskId == null) {
//                shareCdcTaskId = new HashMap<>();
//                subTaskDto.setShareCdcTaskId(shareCdcTaskId);
//            }
//
//            for (Node source : sources) {
//                if (source instanceof DataParentNode) {
//                    String id = ((DataParentNode<?>) source).getConnectionId();
//                    if (newLogCollectorMap.get(id) != null) {
//                        shareCdcTaskId.put(id, newLogCollectorMap.get(id));
//                    }
//                }
//            }
//
//            Update update = new Update();
//            update.set("shareCdcTaskId", shareCdcTaskId);
//            updateById(subTaskDto.getId(), update, user);
//        }
//
//
//    }
//
//    public void updateNode(ObjectId objectId, String nodeId, Document param, UserDetail user) {
//        SubTaskDto subTaskDto = checkExistById(objectId);
//        Criteria criteria = Criteria.where("_id").is(objectId).and("dag.nodes").elemMatch(Criteria.where("id").is(nodeId));
//        Document set = (Document) param.get("$set");
//        for (String s : set.keySet()) {
//            set.put("dag.nodes.$." + s, set.get(s));
//            set.remove(s);
//        }
//        param.put("$set", set);
//
//        Update update = Update.fromDocument(param);
//        update(new Query(criteria), update, user);
//
//        ObjectId parentId = subTaskDto.getParentId();
//        Criteria taskCriteria = Criteria.where("_id").is(parentId).and("dag.nodes").elemMatch(Criteria.where("id").is(nodeId));
//        criteria.and("_id").is(parentId);
//        taskService.update(new Query(taskCriteria), update, user);
//    }
//
//
//    public void updateSyncProgress(ObjectId subTaskId, Document document) {
//        document.forEach((k, v) -> {
//            Criteria criteria = Criteria.where("_id").is(subTaskId);
//            Update update = new Update().set("attrs.syncProgress." + k, v);
//            update(new Query(criteria), update);
//        });
//    }
//
//    public List<IncreaseSyncVO> increaseView(String subTaskId) {
//        SubTaskDto subTaskDto = checkExistById(MongoUtils.toObjectId(subTaskId));
//        Criteria criteria = Criteria.where("tags.subTaskId").is(subTaskId).and("tags.type").is("node");
//        Query query = new Query(criteria);
//        MongoTemplate mongoTemplate = repository.getMongoOperations();
//        List<AgentStatDto> agentStatDtos = mongoTemplate.find(query, AgentStatDto.class);
//        Map<String, AgentStatDto> agentStatMap = agentStatDtos.stream().collect(Collectors.toMap(a -> a.getTags().getNodeId(), a -> a, (a, a1) -> a1));
//        DAG dag = subTaskDto.getDag();
//        List<Edge> fullEdges = fullEdges(dag);
//
//        List<IncreaseSyncVO> increaseSyncVOS = new ArrayList<>();
//        for (Edge edge : fullEdges) {
//            String source = edge.getSource();
//            String target = edge.getTarget();
//
//            DataParentNode sourceNode = (DataParentNode) dag.getNode(source);
//            DataParentNode targetNode = (DataParentNode) dag.getNode(target);
//
//            AgentStatDto sourceAgentStatDto = agentStatMap.get(source);
//            AgentStatDto targetAgentStatDto = agentStatMap.get(target);
//
//
//            IncreaseSyncVO increaseSyncVO = new IncreaseSyncVO();
//            increaseSyncVO.setSrcId(sourceNode.getId());
//            increaseSyncVO.setSrcConnId(sourceNode.getConnectionId());
//            if (sourceNode instanceof TableNode) {
//                increaseSyncVO.setSrcTableName(((TableNode) sourceNode).getTableName());
//            }
//            increaseSyncVO.setTgtId(targetNode.getId());
//            increaseSyncVO.setTgtConnId(targetNode.getConnectionId());
//            if (targetNode instanceof TableNode) {
//                increaseSyncVO.setTgtTableName(((TableNode) targetNode).getTableName());
//            }
//            increaseSyncVO.setDelay(0L);
//            if (targetAgentStatDto != null) {
//                double delay = targetAgentStatDto.getStatistics().getReplicateLag();
//                if (delay > 0) {
//                    increaseSyncVO.setDelay((long) delay);
//                }
//            }
//
//            if (sourceAgentStatDto != null) {
//                double cdcTime = sourceAgentStatDto.getStatistics().getCdcTime();
//                if (cdcTime != 0) {
//                    increaseSyncVO.setCdcTime(new Date((long) cdcTime));
//                }
//            }
//
//            increaseSyncVOS.add(increaseSyncVO);
//
//        }
//
//        List<String> connectionIds = new ArrayList<>();
//        for (IncreaseSyncVO increaseSyncVO : increaseSyncVOS) {
//            if (StringUtils.isNotBlank(increaseSyncVO.getSrcId())) {
//                connectionIds.add(increaseSyncVO.getSrcConnId());
//            }
//
//            if (StringUtils.isNotBlank(increaseSyncVO.getTgtId())) {
//                connectionIds.add(increaseSyncVO.getTgtConnId());
//            }
//        }
//
//        Criteria idCriteria = Criteria.where("_id").in(connectionIds);
//        Query query1 = new Query(idCriteria);
//        List<DataSourceConnectionDto> connections = dataSourceService.findAll(query1);
//        Map<String, String> connectionNameMap = connections.stream().collect(Collectors.toMap(d -> d.getId().toHexString(), DataSourceConnectionDto::getName));
//        for (IncreaseSyncVO increaseSyncVO : increaseSyncVOS) {
//            increaseSyncVO.setSrcName(connectionNameMap.get(increaseSyncVO.getSrcConnId()));
//            increaseSyncVO.setTgtName(connectionNameMap.get(increaseSyncVO.getTgtConnId()));
//        }
//
//        return increaseSyncVOS;
//    }
//
//
//    public List<Edge> fullEdges(DAG dag) {
//        List<Edge> edges = dag.getEdges();
//        List<Edge> fullEdges = new ArrayList<>();
//        for (Edge edge : edges) {
//            Node source = dag.getNode(edge.getSource());
//            if (!source.isDataNode()) {
//                continue;
//            }
//
//            Node target = dag.getNode(edge.getTarget());
//            if (target.isDataNode()) {
//                fullEdges.add(new Edge(source.getId(), target.getId()));
//            }
//
//            fullEdges.addAll(successorEdges(source.getId(), target, dag));
//        }
//        //去掉重复的
//        Map<String, Edge> collect = fullEdges.stream().collect(Collectors.toMap(e -> e.getSource() + e.getTarget(), e -> e));
//        fullEdges = new ArrayList<>(collect.values());
//        return fullEdges;
//    }
//
//    private List<Edge> successorEdges(String source, Node target, DAG dag) {
//        List<Edge> fullEdges = new ArrayList<>();
//        List<Node> successors = dag.successors(target.getId());
//        for (Node successor : successors) {
//            if (successor.isDataNode()) {
//                fullEdges.add(new Edge(source, successor.getId()));
//            } else if (successor.getType().endsWith("_processor")) {
//                fullEdges.addAll(successorEdges(source, successor, dag));
//            }
//        }
//
//        return fullEdges;
//
//    }
//
//
//    public void increaseClear(ObjectId subTaskId, String srcNode, String tgtNode, UserDetail user) {
//        //清理只需要清楚syncProgress数据就行
//        SubTaskDto subTaskDto = checkExistById(subTaskId, "attrs");
//        clear(srcNode, tgtNode, user, subTaskDto);
//
//    }
//
//    private void clear(String srcNode, String tgtNode, UserDetail user, SubTaskDto subTaskDto) {
//        Map<String, Object> attrs = subTaskDto.getAttrs();
//        Object syncProgress = attrs.get("syncProgress");
//        if (syncProgress == null) {
//            return;
//        }
//
//        Map syncProgressMap = (Map) syncProgress;
//        List<String> key = Lists.newArrayList(srcNode, tgtNode);
//
//        syncProgressMap.remove(JsonUtil.toJsonUseJackson(key));
//
//        Update update = Update.update("attrs", attrs);
//        //不需要刷新主任状态， 所以调用super, 本来中重新的自带刷新主任务状态
//        super.updateById(subTaskDto.getId(), update, user);
//    }
//
//    public void increaseBacktracking(ObjectId subTaskId, String srcNode, String tgtNode, TaskDto.SyncPoint point, UserDetail user) {
//        SubTaskDto subTaskDto = checkExistById(subTaskId, "parentId", "attrs", "dag");
//        clear(srcNode, tgtNode, user, subTaskDto);
//
//
//        //更新主任务中的syncPoints时间点
//        Criteria criteria = Criteria.where("_id").is(subTaskDto.getParentId());
//        Query query = new Query(criteria);
//        query.fields().include("syncPoints");
//        DAG dag = subTaskDto.getDag();
//        Node node = dag.getNode(tgtNode);
//        if (node instanceof DataParentNode) {
//            String connectionId = ((DataParentNode<?>) node).getConnectionId();
//            if (StringUtils.isNotBlank(connectionId)) {
//                TaskDto taskDto = taskService.findOne(query, user);
//                List<TaskDto.SyncPoint> syncPoints = taskDto.getSyncPoints();
//                if (CollectionUtils.isEmpty(syncPoints)) {
//                    syncPoints = new ArrayList<>();
//                }
//
//
//                boolean exist = false;
//                TaskDto.SyncPoint syncPoint = new TaskDto.SyncPoint();
//                for (TaskDto.SyncPoint item : syncPoints) {
//                    if (connectionId.equals(item.getConnectionId())) {
//                        syncPoint = item;
//                        exist = true;
//                        break;
//                    }
//                }
//
//                syncPoint.setPointType(point.getPointType());
//                syncPoint.setDateTime(point.getDateTime());
//                syncPoint.setTimeZone(point.getTimeZone());
//                syncPoint.setConnectionId(connectionId);
//
//                if (exist) {
//                    Criteria criteriaPoint = Criteria.where("_id").is(subTaskDto.getParentId()).and("syncPoints")
//                            .elemMatch(Criteria.where("connectionId").is(connectionId));
//                    Update update = Update.update("syncPoints.$", syncPoint);
//                    //更新内嵌文档
//                    taskService.update(new Query(criteriaPoint), update);
//                } else {
//                    syncPoints.add(syncPoint);
//                    Criteria criteriaPoint = Criteria.where("_id").is(subTaskDto.getParentId());
//                    Update update = Update.update("syncPoints", syncPoints);
//                    taskService.update(new Query(criteriaPoint), update);
//                }
//            }
//        }
//
//    }
//
//
//    public void rename(ObjectId taskId, String newTaskName) {
//        List<SubTaskDto> subTaskDtos = findByTaskId(taskId, "name");
//        BulkOperations bulkOperations = repository.bulkOperations(BulkOperations.BulkMode.UNORDERED);
//
//        for (SubTaskDto subTaskDto : subTaskDtos) {
//            String name = subTaskDto.getName();
//            int indexOf = name.indexOf(" (");
//            name = newTaskName + name.substring(indexOf);
//
//            Criteria criteria = Criteria.where("_id").is(subTaskDto.getId());
//            Query query = new Query(criteria);
//
//            Update update = Update.update("name", name);
//            bulkOperations.updateOne(query, update);
//        }
//
//        if (CollectionUtils.isNotEmpty(subTaskDtos)) {
//            bulkOperations.execute();
//        }
//    }
//
//    public void reseted(ObjectId objectId, UserDetail userDetail) {
//        SubTaskDto subTaskDto = checkExistById(objectId, "_id");
//        if (subTaskDto != null) {
//            super.updateById(objectId, Update.update("resetFlag", true), userDetail);
//        }
//    }
//
//    public void deleted(ObjectId objectId, UserDetail userDetail) {
//        SubTaskDto subTaskDto = checkExistById(objectId, "_id");
//        if (subTaskDto != null) {
//            super.updateById(objectId, Update.update("deleteFlag", true), userDetail);
//        }
//    }
//
//    public boolean checkPdkSubTask(SubTaskDto subTaskDto, UserDetail user) {
//        DAG dag = subTaskDto.getDag();
//        if (dag == null) {
//            return false;
//        }
//        List<String> connections = new ArrayList<>();
//        boolean specialTask = false;
//        List<Node> sources = dag.getSources();
//        for (Node source : sources) {
//            if (source instanceof LogCollectorNode) {
//                List<String> connectionIds = ((LogCollectorNode) source).getConnectionIds();
//                if (CollectionUtils.isNotEmpty(connectionIds)) {
//                    connections = connectionIds;
//                    specialTask = true;
//                }
//            }
//        }
//
//        if (!specialTask) {
//            List<Node> nodes = dag.getNodes();
//            if (CollectionUtils.isEmpty(nodes)) {
//                return false;
//            }
//
//            connections = nodes.stream().filter(n -> n instanceof DataParentNode).map(n -> ((DataParentNode<?>) n).getConnectionId())
//                    .collect(Collectors.toList());
//        }
//
//        List<DataSourceConnectionDto> connectionDtos = dataSourceService.findInfoByConnectionIdList(connections, user, "pdkType");
//
//        for (DataSourceConnectionDto connectionDto : connectionDtos) {
//            if (DataSourceDefinitionDto.PDK_TYPE.equals(connectionDto.getPdkType())) {
//                return true;
//            }
//        }
//
//        return false;
//    }
//
//    public boolean checkDeleteFlag(ObjectId id, UserDetail user) {
//        SubTaskDto subTaskDto = checkExistById(id, "deleteFlag");
//        if (subTaskDto.getDeleteFlag() != null) {
//            return subTaskDto.getDeleteFlag();
//        }
//        return false;
//    }
//
//    public boolean checkResetFlag(ObjectId id, UserDetail user) {
//        SubTaskDto subTaskDto = checkExistById(id, "resetFlag");
//        if (subTaskDto.getResetFlag() != null) {
//            return subTaskDto.getResetFlag();
//        }
//        return false;
//    }
//
//    public void resetFlag(ObjectId id, UserDetail user, String flag) {
//        updateById(id, new Update().unset(flag), user);
//    }
//
//    public void startPlanMigrateDagTask() {
//        Criteria migrateCriteria = Criteria.where("syncType").is("migrate")
//                .and("status").is(SubTaskDto.STATUS_EDIT)
//                .and("planStartDateFlag").is(true)
//                .and("planStartDate").lte(System.currentTimeMillis());
//        Query taskQuery = new Query(migrateCriteria);
//        List<TaskDto> taskList = taskService.findAll(taskQuery);
//        if (CollectionUtils.isNotEmpty(taskList)) {
//            List<String> userIdList = taskList.stream().map(TaskDto::getUserId).distinct().collect(Collectors.toList());
//            List<UserDetail> userList = userService.getUserByIdList(userIdList);
//
//            Map<String, UserDetail> userMap = new HashMap<>();
//            if (CollectionUtils.isNotEmpty(userList)) {
//                userMap = userList.stream().collect(Collectors.toMap(UserDetail::getUserId, Function.identity()));
//            }
//
//            List<ObjectId> taskIdList = taskList.stream().map(TaskDto::getId).collect(Collectors.toList());
//
//            Criteria supTaskCriteria = Criteria.where("parentId").in(taskIdList);
//            Query supTaskQuery = new Query(supTaskCriteria);
//            List<SubTaskDto> subTaskList = findAll(supTaskQuery);
//            if (CollectionUtils.isNotEmpty(subTaskList)) {
//
//                Map<String, UserDetail> finalUserMap = userMap;
//                subTaskList.forEach(subTaskDto -> start(subTaskDto.getId(), finalUserMap.get(subTaskDto.getUserId())));
//            }
//        }
//    }
//
//    public SubTaskDto findByCacheName(String cacheName, UserDetail user) {
//        Criteria taskCriteria = Criteria.where("dag.nodes").elemMatch(Criteria.where("catalog").is("memCache").and("cacheName").is(cacheName));
//        Query query = new Query(taskCriteria);
//        SubTaskDto subTaskDto = findOne(query, user);
//        if (subTaskDto != null) {
//            TaskDto taskDto = taskService.findById(subTaskDto.getParentId(), user);
//            if (taskDto != null) {
//                subTaskDto.setParentTask(taskDto);
//            }
//        }
//
//        return subTaskDto;
//    }
//
//    public void updateDag(SubTaskDto subTaskDto, UserDetail user) {
//        SubTaskDto subTaskDto1 = checkExistById(subTaskDto.getId());
//
//        Criteria criteria = Criteria.where("_id").is(subTaskDto.getId());
//        Update update = Update.update("dag", subTaskDto.getDag());
//        long tmCurrentTime = System.currentTimeMillis();
//        update.set("tmCurrentTime", tmCurrentTime);
//        repository.update(new Query(criteria), update, user);
//
//        TaskHistory taskHistory = new TaskHistory();
//        BeanUtils.copyProperties(subTaskDto1, taskHistory);
//        taskHistory.setTaskId(subTaskDto1.getId().toHexString());
//        taskHistory.setId(ObjectId.get());
//
//        //保存子任务历史
//        repository.getMongoOperations().insert(taskHistory, "DDlTaskHistories");
//
//        //同步保存到任务。
//        Field field = new Field();
//        field.put("dag", true);
//        TaskDto taskDto = taskService.findById(subTaskDto1.getParentId(), field, user);
//
//        DAG dag = taskDto.getDag();
//        List<Node> nodes = dag.getNodes();
//        DAG subDag = subTaskDto.getDag();
//        List<Node> subNodes = subDag.getNodes();
//        Map<String, Node> subNodeMap = subNodes.stream().collect(Collectors.toMap(Element::getId, n -> n));
//        for (Node node : nodes) {
//            Node subNode = subNodeMap.get(node.getId());
//            if (subNode != null) {
//                BeanUtils.copyProperties(subNode, node);
//            }
//        }
//
//        taskService.updateDag(taskDto, user);
//    }
//
//    public SubTaskDto findByVersionTime(String id, Long time) {
//        Criteria criteria = Criteria.where("taskId").is(id);
//        criteria.and("tmCurrentTime").is(time);
//
//        Query query = new Query(criteria);
//
//        SubTaskDto dDlTaskHistories = repository.getMongoOperations().findOne(query, TaskHistory.class, "DDlTaskHistories");
//
//        if (dDlTaskHistories == null) {
//            dDlTaskHistories = findById(MongoUtils.toObjectId(id));
//        } else {
//            dDlTaskHistories.setId(MongoUtils.toObjectId(id));
//        }
//
//        TaskDto taskDto = taskService.findById(dDlTaskHistories.getParentId());
//        dDlTaskHistories.setParentTask(taskDto);
//        return dDlTaskHistories;
//    }
//
//    /**
//     * @param time 最近时间戳
//     * @return
//     */
//    public void clean(String taskId, Long time) {
//        Criteria criteria = Criteria.where("taskId").is(taskId);
//        criteria.and("tmCurrentTime").gt(time);
//
//        Query query = new Query(criteria);
//        repository.getMongoOperations().remove(query, "DDlTaskHistories");
//
//        //清理模型
//        //MetaDataHistoryService historyService = SpringContextHelper.getBean(MetaDataHistoryService.class);
//        historyService.clean(taskId, time);
//    }
//
//    public void updateStatus(ObjectId taskId, String status) {
//        Query query = Query.query(Criteria.where("_id").is(taskId));
//        Update update = Update.update("status", status);
//        update(query, update);
//    }
//}
