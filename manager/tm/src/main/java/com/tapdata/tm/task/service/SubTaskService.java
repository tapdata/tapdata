package com.tapdata.tm.task.service;

import cn.hutool.core.bean.BeanUtil;
import com.mongodb.client.result.UpdateResult;
import com.tapdata.manager.common.utils.JsonUtil;
import com.tapdata.tm.CustomerJobLogs.CustomerJobLog;
import com.tapdata.tm.CustomerJobLogs.service.CustomerJobLogsService;
import com.tapdata.tm.base.dto.Field;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.base.service.BaseService;
import com.tapdata.tm.commons.dag.AccessNodeTypeEnum;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Edge;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.logCollector.HazelCastImdgNode;
import com.tapdata.tm.commons.dag.logCollector.LogCollectorNode;
import com.tapdata.tm.commons.dag.nodes.DataParentNode;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.nodes.TableNode;
import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
import com.tapdata.tm.commons.schema.MetadataInstancesDto;
import com.tapdata.tm.commons.task.dto.*;
import com.tapdata.tm.commons.task.dto.progress.SubTaskSnapshotProgress;
import com.tapdata.tm.commons.util.MetaDataBuilderUtils;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.ds.service.impl.DataSourceService;
import com.tapdata.tm.messagequeue.dto.MessageQueueDto;
import com.tapdata.tm.messagequeue.service.MessageQueueService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.monitor.entity.AgentStatDto;
import com.tapdata.tm.monitor.service.MeasurementService;
import com.tapdata.tm.task.bean.FullSyncVO;
import com.tapdata.tm.task.bean.IncreaseSyncVO;
import com.tapdata.tm.task.bean.RunTimeInfo;
import com.tapdata.tm.task.constant.SubTaskOpStatusEnum;
import com.tapdata.tm.task.entity.SubTaskEntity;
import com.tapdata.tm.task.repository.SubTaskRepository;
import com.tapdata.tm.task.vo.SubTaskDetailVo;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.utils.UUIDUtil;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.entity.Worker;
import com.tapdata.tm.worker.service.WorkerService;
import com.tapdata.tm.ws.enums.MessageType;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.checkerframework.checker.units.qual.C;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @Author:
 * @Date: 2021/11/03
 * @Description:
 */
@Service
@Slf4j
@Setter(onMethod_ = {@Autowired})
public class SubTaskService extends BaseService<SubTaskDto, SubTaskEntity, ObjectId, SubTaskRepository> {

    private TaskService taskService;
    private MeasurementService measurementService;
    private SnapshotEdgeProgressService snapshotEdgeProgressService;
    private TaskNodeRuntimeInfoService taskNodeRuntimeInfoService;
    private TaskDatabaseRuntimeInfoService taskDatabaseRuntimeInfoService;
    private TaskSnapshotService taskSnapshotService;
    private TaskRunHistoryService taskRunHistoryService;
    private WorkerService workerService;
    private MessageQueueService messageQueueService;
    private CustomerJobLogsService customerJobLogsService;
    private UserService userService;

    private MetadataInstancesService metadataInstancesService;
    private DataSourceService dataSourceService;

    private DataSourceDefinitionService dataSourceDefinitionService;

    /**
     * ???????????????
     */
    public static Set<String> stopStatus = new HashSet<>();
    /**
     * ????????????
     */
    public static Set<String> runningStatus = new HashSet<>();

//    @Autowired
//    private StateMachineService stateMachineService;

    static {

        runningStatus.add(SubTaskDto.STATUS_SCHEDULING);
        runningStatus.add(SubTaskDto.STATUS_WAIT_RUN);
        runningStatus.add(SubTaskDto.STATUS_RUNNING);
        runningStatus.add(SubTaskDto.STATUS_STOPPING);

        stopStatus.add(SubTaskDto.STATUS_SCHEDULE_FAILED);
        stopStatus.add(SubTaskDto.STATUS_COMPLETE);
        stopStatus.add(SubTaskDto.STATUS_STOP);
    }

    public SubTaskService(@NonNull SubTaskRepository repository, TaskNodeRuntimeInfoService taskNodeRuntimeInfoService,
                          TaskSnapshotService taskSnapshotService, TaskRunHistoryService taskRunHistoryService,
                          TaskDatabaseRuntimeInfoService taskDatabaseRuntimeInfoService,
                          CustomerJobLogsService customerJobLogsService,
                          WorkerService workerService, MessageQueueService messageQueueService) {
        super(repository, SubTaskDto.class, SubTaskEntity.class);
        this.taskNodeRuntimeInfoService = taskNodeRuntimeInfoService;
        this.taskSnapshotService = taskSnapshotService;
        this.taskRunHistoryService = taskRunHistoryService;
        this.workerService = workerService;
        this.messageQueueService = messageQueueService;
        this.taskDatabaseRuntimeInfoService = taskDatabaseRuntimeInfoService;
        this.customerJobLogsService = customerJobLogsService;
    }

    protected void beforeSave(SubTaskDto subTask, UserDetail user) {
        DAG dag = subTask.getDag();
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
        }

    }

    /**
     * ????????????id?????????????????????
     *
     * @param taskId
     * @return
     */
    public List<SubTaskDto> findByTaskId(ObjectId taskId, UserDetail user, String... fields) {
        Criteria criteria = Criteria.where("parentId").is(taskId);
        Query query = new Query(criteria);
        if (fields != null && fields.length > 0) {
            query.fields().include(fields);
        }

        if (user != null) {
            return findAllDto(query, user);
        }

        return findAll(query);
    }

    public List<SubTaskDto> findByTaskId(ObjectId taskId, String... fields) {
        return findByTaskId(taskId, null, fields);
    }

    /**
     * ????????????id?????????????????????
     *
     * @param taskId
     * @return
     */
    public List<SubTaskDto> findByTaskId(String taskId) {
        Criteria criteria = Criteria.where("parentId").is(taskId);
        Query query = new Query(criteria);
        return findAll(query);
    }


    /**
     * ????????????????????????????????????????????????
     * @param subTaskId
     */
    public void renewAgentMeasurement(String subTaskId) {
        //?????????????????????????????????????????????
        //??????subTaskId ????????????????????????
        measurementService.deleteSubTaskMeasurement(subTaskId);
    }


    /**
     * ???????????????
     * ??????????????????????????????????????????
     *
     * @param
     */
    public void renew(ObjectId id, UserDetail user) {
        SubTaskDto subTaskDto = checkExistById(id);
        renew(subTaskDto, user);
        renewAgentMeasurement(id.toString());
    }

    public void renew(SubTaskDto subTaskDto, UserDetail user) {
        sendRenewMq(subTaskDto, user, DataSyncMq.OP_TYPE_RESET);
        renewNotSendMq(subTaskDto, user);
    }
    public void renewNotSendMq(SubTaskDto subTaskDto, UserDetail user) {
        log.info("renew subtask, subtask name = {}, username = {}", subTaskDto.getName(), user.getUsername());



        Update set = Update.update("agentId", null).set("agentTags", null).set("scheduleTimes", null)
                .set("scheduleTime", null)
                .unset("milestones").set("messages", null).set("status", SubTaskDto.STATUS_EDIT);


        if (subTaskDto.getAttrs() != null) {
            subTaskDto.getAttrs().remove("syncProgress");
            subTaskDto.getAttrs().remove("edgeMilestones");

            set.set("attrs", subTaskDto.getAttrs());
        }

        //updateById(subTaskDto.getId(), set, user);

        //?????????????????????????????????node????????????TaskRuntimeInfo
        List<Node> nodes = subTaskDto.getDag().getNodes();
        if (nodes != null) {

            List<String> nodeIds = nodes.stream().map(Node::getId).collect(Collectors.toList());
            Criteria criteria = Criteria.where("subTaskId").is(subTaskDto.getId().toHexString())
                    .and("type").is(SubTaskSnapshotProgress.ProgressType.EDGE_PROGRESS.name())
                    .orOperator(Criteria.where("srcNodeId").in(nodeIds),
                            Criteria.where("tgtNodeId").in(nodeIds));
            Query query = new Query(criteria);

            snapshotEdgeProgressService.deleteAll(query);

            Criteria criteria1 = Criteria.where("subTaskId").is(subTaskDto.getId().toHexString())
                    .and("type").is(SubTaskSnapshotProgress.ProgressType.SUB_TASK_PROGRESS.name());
            Query query1 = new Query(criteria1);

            snapshotEdgeProgressService.deleteAll(query1);
//            taskNodeRuntimeInfoService.deleteAll(query);
//            taskDatabaseRuntimeInfoService.deleteAll(query);
        }

        //????????????????????????????????????temp???????????????????????????
        subTaskDto.setDag(subTaskDto.getTempDag());
        beforeSave(subTaskDto, user);
        set.unset("tempDag").set("isEdit", true).set("status", SubTaskDto.STATUS_EDIT);
        Update update = new Update();
        taskService.update(new Query(Criteria.where("_id").is(subTaskDto.getParentId())), update.unset("temp"));
        updateById(subTaskDto.getId(), set, user);

        CustomerJobLog customerJobLog = new CustomerJobLog(subTaskDto.getId().toString(), subTaskDto.getName());
        customerJobLog.setDataFlowType(CustomerJobLogsService.DataFlowType.sync.getV());
        customerJobLogsService.resetJob(customerJobLog, user);
        resetFlag(subTaskDto.getId(), user, "resetFlag");
    }

    private void sendRenewMq(SubTaskDto subTaskDto, UserDetail user, String opType) {
        if (checkPdkSubTask(subTaskDto, user)) {

            DataSyncMq mq = new DataSyncMq();
            mq.setTaskId(subTaskDto.getId().toHexString());
            mq.setOpType(opType);
            mq.setType(MessageType.DATA_SYNC.getType());


            Map<String, Object> data;
            String json = JsonUtil.toJsonUseJackson(mq);
            data = JsonUtil.parseJsonUseJackson(json, Map.class);

            if (subTaskDto.getParentTask() == null) {
                TaskDto parentTask = taskService.findById(subTaskDto.getParentId(), user);
                subTaskDto.setParentTask(parentTask);
            }
            TaskDto taskDto = subTaskDto.getParentTask();
            if (StringUtils.equals(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), taskDto.getAccessNodeType())
                    && CollectionUtils.isNotEmpty(taskDto.getAccessNodeProcessIdList())) {
                subTaskDto.setAgentId(taskDto.getAccessNodeProcessIdList().get(0));
            } else {
                List<Worker> availableAgent = workerService.findAvailableAgent(user);
                if (CollectionUtils.isNotEmpty(availableAgent)) {
                    Worker worker = availableAgent.get(0);
                    subTaskDto.setAgentId(worker.getProcessId());
                } else {
                    subTaskDto.setAgentId(null);
                }
            }

            MessageQueueDto queueDto = new MessageQueueDto();
            queueDto.setReceiver(subTaskDto.getAgentId());
            queueDto.setData(data);
            queueDto.setType("pipe");

            log.debug("build stop subtask websocket context, processId = {}, userId = {}, queueDto = {}", subTaskDto.getAgentId(), user.getUserId(), queueDto);
            messageQueueService.sendMessage(queueDto);

            //?????????????????????????????????8??????????????????
            boolean checkFlag = false;
            for (int i = 0; i < 16; i++) {
                checkFlag = DataSyncMq.OP_TYPE_RESET.equals(opType) ? checkResetFlag(subTaskDto.getId(), user) : checkDeleteFlag(subTaskDto.getId(), user);
                if (checkFlag) {
                    break;
                }
                try {
                    Thread.sleep(500L);
                } catch (InterruptedException e) {
                    throw new BizException("SystemError");
                }
            }

            if (!checkFlag) {
                log.info((DataSyncMq.OP_TYPE_RESET.equals(opType) ? "reset" : "delete") + "Task reset timeout.");
                throw new BizException(DataSyncMq.OP_TYPE_RESET.equals(opType) ? "Task.ResetTimeout" : "Task.DeleteTimeout");
            }
        }
    }

    public boolean deleteById(SubTaskDto subTaskDto, UserDetail user) {
        //????????????????????????????????????????????????????????????????????????????????????????????????????????????
        if (subTaskDto == null) {
            return true;
        }

        sendRenewMq(subTaskDto, user, DataSyncMq.OP_TYPE_DELETE);

        renewNotSendMq(subTaskDto, user);

        if (runningStatus.contains(subTaskDto.getStatus())) {
            log.warn("SubTask is run, can not delete it");
            throw new BizException("Task.DeleteSubTaskIsRun");
        }

        //TODO ?????????????????????????????????
        resetFlag(subTaskDto.getId(), user, "deleteFlag");
        return super.deleteById(subTaskDto.getId(), user);
    }

    public void update(List<SubTaskDto> newSubTasks, UserDetail user, boolean restart) {
        List<ObjectId> ids = newSubTasks.stream().map(SubTaskDto::getId).collect(Collectors.toList());
        Criteria criteria = Criteria.where("_id").in(ids);
        Query query = new Query(criteria);
        query.fields().include("_id", "dag");
        List<SubTaskDto> subTaskDtos = findAllDto(query, user);
        Map<ObjectId, SubTaskDto> subTaskDtoMap = subTaskDtos.stream().collect(Collectors.toMap(SubTaskDto::getId, s -> s));

        for (SubTaskDto newSubTask : newSubTasks) {
            SubTaskDto oldSubTask = subTaskDtoMap.get(newSubTask.getId());
            if (oldSubTask == null) {
                throw new BizException("SystemError");
            }

            //?????????????????????????????????
            boolean canHotUpdate = canHotUpdate(oldSubTask.getDag(), newSubTask.getDag());
            if (oldSubTask.getIsEdit() != null && !oldSubTask.getIsEdit() && !canHotUpdate) {
                //throw new BizException("Task.HotUpdateFailed", "This modification is not allowed while the task is running");
                //???????????????????????????????????????????????????????????????????????????
                newSubTask.setTempDag(newSubTask.getDag());
                newSubTask.setDag(oldSubTask.getDag());
                //TODO ?????????????????????????????????????????????????????????????????????, ?????????????????????????????????
                super.save(newSubTask, user);
                continue;
            }

            if (stopStatus.contains(oldSubTask.getStatus())) {
                newSubTask.setStatus(SubTaskDto.STATUS_EDIT);

            }

            beforeSave(newSubTask, user);
            Update update = repository.buildUpdateSet(convertToEntity(SubTaskEntity.class, newSubTask));
            update.set("tempDag", null);
            updateById(newSubTask.getId(), update, user);

            if (runningStatus.contains(oldSubTask.getStatus())) {
                restart = restart ? restart : !compareDag(newSubTask.getDag(), oldSubTask.getDag());
                if (restart) {
                    restart(newSubTask.getId(), user);
                }
            }
        }


        //TODO ??????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
    }

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


    @Override
    public SubTaskDetailVo findById(ObjectId id, Field field, UserDetail userDetail) {
        SubTaskDto subTaskDto = super.findById(id, field, userDetail);
        subTaskDto=getSubTaskDto(userDetail,subTaskDto);

        SubTaskDetailVo subTaskDetailVo= BeanUtil.copyProperties(subTaskDto,SubTaskDetailVo.class   );
        FullSyncVO fullSyncVO= snapshotEdgeProgressService.syncOverview(id.toString());
        if (null!=fullSyncVO){
            subTaskDetailVo.setStartTime(fullSyncVO.getStartTs());
        }
        subTaskDetailVo.setCreator(StringUtils.isNotBlank(userDetail.getUsername()) ? userDetail.getUsername() : userDetail.getEmail());
        return subTaskDetailVo;
    }

    private SubTaskDto getSubTaskDto(UserDetail userDetail, SubTaskDto subTaskDto) {
        if (subTaskDto == null) {
            return null;
        }
        if (subTaskDto.getParentId() != null) {
            Query query = new Query(Criteria.where("_id").is(subTaskDto.getParentId()));
            query.fields().exclude("dag");
            TaskDto taskDto = taskService.findOne(query, userDetail);
            subTaskDto.setParentTask(taskDto);

            //TODO ??????????????????????????????????????????????????????flowengin???????????????
            //TODO ????????????????????????????????????????????????????????????
//            List<Node> nodes = subTaskDto.getDag().getNodes();
//            if (CollectionUtils.isNotEmpty(nodes)) {
//                Set<Node> nodeSet = nodes.stream().filter(Node::isDataNode).collect(Collectors.toSet());
//                List<ObjectId> connectionIds = nodeSet.stream().map(n -> MongoUtils.toObjectId(((TableNode) n).getConnectionId())).collect(Collectors.toList());
//                Criteria criteria = Criteria.where("_id").in(connectionIds);
//                List<DataSourceConnectionDto> allDto = dataSourceService.findAllDto(new Query(criteria), userDetail);
//                Map<ObjectId, DataSourceConnectionDto> connectionDtoMap = allDto.stream().collect(Collectors.toMap(DataSourceConnectionDto::getId, d -> d));
//                for (Node node : nodeSet) {
//                    TableNode tableNode = (TableNode) node;
//
//                    if (tableNode.getConnectionId() != null) {
//                        DataSourceConnectionDto dataSourceConnectionDto = connectionDtoMap.get(MongoUtils.toObjectId(tableNode.getConnectionId()));
//                        if (dataSourceConnectionDto != null) {
//                            com.tapdata.tm.commons.schema.DataSourceConnectionDto target = new com.tapdata.tm.commons.schema.DataSourceConnectionDto();
//                            BeanUtils.copyProperties(dataSourceConnectionDto, target);
//                            tableNode.setConnectionDto(target);
//                        }
//                    }
//                }
//            }
        }

        return subTaskDto;
    }


    public SubTaskDto findOne(Filter filter, UserDetail userDetail) {
        Query query = repository.filterToQuery(filter);
        SubTaskDto subTaskDto = findOne(query, userDetail);
        return getSubTaskDto(userDetail, subTaskDto);
    }

    /**
     * Paging query
     *
     * @param filter optional, page query parameters
     * @return the Page of current page, include page data and total size.
     */
    public Page<SubTaskDto> find(Filter filter, UserDetail userDetail) {
        Page<SubTaskDto> page = super.find(filter, userDetail);
        List<SubTaskDto> items = page.getItems();
        if (CollectionUtils.isNotEmpty(items)) {
            for (SubTaskDto item : items) {
                getSubTaskDto(userDetail, item);
            }
        }

        return page;
    }

    /**
     * ???????????????
     *
     * @param id
     */
    public void start(ObjectId id, UserDetail user) {
        start(id, user, "11");
    }


    /**
     * ???????????????
     *
     * @param id
     * @param id
     */
    @Async
    public void start(ObjectId id, UserDetail user, String startFlag) {
        SubTaskDto subTaskDto = checkExistById(id);
        TaskDto taskDto = taskService.findById(subTaskDto.getParentId(), user);
        subTaskDto.setParentTask(taskDto);
        start(subTaskDto, user, startFlag);
    }

    /**
     * ????????????????????????????????????
     *
     * @param subTaskDto
     * @param startFlag ??????????????????
     *                  ????????? ????????????????????????????????? 1 ???   0 ???
     *                  ????????? ????????????????????????      1 ???   0 ???
     */
    private void start(SubTaskDto subTaskDto, UserDetail user, String startFlag) {

        TaskDto parentTask = subTaskDto.getParentTask();

        //????????????
        if (startFlag.charAt(0) == '1') {
            logCollector(user, parentTask);
        }

        //???????????????????????????????????????????????????????????????????????????
        if (startFlag.charAt(1) == '1') {
            //startConnHeartbeat(user, parentTask);
        }

        //????????????,??????????????????????????????????????????
        DAG dag = subTaskDto.getDag();
//        Map<String, List<Message>> schemaErrorMap = transformSchemaService.transformSchemaSync(dag, user, subTaskDto.getParentId());
//        if (!schemaErrorMap.isEmpty()) {
//            throw new BizException("Task.ListWarnMessage", schemaErrorMap);
//        }

        //???????????????????????????????????????
        if (!SubTaskOpStatusEnum.to_start_status.v().contains(subTaskDto.getStatus())) {
            log.warn("subTask current status not allow to start, subTask = {}, status = {}", subTaskDto.getName(), subTaskDto.getStatus());
            throw new BizException("Task.StartStatusInvalid");
        }


        run(subTaskDto, user);

    }

    public void run(SubTaskDto subTaskDto, UserDetail user) {
        //?????????????????????????????????
        DAG dag = subTaskDto.getDag();
        Query query = new Query(Criteria.where("id").is(subTaskDto.getId()).and("status").is(subTaskDto.getStatus()));
        //???????????????????????????
        UpdateResult update = update(query, Update.update("status", SubTaskDto.STATUS_SCHEDULING).set("isEdit", false).set("restartFlag", false), user);
        if (update.getModifiedCount() == 0) {
            //??????????????????????????????????????????????????????????????????????????????
            log.info("concurrent start operations, this operation don???t effective, subtask name = {}", subTaskDto.getName());
            return;
        }

        TaskDto taskDto = subTaskDto.getParentTask();
        if (StringUtils.equals(AccessNodeTypeEnum.MANUALLY_SPECIFIED_BY_THE_USER.name(), taskDto.getAccessNodeType())
                && CollectionUtils.isNotEmpty(taskDto.getAccessNodeProcessIdList())) {
            subTaskDto.setAgentId(taskDto.getAccessNodeProcessIdList().get(0));
        } else {
            subTaskDto.setAgentId(null);
        }

        workerService.scheduleTaskToEngine(subTaskDto, user, "SubTask", subTaskDto.getName());
        CustomerJobLog customerJobLog = new CustomerJobLog(subTaskDto.getId().toString(), subTaskDto.getName());
        customerJobLog.setDataFlowType(CustomerJobLogsService.DataFlowType.sync.getV());
        if (StringUtils.isBlank(subTaskDto.getAgentId())) {
            log.warn("No available agent found, task name = {}", subTaskDto.getName());
            Query query1 = new Query(Criteria.where("_id").is(subTaskDto.getId()).and("status").is(SubTaskDto.STATUS_SCHEDULING));
            update(query1, Update.update("status", SubTaskDto.STATUS_SCHEDULE_FAILED), user);
            customerJobLogsService.noAvailableAgents(customerJobLog, user);
            throw new BizException("Task.AgentNotFound");
        }

        WorkerDto workerDto = workerService.findOne(new Query(Criteria.where("processId").is(subTaskDto.getAgentId())));
        customerJobLog.setAgentHost(workerDto.getHostname());
        customerJobLogsService.assignAgent(customerJobLog, user);

        //??????????????????????????????????????????
        Query query1 = new Query(Criteria.where("_id").is(subTaskDto.getId()).and("status").is(SubTaskDto.STATUS_SCHEDULING));
        UpdateResult update1 = update(query1, Update.update("status", SubTaskDto.STATUS_WAIT_RUN).set("agentId", subTaskDto.getAgentId()), user);
        if (update1.getModifiedCount() == 0) {
            log.info("concurrent start operations, this operation don???t effective, subtask name = {}", subTaskDto.getName());
            return;
        }
        customerJobLog.setJobName(subTaskDto.getName());
        customerJobLog.setJobInfos(TaskService.printInfos(dag));
        customerJobLogsService.startJob(customerJobLog, user);
        //??????websocket???????????????flowengin??????
        DataSyncMq dataSyncMq = new DataSyncMq();
        dataSyncMq.setTaskId(subTaskDto.getId().toHexString());
        dataSyncMq.setOpType(DataSyncMq.OP_TYPE_START);
        dataSyncMq.setType(MessageType.DATA_SYNC.getType());

        Map<String, Object> data;
        String json = JsonUtil.toJsonUseJackson(dataSyncMq);
        data = JsonUtil.parseJsonUseJackson(json, Map.class);
        MessageQueueDto queueDto = new MessageQueueDto();
        queueDto.setReceiver(subTaskDto.getAgentId());
        queueDto.setData(data);
        queueDto.setType("pipe");

        log.debug("build start subTask websocket context, processId = {}, userId = {}, queueDto = {}", subTaskDto.getAgentId(), user.getUserId(), queueDto);
        messageQueueService.sendMessage(queueDto);

        //????????????????????????????????????????????????)
        //?????????????????????????????????TaskRunHistory???
        createTaskSnapshot(subTaskDto, user, TaskRunHistoryDto.ACTION_RUN);
    }


    /**
     * ???????????????
     *
     * @param id
     */
    public void pause(ObjectId id, UserDetail user, boolean force) {
        SubTaskDto subTaskDto = checkExistById(id);
        pause(subTaskDto, user, force);
    }

    /**
     * ???????????????  ??????????????????????????????????????????
     *
     * @param subTaskDto ?????????
     * @param user       ??????
     * @param force      ??????????????????
     */
    @Transactional
    @Async
    public void pause(SubTaskDto subTaskDto, UserDetail user, boolean force) {
        pause(subTaskDto, user, force, false);
    }

    /**
     * ???????????????  ??????????????????????????????????????????
     *
     * @param subTaskDto ?????????
     * @param user       ??????
     * @param force      ??????????????????
     */
    //@Transactional
    @Async
    public void pause(SubTaskDto subTaskDto, UserDetail user, boolean force, boolean restart) {
        //????????????????????????????????????????????????
        if (!SubTaskOpStatusEnum.to_stop_status.v().contains(subTaskDto.getStatus()) && !restart) {
            log.warn("subTask current status not allow to pause, subTask = {}, status = {}", subTaskDto.getName(), subTaskDto.getStatus());
            throw new BizException("Task.PauseStatusInvalid");
        }

        //????????????????????????????????????????????????
        if (SubTaskDto.STATUS_STOP.equals(subTaskDto.getStatus()) && restart) {
            Update update = Update.update("restartFlag", true).set("restartUserId", user.getUserId());
            Query query = new Query(Criteria.where("_id").is(subTaskDto.getId()));
            update(query, update, user);
            return;
        }


        String pauseStatus = SubTaskDto.STATUS_STOPPING;
        if (force) {
            pauseStatus = SubTaskDto.STATUS_STOP;
        }

        //??????????????????????????????flowengin??????????????????????????????????????????????????????????????????
        Update update = Update.update("status", pauseStatus);
        if (restart) {
            update.set("restartFlag", true).set("restartUserId", user.getUserId());
        }

        Query query1 = new Query(Criteria.where("_id").is(subTaskDto.getId()).and("status").is(subTaskDto.getStatus()));
        UpdateResult update1 = update(query1, update, user);
        if (update1.getModifiedCount() == 0) {
            //??????????????????????????????????????????????????????
            log.info("concurrent pause operations, this operation don???t effective, subtask name = {}", subTaskDto.getName());
            return;
        }
        CustomerJobLog customerJobLog = new CustomerJobLog(subTaskDto.getId().toString(), subTaskDto.getName());
        customerJobLog.setDataFlowType(CustomerJobLogsService.DataFlowType.sync.getV());
        customerJobLog.setJobName(subTaskDto.getName());
        if (force) {
            customerJobLogsService.forceStopJob(customerJobLog, user);
        } else {
            customerJobLogsService.stopJob(customerJobLog, user);
        }

        DataSyncMq dataSyncMq = new DataSyncMq();
        dataSyncMq.setTaskId(subTaskDto.getId().toHexString());
        dataSyncMq.setForce(force);
        dataSyncMq.setOpType(DataSyncMq.OP_TYPE_STOP);
        dataSyncMq.setType(MessageType.DATA_SYNC.getType());

        Map<String, Object> data;
        String json = JsonUtil.toJsonUseJackson(dataSyncMq);
        data = JsonUtil.parseJsonUseJackson(json, Map.class);
        MessageQueueDto queueDto = new MessageQueueDto();
        queueDto.setReceiver(subTaskDto.getAgentId());
        queueDto.setData(data);
        queueDto.setType("pipe");

        log.debug("build stop subtask websocket context, processId = {}, userId = {}, queueDto = {}", subTaskDto.getAgentId(), user.getUserId(), queueDto);
        messageQueueService.sendMessage(queueDto);

        //????????????????????????????????????????????????)
        //?????????????????????????????????TaskRunHistory???
        createTaskSnapshot(subTaskDto, user, TaskRunHistoryDto.ACTION_STOP);
    }


    /**
     * ????????????????????????????????????
     *
     * @param id
     */
    public String running(ObjectId id, UserDetail user) {
        //???????????????????????????
        SubTaskDto subTaskDto = checkExistById(id, "_id", "status", "name");
        //?????????????????????????????????
        if (!SubTaskDto.STATUS_WAIT_RUN.equals(subTaskDto.getStatus())) {
            log.info("concurrent runError operations, this operation don???t effective, subtask name = {}", subTaskDto.getName());
            return null;
        }
        Query query1 = new Query(Criteria.where("_id").is(subTaskDto.getId()).and("status").is(SubTaskDto.STATUS_WAIT_RUN));
        UpdateResult update1 = update(query1, Update.update("status", SubTaskDto.STATUS_RUNNING).set("startTime", new Date()), user);
        if (update1.getModifiedCount() == 0) {
            log.info("concurrent running operations, this operation don???t effective, subtask name = {}", subTaskDto.getName());
            return null;
        } else {
            return id.toHexString();
        }
    }

    /**
     * ????????????????????????????????????
     *
     * @param id
     */
    public String runError(ObjectId id, UserDetail user, String errMsg, String errStack) {
        //??????????????????????????????
        SubTaskDto subTaskDto = checkExistById(id, "_id", "status", "name");

        if (!SubTaskOpStatusEnum.to_error_status.v().contains(subTaskDto.getStatus())) {
            log.info("concurrent runError operations, this operation don???t effective, subtask name = {}", subTaskDto.getName());
            return null;
        }
        //?????????????????????????????????.
        Query query1 = new Query(Criteria.where("_id").is(subTaskDto.getId()).and("status").in(SubTaskOpStatusEnum.to_error_status.v()));
        UpdateResult update1 = update(query1, Update.update("status", SubTaskDto.STATUS_ERROR), user);
        if (update1.getModifiedCount() == 0) {
            log.info("concurrent runError operations, this operation don???t effective, subtask name = {}", subTaskDto.getName());
            return null;
        } else {
            return id.toHexString();
        }

    }

    /**
     * ????????????????????????????????????
     *
     * @param id
     */
    public String complete(ObjectId id, UserDetail user) {
        //???????????????????????????
        SubTaskDto subTaskDto = checkExistById(id, "_id", "status", "name");
        if (!SubTaskOpStatusEnum.to_complete_status.v().contains(subTaskDto.getStatus())) {
            log.info("concurrent complete operations, this operation don???t effective, subtask name = {}", subTaskDto.getName());
            return null;
        }
        //???????????????????????????????????????
        Query query1 = new Query(Criteria.where("_id").is(subTaskDto.getId()).and("status").in(SubTaskOpStatusEnum.to_complete_status.v()));
        UpdateResult update1 = update(query1, Update.update("status", SubTaskDto.STATUS_COMPLETE), user);
        if (update1.getModifiedCount() == 0) {
            log.info("concurrent complete operations, this operation don???t effective, subtask name = {}", subTaskDto.getName());
            return null;
        } else {
            return id.toHexString();
        }
    }

    /**
     * ????????????????????????????????????
     *
     * @param id
     */
    public String stopped(ObjectId id, UserDetail user) {
        //??????????????????????????????
        SubTaskDto subTaskDto = checkExistById(id, "dag", "name", "status", "_id");


        //???????????????????????????????????????????????????????????????????????????????????????????????????
        if (!SubTaskDto.STATUS_STOPPING.equals(subTaskDto.getStatus())) {
            log.info("concurrent stopped operations, this operation don???t effective, subtask name = {}", subTaskDto.getName());
            return null;
        }

        Query query1 = new Query(Criteria.where("_id").is(subTaskDto.getId()).and("status").is(SubTaskDto.STATUS_STOPPING));

        //endConnHeartbeat(user, subTaskDto);

        UpdateResult update1 = update(query1, Update.update("status", SubTaskDto.STATUS_STOP), user);
        if (update1.getModifiedCount() == 0) {
            log.info("concurrent stopped operations, this operation don???t effective, subtask name = {}", subTaskDto.getName());
            return null;
        } else {
            return id.toHexString();
        }
    }


    public void restart(ObjectId id, UserDetail user) {
        SubTaskDto subTaskDto = checkExistById(id);


        //?????????????????????????????????
        updateById(subTaskDto.getId(), Update.update("status", SubTaskDto.STATUS_WAIT_RUN), user);

        pause(subTaskDto, user, false, true);

        //????????????????????????????????????????????????)
        //?????????????????????????????????TaskRunHistory???
        createTaskSnapshot(subTaskDto, user, TaskRunHistoryDto.ACTION_STOP);
    }

    public void restarted(ObjectId id, UserDetail user) {

    }


    /**
     * ?????????????????????????????????????????????????????????
     * ?????????????????????????????????????????????????????????????????????????????????
     *
     * @param old ????????????dag
     * @param cur ????????????dag
     * @return
     */
    public boolean canHotUpdate(DAG old, DAG cur) {
        //????????????????????????????????????????????????????????????????????????
        List<Node> curNodes = cur.getNodes();
        List<Node> oldNodes = old.getNodes();

        List<Edge> curEdges = cur.getEdges();
        List<Edge> oldEdges = old.getEdges();

        //???????????????????????????????????????????????????false
        if (CollectionUtils.isEmpty(curNodes) || CollectionUtils.isEmpty(oldNodes)
                || CollectionUtils.isEmpty(curEdges) || CollectionUtils.isEmpty(oldEdges)) {
            return false;
        }

        if (curNodes.size() < oldNodes.size() || curEdges.size() < oldEdges.size()) {
            return false;
        }

        //????????????????????????????????????????????????
        //?????????????????????????????????????????????
        //???????????????????????????????????????????????????????????????????????????
        Map<String, Node> curNodeMap = curNodes.stream().collect(Collectors.toMap(Node::getId, n -> n));
        for (Node oldNode : oldNodes) {
//            Node node = curNodeMap.get(oldNode.getId());
//            if (!oldNode.getId().equals(node.getId())) {
//                return false;
//            }
        }

        for (Edge oldEdge : oldEdges) {
            boolean eq = false;
            for (Edge curEdge : curEdges) {
                if (curEdge.equals(oldEdge)) {
                    eq = true;
                    break;
                }
            }
            if (!eq) {
                return false;
            }
        }


        //??????????????????????????????????????????
        Map<String, Node> oldNodeMap = oldNodes.stream().collect(Collectors.toMap(Node::getId, n -> n));
        List<Edge> edges = curEdges.stream().filter(e -> !oldEdges.contains(e)).collect(Collectors.toList());
        for (Edge curEdge : edges) {
            if (oldNodeMap.get(curEdge.getTarget()) != null) {
                return false;
            }
        }

        return true;
    }


    private void createTaskSnapshot(SubTaskDto subTaskDto, UserDetail user, String action) {

        long time = System.currentTimeMillis() - 300000L;
        Date date = new Date(time);
        Criteria criteriaHistory = Criteria.where("taskId").is(subTaskDto.getParentId()).and("createTime").gt(date);


        long count = taskSnapshotService.count(new Query(criteriaHistory), user);
        if (count == 0) {
            //??????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
            TaskDto taskDto = taskService.findById(subTaskDto.getParentId());
            List<SubTaskDto> subTaskDtos = findByTaskId(subTaskDto.getParentId());
            Criteria criteria = Criteria.where("taskId").is(subTaskDto.getParentId());
            Query query = new Query(criteria);
            TaskNodeRuntimeInfoDto taskNodeRuntimeInfoDto = taskNodeRuntimeInfoService.findOne(query);
            TaskDatabaseRuntimeInfoDto taskDatabaseRuntimeInfoDto = taskDatabaseRuntimeInfoService.findOne(query);

            TaskSnapshotsDto taskSnapshotDto = new TaskSnapshotsDto();
            taskSnapshotDto.setTaskId(subTaskDto.getParentId());
            taskSnapshotDto.setSnapshot(taskDto);
            taskSnapshotDto.setSubtasks(subTaskDtos);
            taskSnapshotDto.setNodeRunTimeInfo(taskNodeRuntimeInfoDto);
            taskSnapshotDto.setDatabaseRunNodeTimeInfo(taskDatabaseRuntimeInfoDto);

            taskSnapshotService.save(taskSnapshotDto, user);

        }
        TaskRunHistoryDto taskRunHistoryDto = new TaskRunHistoryDto();
        taskRunHistoryDto.setTaskId(subTaskDto.getParentId());
        taskRunHistoryDto.setSubTaskId(subTaskDto.getId());
        taskRunHistoryDto.setSubTaskName(subTaskDto.getName());
        taskRunHistoryDto.setAction(action);
        taskRunHistoryService.save(taskRunHistoryDto, user);
    }


    public SubTaskDto checkExistById(ObjectId id) {
        SubTaskDto subTaskDto = findById(id);
        if (subTaskDto == null) {
            throw new BizException("Task.subTaskNotFound");
        }
        return subTaskDto;
    }

    public SubTaskDto checkExistById(ObjectId id, String... fields) {
        Field field = null;
        if (fields != null && fields.length != 0) {
            field = new Field();
            for (String s : fields) {
                field.put(s, true);
            }
        }

        SubTaskDto subTaskDto = null;
        if (field == null) {
            subTaskDto = findById(id);
        } else {
            subTaskDto = findById(id, field);
        }
        if (subTaskDto == null) {
            throw new BizException("Task.subTaskNotFound");
        }
        return subTaskDto;
    }

    /**
     * ?????????????????? ????????????????????? ???????????????????????????????????????
     * ?????????????????????????????????????????????????????? ????????????????????????????????????????????????????????????????????? ???????????????????????????
     *
     * @param subId   ?????????id
     * @param endTime ????????????????????????????????????????????? ????????????????????????????????????????????????????????? ?????????????????????
     */
    public RunTimeInfo runtimeInfo(ObjectId subId, Long endTime, UserDetail user) {
        log.debug("query subtask runtime info, subtask id = {}, endTime = {}, user = {}", subId, endTime, user);

        //???????????????????????????
        SubTaskDto subTaskDto = findById(subId, user);
        if (subTaskDto == null) {
            return null;
        }
        //??????????????????????????????
        List<Milestone> milestones = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(subTaskDto.getMilestones())) {
            milestones.addAll(subTaskDto.getMilestones());
        }
        RunTimeInfo runTimeInfo = new RunTimeInfo();
        runTimeInfo.setMilestones(milestones);

        log.debug("runtime info ={}", runTimeInfo);
        return runTimeInfo;
    }

    public UpdateResult update(Query query, Update update, UserDetail userDetail) {
        UpdateResult update1 = repository.update(query, update, userDetail);

        try {
            Document queryObject = query.getQueryObject();
            Object id = queryObject.get("id");
            if (id == null) {
                id = queryObject.get("_id");
            }

            if (id != null) {

                flushStatus((ObjectId) id, userDetail);
            } else {
                query.fields().include("_id");
                SubTaskDto one = findOne(query, userDetail);
                flushStatus(one.getId(), userDetail);
            }
        } catch (Exception e) {
            log.info("flush task status list failed");
        }
        return update1;
    }

    public UpdateResult updateById(ObjectId id, Update update, UserDetail userDetail) {
        UpdateResult updateResult = super.updateById(id, update, userDetail);
        flushStatus(id, userDetail);
        return updateResult;
    }

    public void flushStatus(ObjectId id, UserDetail userDetail) {
        Criteria criteria = Criteria.where("_id").is(id);
        Query query = new Query(criteria);
        query.fields().include("parentId");
        SubTaskDto one = findOne(query);
        flushStatusParentId(one.getParentId(), userDetail);
    }

    private void flushStatusParentId(ObjectId id, UserDetail userDetail) {
        taskService.flushStatus(id, userDetail);
    }


    public long updateByWhere(Where where, Document doc, UserDetail user, String reqBody) {
        long count = super.updateByWhere(where, doc, user);

        if (count > 0 && reqBody.contains("\"status\"")) {
            Criteria criteria = Criteria.where("_id").is(where.get("_id"));
            Query query = new Query(criteria);
            query.fields().include("parentId");
            SubTaskDto subTaskDto = findOne(query, user);
            if (subTaskDto != null) {
                flushStatusParentId(subTaskDto.getParentId(), user);
            }
        }
        return count;
    }


    /**
     * ????????????
     *
     * @param user
     * @param oldTaskDto
     */
    public void logCollector(UserDetail user, TaskDto oldTaskDto) {

        if (!oldTaskDto.getShareCdcEnable()) {
            //??????????????????????????????
            return;
        }

        //??????DAG???????????????????????????
        DAG dag = oldTaskDto.getDag();

        if (!TaskDto.SYNC_TYPE_MIGRATE.equals(oldTaskDto.getSyncType()) && !TaskDto.SYNC_TYPE_SYNC.equals(oldTaskDto.getSyncType())) {
            //?????????????????????????????????????????????????????????
            return;
        }

        //????????????+???????????????????????????????????????????????????
        if (ParentTaskDto.TYPE_INITIAL_SYNC.equals(oldTaskDto.getType())) {
            //????????????????????????????????????????????????
            return;
        }

        List<Node> allNodes = dag.getNodes();
        List<Node> targets = dag.getTargets();
        Set<String> sinkIds = targets.stream().map(Node::getId).collect(Collectors.toSet());

        Map<ObjectId, List<Node>> group = allNodes.stream().
                filter(Node::isDataNode)
                .filter(n -> !sinkIds.contains(n.getId()))
                .collect(Collectors.groupingBy(s -> MongoUtils.toObjectId(((DataParentNode<?>) s).getConnectionId())));
        //???????????????????????????????????????
        Criteria criteria = Criteria.where("_id").in(group.keySet());
        Query query = new Query(criteria);
        query.fields().include("_id", "shareCdcEnable", "shareCdcTTL", "uniqueName", "database_type", "name");
        List<DataSourceConnectionDto> dataSourceDtos = dataSourceService.findAllDto(query, user);

        //?????????????????????
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


        //????????????????????????id??????
        Map<String, List<DataSourceConnectionDto>> dataSourceCacheByType = new HashMap<>();

        //?????????id???????????????????????????id
        Map<String, String> newLogCollectorMap = new HashMap<>();

        datasourceMap.forEach((k, v) -> {


            //?????????????????????????????????
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

            //??????????????????????????????????????????????????????????????????????????????????????????
            //??????unique name?????????????????????id??????
            DataSourceConnectionDto dataSource = v.get(0);
            List<String> ids = new ArrayList<>();

            //????????????uniqname,?????????????????????id???????????????????????????????????????
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
                List<SubTaskDto> id1s = findByTaskId(oldLogCollectorTask.getId(), user, "_id");
                SubTaskDto subTaskDto = id1s.get(0);
                for (String id : ids) {
                    newLogCollectorMap.put(id, subTaskDto.getId().toHexString());
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
                    //???????????????????????????????????????????????????????????????
                    List<SubStatus> statuses = oldLogCollectorTask.getStatuses();
                    if (updateConnectionId) {
                        taskService.confirmById(oldLogCollectorTask, user, true);
                    }
                    if (CollectionUtils.isNotEmpty(statuses)) {
                        SubStatus subStatus = statuses.get(0);
                        if (SubTaskDto.STATUS_RUNNING.equals(subStatus.getStatus())) {
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
                //??????stop??????????????? ?????????????????????????????????????????????????????????
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
            taskDto.setName("??????" + dataSource.getName() + "?????????????????????");
            taskDto.setDag(build);
            taskDto.setType("cdc");
            taskDto.setSyncType("logCollector");
            taskDto = taskService.create(taskDto, user);
            taskDto = taskService.confirmById(taskDto, user, true);

            //????????????????????????id???????????????
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

    public void startConnHeartbeat(UserDetail user, TaskDto oldTaskDto) {

        if (!TaskDto.SYNC_TYPE_MIGRATE.equals(oldTaskDto.getSyncType()) && !TaskDto.SYNC_TYPE_SYNC.equals(oldTaskDto.getSyncType())) {
            //?????????????????????????????????????????????????????????
            return;
        }

        //????????????+???????????????????????????????????????????????????
        if (ParentTaskDto.TYPE_INITIAL_SYNC.equals(oldTaskDto.getType())) {
            //????????????????????????????????????????????????
            return;
        }

        List<DataSourceConnectionDto> dataSourceDtos = getConnectionByDag(user, oldTaskDto.getDag());

        //????????????????????????id??????
        Map<String, List<DataSourceConnectionDto>> dataSourceCacheByType = new HashMap<>();


        DataSourceConnectionDto heartbeatConnection = null;
        String heartbeatTable = "tapdata_heartbeat_table_names";
        for (DataSourceConnectionDto dataSource : dataSourceDtos) {
            List<String> connectionIds = getConnectionIds(user, dataSourceCacheByType, dataSource);
            TaskDto oldConnHeartbeatTask = getHeartbeatTaskDto(connectionIds, user);
            if (oldConnHeartbeatTask != null) {
                HashSet<String> heartbeatTasks = oldConnHeartbeatTask.getHeartbeatTasks();
                List<SubTaskDto> subTaskDtos = findByTaskId(oldTaskDto.getId(), user, "_id");
                List<String> collect = subTaskDtos.stream().map(s -> s.getId().toHexString()).collect(Collectors.toList());
                heartbeatTasks.addAll(collect);
                taskService.update(new Query(Criteria.where("_id").is(oldConnHeartbeatTask.getId())), Update.update("heartbeatTasks", heartbeatTasks), user);
                if (SubTaskDto.STATUS_RUNNING.equals(oldConnHeartbeatTask.getStatus())) {
                    taskService.start(oldConnHeartbeatTask.getId(), user);
                }
                return;
            }


            //??????????????????????????????dummy?????????????????????
            if (heartbeatConnection == null) {

                Criteria criteria2 = Criteria.where("pdkId").is("dummy db");
                Query query3 = new Query(criteria2);
                query3.fields().include("pdkHash", "type");
                DataSourceDefinitionDto definitionDto = dataSourceDefinitionService.findOne(query3);
                //???????????????Dummy?????????
                Criteria criteriaCon = Criteria.where("database_type").is("dummy db").and("config.mode").is("Heartbeat");
                Query query2 = new Query(criteriaCon);
                heartbeatConnection = dataSourceService.findOne(query2, user);

                boolean addDummy = false;
                if (heartbeatConnection == null) {
                    heartbeatConnection = new DataSourceConnectionDto();
                    heartbeatConnection.setName("tapdata_heartbeat_dummy_connection");
                    LinkedHashMap<String, Object> config = new LinkedHashMap<>();
                    config.put("mode", "Heartbeat");
                    config.put("incremental_interval", "1000");

                    heartbeatConnection.setConfig(config);
                    heartbeatConnection.setConnection_type("source");
                    heartbeatConnection.setPdkType("pdk");
                    heartbeatConnection.setRetry(0);
                    heartbeatConnection.setStatus("testing");
                    heartbeatConnection.setShareCdcEnable(false);
                    heartbeatConnection.setDatabase_type(definitionDto.getType());
                    heartbeatConnection.setPdkHash(definitionDto.getPdkHash());
                    heartbeatConnection = dataSourceService.add(heartbeatConnection, user);
                    addDummy = true;

                }

                String qualifiedName = MetaDataBuilderUtils.generateQualifiedName("table", heartbeatConnection, "heartbeatTable");
                MetadataInstancesDto metadata = metadataInstancesService.findByQualifiedNameNotDelete(qualifiedName, user, "_id");
                if (metadata == null) {
                    if (!addDummy) {
                        //???????????????????????? ?????????????????????
                        dataSourceService.sendTestConnection(heartbeatConnection, true, true, user);
                    }

                    for (int i = 0; i < 8; i++) {
                        if (metadataInstancesService.findByQualifiedNameNotDelete(qualifiedName, user, "_id") == null) {
                            try {
                                Thread.sleep(500 * i);
                            } catch (InterruptedException e) {
                                throw new BizException("SystemError");
                            }
                        }

                    }
                }
            }


            TableNode sourceNode = new TableNode();
            sourceNode.setId(UUID.randomUUID().toString());
            sourceNode.setTableName(heartbeatTable);
            sourceNode.setConnectionId(heartbeatConnection.getId().toHexString());
            sourceNode.setDatabaseType(heartbeatConnection.getDatabase_type());
            sourceNode.setName(heartbeatTable);
            TableNode targetNode = new TableNode();
            targetNode.setId(UUID.randomUUID().toString());
            sourceNode.setTableName(heartbeatTable);
            sourceNode.setConnectionId(dataSource.getId().toHexString());
            sourceNode.setDatabaseType(dataSource.getDatabase_type());
            sourceNode.setName(heartbeatTable);

            List<Node> nodes = Lists.newArrayList(sourceNode, targetNode);

            Edge edge = new Edge(sourceNode.getId(), targetNode.getId());
            List<Edge> edges = Lists.newArrayList(edge);
            Dag dag1 = new Dag(edges, nodes);
            DAG build = DAG.build(dag1);
            TaskDto taskDto = new TaskDto();
            taskDto.setName("??????" + dataSource.getName() + "???????????????");
            taskDto.setDag(build);
            taskDto.setType("cdc");
            taskDto.setSyncType(TaskDto.SYNC_TYPE_CONN_HEARTBEAT);
            List<SubTaskDto> subTaskDtos = findByTaskId(oldTaskDto.getId(), user, "_id");
            HashSet<String> heartbeatTasks = subTaskDtos.stream().map(s -> s.getId().toHexString()).collect(Collectors.toCollection(HashSet::new));
            taskDto.setHeartbeatTasks(heartbeatTasks);
            taskDto = taskService.create(taskDto, user);
            taskDto = taskService.confirmById(taskDto, user, true);

            taskService.start(taskDto.getId(), user);
        }

    }


    private List<String> getConnectionIds(UserDetail user, Map<String, List<DataSourceConnectionDto>> dataSourceCacheByType, DataSourceConnectionDto dataSource) {
        List<String> ids = new ArrayList<>();

        //????????????uniqname,?????????????????????id???????????????????????????????????????
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


    public void endConnHeartbeat(UserDetail user, SubTaskDto subTaskDto) {
        TaskDto parentTask = subTaskDto.getParentTask();
        if (!TaskDto.SYNC_TYPE_MIGRATE.equals(parentTask.getSyncType()) && !TaskDto.SYNC_TYPE_SYNC.equals(parentTask.getSyncType())) {
            //????????????????????????????????????????????????????????????????????????
            return;
        }

        //????????????+????????????????????????????????????????????????????????????
        if (ParentTaskDto.TYPE_INITIAL_SYNC.equals(parentTask.getType())) {
            //??????????????????????????????
            return;
        }

        List<DataSourceConnectionDto> dataSourceDtos = getConnectionByDag(user, subTaskDto.getDag());


        //????????????????????????id??????
        Map<String, List<DataSourceConnectionDto>> dataSourceCacheByType = new HashMap<>();

        for (DataSourceConnectionDto dataSource : dataSourceDtos) {
            List<String> connectionIds = getConnectionIds(user, dataSourceCacheByType, dataSource);
            //????????????????????????connectionIds?????????????????????

            TaskDto oldConnHeartbeatTask = getHeartbeatTaskDto(connectionIds, user);
            HashSet<String> heartbeatTasks = oldConnHeartbeatTask.getHeartbeatTasks();
            heartbeatTasks.remove(subTaskDto.getId().toHexString());
            taskService.update(new Query(Criteria.where("_id").is(oldConnHeartbeatTask.getId())), Update.update("heartbeatTasks", heartbeatTasks), user);
            if (heartbeatTasks.size() == 0) {
                taskService.stop(oldConnHeartbeatTask.getId(), user, false);
            }
        }
    }

    private List<DataSourceConnectionDto> getConnectionByDag(UserDetail user, DAG dag) {
        List<Node> sources = dag.getSources();

        Set<String> connectionIds = sources.stream().map(n -> ((DataParentNode) n).getConnectionId()).collect(Collectors.toSet());
        //???????????????????????????????????????
        Criteria criteria = Criteria.where("_id").in(connectionIds);
        Query query = new Query(criteria);
        query.fields().include("_id", "uniqueName", "database_type", "name");
        List<DataSourceConnectionDto> dataSourceDtos = dataSourceService.findAllDto(query, user);
        return dataSourceDtos;
    }

    //??????????????????????????????????????????????????????????????????????????????????????????????????????sleep??????
    private void delayCheckSubTaskStatus(ObjectId taskId, String subStatus, UserDetail user) {
        Criteria criteria = Criteria.where("parentId").is(taskId);
        Query query = new Query(criteria);
        query.fields().include("status");
        level1:
        //??????16???????????????????????????????????????????????????????????????
        for (int i = 0; i < 100; i++) {
            int ms = (1 << i) * 1000;
            try {
                Thread.sleep(ms);
            } catch (InterruptedException e) {
                log.warn("thread sleep interrupt exception, e = {}", e.getMessage());
                return;
            }

            List<SubTaskDto> allDto = findAllDto(query, user);
            for (SubTaskDto subTaskDto : allDto) {
                if (!subStatus.equals(subTaskDto.getStatus())) {
                    break;
                }
                break level1;
            }
        }
    }


    //????????????????????????id?????????????????????
    private void updateLogCollectorMap(ObjectId taskId, Map<String, String> newLogCollectorMap, UserDetail user) {
        List<SubTaskDto> subTaskDtos = findByTaskId(taskId, "dag", "_id");
        if (CollectionUtils.isEmpty(subTaskDtos)) {
            return;
        }

        if (newLogCollectorMap == null || newLogCollectorMap.isEmpty()) {

            return;
        }

        for (SubTaskDto subTaskDto : subTaskDtos) {
            DAG dag = subTaskDto.getDag();
            List<Node> sources = dag.getSources();
            Map<String, String> shareCdcTaskId = subTaskDto.getShareCdcTaskId();
            if (shareCdcTaskId == null) {
                shareCdcTaskId = new HashMap<>();
                subTaskDto.setShareCdcTaskId(shareCdcTaskId);
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
            updateById(subTaskDto.getId(), update, user);
        }


    }

    public void updateNode(ObjectId objectId, String nodeId, Document param, UserDetail user) {
        SubTaskDto subTaskDto = checkExistById(objectId);
        Criteria criteria = Criteria.where("_id").is(objectId).and("dag.nodes").elemMatch(Criteria.where("id").is(nodeId));
        Document set = (Document) param.get("$set");
        for (String s : set.keySet()) {
            set.put("dag.nodes.$." + s, set.get(s));
            set.remove(s);
        }
        param.put("$set", set);

        Update update = Update.fromDocument(param);
        update(new Query(criteria), update, user);

        ObjectId parentId = subTaskDto.getParentId();
        Criteria taskCriteria = Criteria.where("_id").is(parentId).and("dag.nodes").elemMatch(Criteria.where("id").is(nodeId));
        criteria.and("_id").is(parentId);
        taskService.update(new Query(taskCriteria), update, user);
    }


    public void updateSyncProgress(ObjectId subTaskId, Document document) {
        document.forEach((k, v) -> {
            Criteria criteria = Criteria.where("_id").is(subTaskId);
            Update update = new Update().set("attrs.syncProgress." + k, v);
            update(new Query(criteria), update);
        });
    }

    public List<IncreaseSyncVO> increaseView(String subTaskId) {
        SubTaskDto subTaskDto = checkExistById(MongoUtils.toObjectId(subTaskId));
        Criteria criteria = Criteria.where("tags.subTaskId").is(subTaskId).and("tags.type").is("node");
        Query query = new Query(criteria);
        MongoTemplate mongoTemplate = repository.getMongoOperations();
        List<AgentStatDto> agentStatDtos = mongoTemplate.find(query, AgentStatDto.class);
        Map<String, AgentStatDto> agentStatMap = agentStatDtos.stream().collect(Collectors.toMap(a -> a.getTags().getNodeId(), a -> a, (a, a1)->a1));
        DAG dag = subTaskDto.getDag();
        List<Edge> fullEdges = fullEdges(dag);

        List<IncreaseSyncVO> increaseSyncVOS = new ArrayList<>();
        for (Edge edge : fullEdges) {
            String source = edge.getSource();
            String target = edge.getTarget();

            DataParentNode sourceNode = (DataParentNode) dag.getNode(source);
            DataParentNode targetNode = (DataParentNode) dag.getNode(target);

            AgentStatDto sourceAgentStatDto = agentStatMap.get(source);
            AgentStatDto targetAgentStatDto = agentStatMap.get(target);


            IncreaseSyncVO increaseSyncVO = new IncreaseSyncVO();
            increaseSyncVO.setSrcId(sourceNode.getId());
            increaseSyncVO.setSrcConnId(sourceNode.getConnectionId());
            if (sourceNode instanceof TableNode) {
                increaseSyncVO.setSrcTableName(((TableNode) sourceNode).getTableName());
            }
            increaseSyncVO.setTgtId(targetNode.getId());
            increaseSyncVO.setTgtConnId(targetNode.getConnectionId());
            if (targetNode instanceof TableNode) {
                increaseSyncVO.setTgtTableName(((TableNode) targetNode).getTableName());
            }
            increaseSyncVO.setDelay(0L);
            if (targetAgentStatDto != null) {
                double delay = targetAgentStatDto.getStatistics().getReplicateLag();
                if (delay > 0) {
                    increaseSyncVO.setDelay((long) delay);
                }
            }

            if (sourceAgentStatDto != null) {
                double cdcTime = sourceAgentStatDto.getStatistics().getCdcTime();
                if (cdcTime != 0) {
                    increaseSyncVO.setCdcTime(new Date((long) cdcTime));
                }
            }

            increaseSyncVOS.add(increaseSyncVO);

        }

        List<String> connectionIds = new ArrayList<>();
        for (IncreaseSyncVO increaseSyncVO : increaseSyncVOS) {
            if (StringUtils.isNotBlank(increaseSyncVO.getSrcId())) {
                connectionIds.add(increaseSyncVO.getSrcConnId());
            }

            if (StringUtils.isNotBlank(increaseSyncVO.getTgtId())) {
                connectionIds.add(increaseSyncVO.getTgtConnId());
            }
        }

        Criteria idCriteria = Criteria.where("_id").in(connectionIds);
        Query query1 = new Query(idCriteria);
        List<DataSourceConnectionDto> connections = dataSourceService.findAll(query1);
        Map<String, String> connectionNameMap = connections.stream().collect(Collectors.toMap(d -> d.getId().toHexString(), DataSourceConnectionDto::getName));
        for (IncreaseSyncVO increaseSyncVO : increaseSyncVOS) {
            increaseSyncVO.setSrcName(connectionNameMap.get(increaseSyncVO.getSrcConnId()));
            increaseSyncVO.setTgtName(connectionNameMap.get(increaseSyncVO.getTgtConnId()));
        }

        return increaseSyncVOS;
    }


    public List<Edge> fullEdges(DAG dag) {
        List<Edge> edges = dag.getEdges();
        List<Edge> fullEdges = new ArrayList<>();
        for (Edge edge : edges) {
            Node source = dag.getNode(edge.getSource());
            if (!source.isDataNode()) {
                continue;
            }

            Node target = dag.getNode(edge.getTarget());
            if (target.isDataNode()) {
                fullEdges.add(new Edge(source.getId(), target.getId()));
            }

            fullEdges.addAll(successorEdges(source.getId(), target, dag));
        }
        //???????????????
        Map<String, Edge> collect = fullEdges.stream().collect(Collectors.toMap(e -> e.getSource() + e.getTarget(), e -> e));
        fullEdges = new ArrayList<>(collect.values());
        return fullEdges;
    }

    private List<Edge> successorEdges(String source, Node target, DAG dag) {
        List<Edge> fullEdges = new ArrayList<>();
        List<Node> successors = dag.successors(target.getId());
        for (Node successor : successors) {
            if (successor.isDataNode()) {
                fullEdges.add(new Edge(source, successor.getId()));
            } else if (successor.getType().endsWith("_processor")) {
                fullEdges.addAll(successorEdges(source, successor, dag));
            }
        }

        return fullEdges;

    }


    public void increaseClear(ObjectId subTaskId, String srcNode, String tgtNode, UserDetail user) {
        //?????????????????????syncProgress????????????
        SubTaskDto subTaskDto = checkExistById(subTaskId, "attrs");
        clear(srcNode, tgtNode, user, subTaskDto);

    }

    private void clear(String srcNode, String tgtNode, UserDetail user, SubTaskDto subTaskDto) {
        Map<String, Object> attrs = subTaskDto.getAttrs();
        Object syncProgress = attrs.get("syncProgress");
        if (syncProgress == null) {
            return;
        }

        Map syncProgressMap = (Map) syncProgress;
        List<String> key = Lists.newArrayList(srcNode, tgtNode);

        syncProgressMap.remove(JsonUtil.toJsonUseJackson(key));

        Update update = Update.update("attrs", attrs);
        //?????????????????????????????? ????????????super, ?????????????????????????????????????????????
        super.updateById(subTaskDto.getId(), update, user);
    }

    public void increaseBacktracking(ObjectId subTaskId, String srcNode, String tgtNode, TaskDto.SyncPoint point, UserDetail user) {
        SubTaskDto subTaskDto = checkExistById(subTaskId, "parentId", "attrs", "dag");
        clear(srcNode, tgtNode, user, subTaskDto);


        //?????????????????????syncPoints?????????
        Criteria criteria = Criteria.where("_id").is(subTaskDto.getParentId());
        Query query = new Query(criteria);
        query.fields().include("syncPoints");
        DAG dag = subTaskDto.getDag();
        Node node = dag.getNode(tgtNode);
        if (node instanceof DataParentNode) {
            String connectionId = ((DataParentNode<?>) node).getConnectionId();
            if (StringUtils.isNotBlank(connectionId)) {
                TaskDto taskDto = taskService.findOne(query, user);
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
                    Criteria criteriaPoint = Criteria.where("_id").is(subTaskDto.getParentId()).and("syncPoints")
                            .elemMatch(Criteria.where("connectionId").is(connectionId));
                    Update update = Update.update("syncPoints.$", syncPoint);
                    //??????????????????
                    taskService.update(new Query(criteriaPoint), update);
                } else {
                    syncPoints.add(syncPoint);
                    Criteria criteriaPoint = Criteria.where("_id").is(subTaskDto.getParentId());
                    Update update = Update.update("syncPoints", syncPoints);
                    taskService.update(new Query(criteriaPoint), update);
                }
            }
        }

    }


    public void rename(ObjectId taskId, String newTaskName) {
        List<SubTaskDto> subTaskDtos = findByTaskId(taskId, "name");
        BulkOperations bulkOperations = repository.bulkOperations(BulkOperations.BulkMode.UNORDERED);

        for (SubTaskDto subTaskDto : subTaskDtos) {
            String name = subTaskDto.getName();
            int indexOf = name.indexOf(" (");
            name = newTaskName + name.substring(indexOf);

            Criteria criteria = Criteria.where("_id").is(subTaskDto.getId());
            Query query = new Query(criteria);

            Update update = Update.update("name", name);
            bulkOperations.updateOne(query, update);
        }

        if (CollectionUtils.isNotEmpty(subTaskDtos)) {
            bulkOperations.execute();
        }
    }

    public void reseted(ObjectId objectId, UserDetail userDetail) {
        SubTaskDto subTaskDto = checkExistById(objectId, "_id");
        if (subTaskDto != null) {
            super.updateById(objectId, Update.update("resetFlag", true), userDetail);
        }
    }

    public void deleted(ObjectId objectId, UserDetail userDetail) {
        SubTaskDto subTaskDto = checkExistById(objectId, "_id");
        if (subTaskDto != null) {
            super.updateById(objectId, Update.update("deleteFlag", true), userDetail);
        }
    }

    public boolean checkPdkSubTask(SubTaskDto subTaskDto, UserDetail user) {
        DAG dag = subTaskDto.getDag();
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

    public boolean checkDeleteFlag(ObjectId id, UserDetail user) {
        SubTaskDto subTaskDto = checkExistById(id, "deleteFlag");
        if (subTaskDto.getDeleteFlag() != null) {
            return subTaskDto.getDeleteFlag();
        }
        return false;
    }

    public boolean checkResetFlag(ObjectId id, UserDetail user) {
        SubTaskDto subTaskDto = checkExistById(id, "resetFlag");
        if (subTaskDto.getResetFlag() != null) {
            return subTaskDto.getResetFlag();
        }
        return false;
    }
    public void resetFlag(ObjectId id, UserDetail user, String flag) {
        updateById(id, new Update().unset(flag), user);
    }

    public void startPlanMigrateDagTask() {
        Criteria migrateCriteria = Criteria.where("syncType").is("migrate")
                .and("status").is(SubTaskDto.STATUS_EDIT)
                .and("planStartDateFlag").is(true)
                .and("planStartDate").lte(System.currentTimeMillis());
        Query taskQuery = new Query(migrateCriteria);
        List<TaskDto> taskList = taskService.findAll(taskQuery);
        if (CollectionUtils.isNotEmpty(taskList)) {
            List<String> userIdList = taskList.stream().map(TaskDto::getUserId).distinct().collect(Collectors.toList());
            List<UserDetail> userList = userService.getUserByIdList(userIdList);

            Map<String, UserDetail> userMap = new HashMap<>();
            if (CollectionUtils.isNotEmpty(userList)) {
                userMap = userList.stream().collect(Collectors.toMap(UserDetail::getUserId, Function.identity()));
            }

            List<ObjectId> taskIdList = taskList.stream().map(TaskDto::getId).collect(Collectors.toList());

            Criteria supTaskCriteria = Criteria.where("parentId").in(taskIdList);
            Query supTaskQuery = new Query(supTaskCriteria);
            List<SubTaskDto> subTaskList = findAll(supTaskQuery);
            if (CollectionUtils.isNotEmpty(subTaskList)) {

                Map<String, UserDetail> finalUserMap = userMap;
                subTaskList.forEach(subTaskDto -> start(subTaskDto.getId(), finalUserMap.get(subTaskDto.getUserId())));
            }
        }
    }
}
