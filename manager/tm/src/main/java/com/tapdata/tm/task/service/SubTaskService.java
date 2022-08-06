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
//import com.tapdata.tm.commons.dag.process.MigrateFieldRenameProcessorNode;
//import com.tapdata.tm.commons.dag.process.MigrateJsProcessorNode;
//import com.tapdata.tm.commons.dag.process.TableRenameProcessNode;
//import com.tapdata.tm.commons.schema.DataSourceConnectionDto;
//import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
//import com.tapdata.tm.commons.schema.MetadataInstancesDto;
//import com.tapdata.tm.commons.task.dto.*;
//import com.tapdata.tm.commons.task.dto.progress.SubTaskSnapshotProgress;
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
//import java.util.concurrent.atomic.AtomicBoolean;
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
//
//    public void updateStatus(ObjectId taskId, String status) {
//        Query query = Query.query(Criteria.where("_id").is(taskId));
//        Update update = Update.update("status", status);
//        update(query, update);
//    }
//}
