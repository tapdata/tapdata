package com.tapdata.tm.task.controller;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.alarm.service.AlarmService;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.commons.dag.DAG;
import com.tapdata.tm.commons.dag.Node;
import com.tapdata.tm.commons.dag.SchemaTransformerResult;
import com.tapdata.tm.commons.dag.nodes.DatabaseNode;
import com.tapdata.tm.commons.dag.vo.FieldProcess;
import com.tapdata.tm.commons.dag.vo.TestRunDto;
import com.tapdata.tm.commons.schema.DataSourceDefinitionDto;
import com.tapdata.tm.commons.schema.MetadataTransformerItemDto;
import com.tapdata.tm.commons.schema.TransformerWsMessageDto;
import com.tapdata.tm.commons.schema.TransformerWsMessageResult;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflowinsight.dto.DataFlowInsightStatisticsDto;
import com.tapdata.tm.ds.service.impl.DataSourceDefinitionService;
import com.tapdata.tm.message.constant.Level;
import com.tapdata.tm.message.constant.MsgTypeEnum;
import com.tapdata.tm.message.service.MessageService;
import com.tapdata.tm.metadatadefinition.param.BatchUpdateParam;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.metadatainstance.service.MetadataInstancesService;
import com.tapdata.tm.task.bean.*;
import com.tapdata.tm.task.entity.TaskEntity;
import com.tapdata.tm.task.param.LogSettingParam;
import com.tapdata.tm.task.service.*;
import com.tapdata.tm.task.vo.*;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.service.WorkerService;
import io.github.openlg.graphlib.Graph;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * @Date: 2021/11/03
 * @Description:
 */
@Tag(name = "Task", description = "Task相关接口")
@RestController
@Slf4j
@RequestMapping({"/api/Task", "/api/task"})
@Setter(onMethod_ = {@Autowired})
public class TaskController extends BaseController {
    private TaskService taskService;
    private MessageService messageService;
    private TransformSchemaAsyncService transformSchemaAsyncService;
    private TransformSchemaService transformSchemaService;
    private MetadataDefinitionService metadataDefinitionService;
    private DataSourceDefinitionService definitionService;
    private TaskCheckInspectService taskCheckInspectService;
    private MetadataInstancesService metadataInstancesService;
    private TaskNodeService taskNodeService;
    private TaskSaveService taskSaveService;
    private TaskStartService taskStartService;
    private SnapshotEdgeProgressService snapshotEdgeProgressService;
    private TaskRecordService taskRecordService;
    private WorkerService workerService;
    private AlarmService alarmService;

    private TaskResetLogService taskResetLogService;

    /**
     * Create a new instance of the model and persist it into the data source
     *
     * @param task task
     * @return TaskDto
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<TaskDto> save(@RequestBody TaskDto task) {
        task.setId(null);
        return success(taskService.create(task, getLoginUser()));
    }

    /**
     * Patch an existing model instance or insert a new one into the data source
     *
     * @param task task
     * @return TaskDto
     */
    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
    @PatchMapping()
    public ResponseMessage<TaskDto> update(@RequestBody TaskDto task) {
        UserDetail user = getLoginUser();
        taskCheckInspectService.getInspectFlagDefaultFlag(task, user);
        taskSaveService.supplementAlarm(task, user);
        task.setStatus(null);
        return success(taskService.updateById(task, user));
    }


    /**
     * 获取数据开发列表
     * 同步任务类型
     * sync 同步任务 (即数据开发 )
     * migrate迁移  （ 即数据复制）
     * logCollector 挖掘任务
     *
     * @param filterJson filterJson
     * @return TaskDto
     */
    @Operation(summary = "获取数据开发列表")
    @GetMapping
    public ResponseMessage<Page<TaskDto>> find(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);

        if (filter == null) {
            filter = new Filter();
        }
        if (!filterJson.contains("limit") && isAgentReq()) {
            filter.setLimit(10000);
        }

        return success(taskService.find(filter, getLoginUser()));
    }


    @Operation(summary = "获取数据开发列表")
    @GetMapping("scanTask")
    public ResponseMessage<Page<TaskDto>> scanTask(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);

        if (filter == null) {
            filter = new Filter();
        }

        Page<TaskDto> taskDtoPage = taskService.scanTask(filter, getLoginUser());
        return success(taskDtoPage);
    }

    /**
     * Replace an existing model instance or insert a new one into the data source
     *
     * @param task task
     * @return TaskDto
     */
    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<TaskDto> put(@RequestBody TaskDto task) {
        return success(taskService.replaceOrInsert(task, getLoginUser()));
    }

    /**
     * Check whether a model instance exists in the data source
     *
     * @return map
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
    public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
        long count = taskService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     *web调用，修改任务属性
     * @param task task
     * @return TaskDto
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<TaskDto> updateById(@PathVariable("id") String id, @RequestBody TaskDto task) {
        task.setId(MongoUtils.toObjectId(id));
        UserDetail user = getLoginUser();
        taskCheckInspectService.getInspectFlagDefaultFlag(task, user);
        taskSaveService.supplementAlarm(task, user);
        task.setStatus(null);
        TaskDto taskDto = taskService.updateById(task, user);
        return success(taskDto);
    }


    /**
     *  编辑以一个已经存在的任务
     * @param task task
     * @return TaskDto
     */
    @Operation(summary = "编辑以一个已经存在的任务")
    @PatchMapping("confirm/{id}")
    public ResponseMessage<TaskDto> confirmById(@PathVariable("id") String id, @RequestParam(value = "confirm", required = false, defaultValue = "false") Boolean confirm,
                                                @RequestBody TaskDto task) {
        task.setId(MongoUtils.toObjectId(id));
        UserDetail user = getLoginUser();
        taskCheckInspectService.getInspectFlagDefaultFlag(task, user);
        taskSaveService.supplementAlarm(task, user);
        TaskDto taskDto = taskService.confirmById(task, user, confirm);
        return success(taskDto);
    }

    /**
     * 新增一个任务
     *
     * @param task task
     * @return TaskDto
     */
    @Operation(summary = "新增一个任务")
    @PatchMapping("confirm")
    public ResponseMessage<TaskDto> confirmById(@RequestParam(value = "confirm", required = false, defaultValue = "false") Boolean confirm,
                                                @RequestBody TaskDto task) {
        UserDetail user = getLoginUser();
        taskSaveService.supplementAlarm(task, user);
        return success(taskService.confirmById(task, user, confirm));
    }

    /**
     * auto Patch attributes for a model instance and persist it into the data source
     *
     * @param task task
     * @return TaskDto
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("confirmStart/{id}")
    public ResponseMessage<TaskDto> confirmStart(@PathVariable("id") String id, @RequestParam(value = "confirm", required = false, defaultValue = "false") Boolean confirm,
                                                 @RequestBody TaskDto task) {
        task.setId(MongoUtils.toObjectId(id));
        UserDetail user = getLoginUser();

        TaskDto taskDto = taskService.confirmStart(task, user, confirm);

        return success(taskDto);
    }

    /**
     * Find a model instance by {{id}} from the data source
     *
     * @param fieldsJson fieldsJson
     * @return TaskDto
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<TaskDto> findById(@PathVariable("id") String id,
                                             @RequestParam(value = "fields", required = false) String fieldsJson,
                                             @RequestParam(value = "taskRecordId", required = false) String taskRecordId) {
        Field fields = parseField(fieldsJson);
        UserDetail user = getLoginUser();
        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(id), fields, user);
        if (taskDto != null) {
            if (StringUtils.isNotBlank(taskRecordId) && !taskRecordId.equals(taskDto.getTaskRecordId())) {
                taskDto = taskRecordService.queryTask(taskRecordId, user.getUserId());
            }

            taskDto.setCreator(StringUtils.isNotBlank(user.getUsername()) ? user.getUsername() : user.getEmail());
            taskCheckInspectService.getInspectFlagDefaultFlag(taskDto, user);

            taskNodeService.checkFieldNode(taskDto, user);

            // set hostName;
            workerService.setHostName(taskDto);

            // supplement startTime
            if (Objects.isNull(taskDto.getStartTime())) {
                TaskDto taskRecord = taskRecordService.queryTask(taskDto.getTaskRecordId(), user.getUserId());
                if (Objects.nonNull(taskRecord)) {
                    taskDto.setStartTime(taskRecord.getStartTime());
                }
            }
        }
        return success(taskDto);
    }

    /**
     * 获取任务详情
     *
     * @param fieldsJson fieldsJson
     * @return TaskDetailVo
     */
    @Operation(summary = "获取任务详情")
    @GetMapping("findTaskDetailById/{id}")
    public ResponseMessage<TaskDetailVo> findTaskDetailById(@PathVariable("id") String id,
                                                            @RequestParam(value = "fields", required = false) String fieldsJson) {
        Field fields = parseField(fieldsJson);
        TaskDetailVo taskDetailVo = taskService.findTaskDetailById(id, fields, getLoginUser());
        return success(taskDetailVo);
    }

    /**
     * 查询编辑的任务版本，如果temp存在则返回temp的信息
     *
     * @param fieldsJson fieldsJson
     * @return TaskDto
     */
    @Operation(summary = "查询编辑的任务版本，如果temp存在则返回temp的信息")
    @GetMapping("edit/{id}")
    public ResponseMessage<TaskDto> findEditDataById(@PathVariable("id") String id,
                                                     @RequestParam(value = "fields", required = false) String fieldsJson) {
        Field fields = parseField(fieldsJson);
        TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(id), fields, getLoginUser());
//        if (taskDto != null) {
//            if (taskDto.getTemp() != null) {
//                taskDto.setDag(taskDto.getTemp());
//                taskDto.setTemp(null);
//            }
//        }
        return success(taskDto);
    }

    /**
     * Replace attributes for a model instance and persist it into the data source.
     *
     * @param task task
     * @return TaskDto
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PutMapping("{id}")
    public ResponseMessage<TaskDto> replceById(@PathVariable("id") String id, @RequestBody TaskDto task) {
        return success(taskService.replaceById(MongoUtils.toObjectId(id), task, getLoginUser()));
    }

    /**
     * Replace attributes for a model instance and persist it into the data source.
     *
     * @param task task
     * @return TaskDto
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PostMapping("{id}/replace")
    public ResponseMessage<TaskDto> replaceById2(@PathVariable("id") String id, @RequestBody TaskDto task) {
        return success(taskService.replaceById(MongoUtils.toObjectId(id), task, getLoginUser()));
    }


    /**
     * Delete a model instance by {{id}} from the data source
     *
     * @param id id
     * @return TaskDto
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        taskService.remove(MongoUtils.toObjectId(id), getLoginUser());
        return success();
    }

    /**
     * Check whether a model instance exists in the data source
     *
     * @param id id
     * @return TaskDto
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @GetMapping("{id}/exists")
    public ResponseMessage<HashMap<String, Boolean>> checkById1(@PathVariable("id") String id) {
        long count = taskService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     * Count instances of the model matched by where from the data source
     *
     * @param whereJson whereJson
     * @return map
     */
    @Operation(summary = "Count instances of the model matched by where from the data source")
    @GetMapping("count")
    public ResponseMessage<HashMap<String, Long>> count(@RequestParam("where") String whereJson) {
        Where where = parseWhere(whereJson);
        if (where == null) {
            where = new Where();
        }
        long count = taskService.count(where, getLoginUser());
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }

    /**
     * Find first instance of the model matched by filter from the data source.
     *
     * @param filterJson filterJson
     * @return TaskDto
     */
    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<TaskDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        Where where = filter.getWhere();
        Boolean deleted = (Boolean) where.get("is_deleted");
        if (deleted == null) {
            where.put("is_deleted", false);
        }
        return success(taskService.findOne(filter, getLoginUser()));
    }

    /**
     * Update instances of the model matched by {{where}} from the data source.
     *
     * @param whereJson whereJson
     * @return map
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody String reqBody) {
//        log.info("subTask updateByWhere, whereJson:{},reqBody:{}",whereJson,reqBody);
        Where where = parseWhere(whereJson);
        Document update = Document.parse(reqBody);
        if (!update.containsKey("$set") && !update.containsKey("$setOnInsert") && !update.containsKey("$unset")) {
            Document _body = new Document();
            _body.put("$set", update);
            update = _body;
        } else if (update.containsKey("$set")) {
           Document ping = (Document) update.get("$set");
           if (ping.containsKey("pingTime")) {
               ping.put("pingTime", System.currentTimeMillis());
           }
        }

        long count = taskService.updateByWhere(where, update, getLoginUser());
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);

        Object id = where.get("_id");

        //更新完任务，addMessage
        try {
            String status = update.getString("status");
            if (StringUtils.isNotEmpty(status) && null != id) {
                String idString = id.toString();
                TaskDto taskDto = taskService.findById(MongoUtils.toObjectId(idString));
                String name = taskDto.getName();
                log.info("subtask addMessage ,id :{},name :{},status:{}  ", id, name, status);
                if ("error".equals(status)) {
                    messageService.addMigration(name, idString, MsgTypeEnum.STOPPED_BY_ERROR, Level.ERROR, getLoginUser());
                } else if ("running".equals(status)) {
                    messageService.addMigration(name, idString, MsgTypeEnum.CONNECTED, Level.INFO, getLoginUser());
                }
            }
        } catch (Exception e) {
            log.error("任务状态添加 message 异常",e);
        }
        return success(countValue);
    }

    /**
     * Update an existing model instance or insert a new one into the data source based on the where criteria.
     *
     * @param whereJson whereJson
     * @return TaskDto
     */
    @Operation(summary = "Update an existing model instance or insert a new one into the data source based on the where criteria.")
    @PostMapping("upsertWithWhere")
    public ResponseMessage<TaskDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody TaskDto task) {
        Where where = parseWhere(whereJson);
        return success(taskService.upsertByWhere(where, task, getLoginUser()));
    }


    @Operation(summary = "运行时信息接口")
    @GetMapping("runtimeInfo/{taskId}")
    public ResponseMessage<RunTimeInfo> runtimeInfo(@PathVariable("taskId") String taskId
            , @RequestParam(value = "endTime", required = false) Long endTime) {
        return success(taskService.runtimeInfo(MongoUtils.toObjectId(taskId), endTime, getLoginUser()));

    }


    @GetMapping("view/sync/overview/{taskId}")
    public ResponseMessage<FullSyncVO> syncOverview(@PathVariable("taskId") String taskId) {
        return success(snapshotEdgeProgressService.syncOverview(taskId));
    }

    @GetMapping("view/sync/table/{taskId}")
    public ResponseMessage<Page<TableStatus>> syncTableView(@PathVariable("taskId") String subTaskId,
                                                            @RequestParam(value = "skip", required = false, defaultValue = "0") Long skip,
                                                            @RequestParam(value = "limit", required = false, defaultValue = "20") Integer limit) {
        return success(snapshotEdgeProgressService.syncTableView(subTaskId, skip, limit));
    }

//    @GetMapping("view/increase/{taskId}")
//    public ResponseMessage<List<IncreaseSyncVO>> increaseView(@PathVariable("taskId") String taskId,
//                                                              @RequestParam(value = "skip", required = false, defaultValue = "0") Long skip,
//                                                              @RequestParam(value = "limit", required = false, defaultValue = "20") Integer limit) {
//        return success(taskService.increaseView(taskId, getLoginUser()));
//    }

    @PostMapping("increase/clear/{taskId}")
    public ResponseMessage<List<IncreaseSyncVO>> increaseClear(@PathVariable("taskId") String taskId,
                                                               @RequestParam("srcNode") String srcNode,
                                                               @RequestParam("tgtNode") String tgtNode) {
        taskService.increaseClear(MongoUtils.toObjectId(taskId), srcNode, tgtNode, getLoginUser());
        return success();
    }

    @PostMapping("increase/backtracking/{taskId}")
    public ResponseMessage<List<IncreaseSyncVO>> increaseBacktracking(@PathVariable("taskId") String taskId,
                                                                      @RequestParam("srcNode") String srcNode,
                                                                      @RequestParam("tgtNode") String tgtNode,
                                                                      @RequestBody TaskDto.SyncPoint point) {
        taskService.increaseBacktracking(MongoUtils.toObjectId(taskId), srcNode, tgtNode, point, getLoginUser());
        return success();

    }

    @Operation(summary = "复制同步任务")
    @PutMapping("copy/{id}")
    public ResponseMessage<TaskDto> copy(@PathVariable("id") String id) {
        return success(taskService.copy(MongoUtils.toObjectId(id), getLoginUser()));
    }


    @Operation(summary = "启动同步任务")
    @PutMapping("start/{id}")
    public ResponseMessage<Void> start(@PathVariable("id") String id) {
        taskService.start(MongoUtils.toObjectId(id), getLoginUser());
        return success();
    }

    @Operation(summary = "暂停同步任务")
    //@PutMapping("pause/{id}")
    public ResponseMessage<Void> pause(@PathVariable("id") String id
            , @RequestParam(value = "force", defaultValue = "false") Boolean force) {
        taskService.pause(MongoUtils.toObjectId(id), getLoginUser(), force);
        return success();
    }

    @Operation(summary = "重置同步任务")
    @PutMapping("renew/{id}")
    public ResponseMessage<Void> renew(@PathVariable("id") String id) {
        taskService.renew(MongoUtils.toObjectId(id), getLoginUser());
        return success();
    }

    @Operation(summary = "停止同步任务")
    @PutMapping("stop/{id}")
    public ResponseMessage<TaskDto> stop(@PathVariable("id") String id
            , @RequestParam(value = "force", defaultValue = "false") Boolean force) {
        taskService.pause(MongoUtils.toObjectId(id), getLoginUser(), force);
        return success();
    }

    @Operation(summary = "子任务已经成功运行回调接口")
    @PostMapping("running/{id}")
    public ResponseMessage<TaskOpResp> running(@PathVariable("id") String id) {
        log.info("subTask running status report by http, id = {}", id);
        String successId = taskService.running(MongoUtils.toObjectId(id), getLoginUser());
        TaskOpResp taskOpResp = new TaskOpResp();
        if (StringUtils.isBlank(successId)) {
            taskOpResp = new TaskOpResp(successId);
        }

        return success(taskOpResp);
    }

    /**
     * 收到任务运行失败的消息
     * @param id
     */
    @Operation(summary = "任务运行失败回调接口")
    @PostMapping("runError/{id}")
    public ResponseMessage<TaskOpResp> runError(@PathVariable("id") String id, @RequestParam(value = "errMsg", required = false) String errMsg,
                                                @RequestParam(value = "errStack", required = false) String errStack) {
        String successId = taskService.runError(MongoUtils.toObjectId(id), getLoginUser(), errMsg, errStack);
        TaskOpResp taskOpResp = new TaskOpResp();
        if (StringUtils.isBlank(successId)) {
            taskOpResp = new TaskOpResp(successId);
        }

        return success(taskOpResp);
    }


    /**
     * 收到任务运行完成的消息
     * @param id
     */
    @Operation(summary = "任务执行完成回调接口")
    @PostMapping("complete/{id}")
    public ResponseMessage<TaskOpResp> complete(@PathVariable("id") String id) {
        String successId = taskService.complete(MongoUtils.toObjectId(id), getLoginUser());
        TaskOpResp taskOpResp = new TaskOpResp();
        if (StringUtils.isBlank(successId)) {
            taskOpResp = new TaskOpResp(successId);
        }

        return success(taskOpResp);
    }


    /**
     * 收到任务已经停止的消息
     * @param id
     */
    @Operation(summary = "停止成功回调接口")
    @PostMapping("stopped/{id}")
    public ResponseMessage<TaskOpResp> stopped(@PathVariable("id") String id) {
        String successId = taskService.stopped(MongoUtils.toObjectId(id), getLoginUser());
        TaskOpResp taskOpResp = new TaskOpResp();
        if (StringUtils.isBlank(successId)) {
            taskOpResp = new TaskOpResp(successId);
        }

        return success(taskOpResp);
    }


    /**
     * 重置任务接口
     * @param id
     */
    @Operation(summary = "重置任务接口")
    @PostMapping("renew/{id}")
    public ResponseMessage<Void> renew(@PathVariable("id") String id, @RequestParam(value = "force", defaultValue = "false") Boolean force) {
        taskService.renew(MongoUtils.toObjectId(id), getLoginUser());
        return success();
    }


    /**
     * 更新子任务的node跟任务中的node
     * @param taskId 子任务id
     * @param nodeId nodeId
     * @param reqBody 更新的参数
     * @return
     */
    @Operation(summary = "更新子任务的node跟任务中的node")
    @PostMapping("node/{taskId}/{nodeId}")
    public ResponseMessage<Void> updateNode(@PathVariable("taskId") String taskId, @PathVariable("nodeId") String nodeId, @RequestBody String reqBody) {
        Document update = Document.parse(reqBody);
        if (!update.containsKey("$set") && !update.containsKey("$setOnInsert") && !update.containsKey("$unset")) {
            Document _body = new Document();
            _body.put("$set", update);
            update = _body;
        }
        taskService.updateNode(MongoUtils.toObjectId(taskId), nodeId, update, getLoginUser());
        return success();
    }




    @GetMapping("byCacheName/{cacheName}")
    public ResponseMessage<TaskDto> findByCacheName(@PathVariable("cacheName") String cacheName) {
        return success(taskService.findByCacheName(cacheName, getLoginUser()));
    }


    @GetMapping("history")
    public ResponseMessage<TaskDto> queryHistory(@RequestParam("id") String id, @RequestParam("time") Long time) {
        TaskDto taskDto  = taskService.findByVersionTime(id, time);
        return success(taskDto);
    }

    @DeleteMapping("history")
    public ResponseMessage<TaskDto> cleanHistory(@RequestParam("id") String id, @RequestParam("time") Long time) {
        taskService.clean(id, time);
        return success();
    }

    @PutMapping("batchStart")
    public ResponseMessage<List<MutiResponseMessage>> batchStart(@RequestParam("taskIds") List<String> taskIds,
                                                                 HttpServletRequest request,
                                                                 HttpServletResponse response) {
        List<ObjectId> taskObjectIds = taskIds.stream().map(MongoUtils::toObjectId).collect(Collectors.toList());
        List<MutiResponseMessage> responseMessages = taskService.batchStart(taskObjectIds, getLoginUser(), request, response);
        return success(responseMessages);
    }

    @PutMapping("batchStop")
    public ResponseMessage<List<MutiResponseMessage>> batchStop(@RequestParam("taskIds") List<String> taskIds,
                                                                HttpServletRequest request,
                                                                HttpServletResponse response) {
        List<ObjectId> taskObjectIds = taskIds.stream().map(MongoUtils::toObjectId).collect(Collectors.toList());
        List<MutiResponseMessage> responseMessages = taskService.batchStop(taskObjectIds, getLoginUser(), request, response);

        //add message
        List<TaskEntity> taskEntityList = taskService.findByIds(taskObjectIds);
        try {
            if (CollectionUtils.isNotEmpty(taskEntityList)) {
                for (TaskEntity task : taskEntityList) {
                    messageService.addMigration(task.getName(), task.getId().toString(), MsgTypeEnum.PAUSED, Level.INFO, getLoginUser());
                }
            }
        } catch (Exception e) {
            log.warn("add migration message error");
        }
        return success(responseMessages);
    }

    @DeleteMapping("batchDelete")
    public ResponseMessage<List<MutiResponseMessage>> batchDelete(@RequestParam("taskIds") List<String> taskIds,
                                                                  HttpServletRequest request,
                                                                  HttpServletResponse response) {
        List<ObjectId> taskObjectIds = taskIds.stream().map(MongoUtils::toObjectId).collect(Collectors.toList());
        List<MutiResponseMessage> responseMessages = taskService.batchDelete(taskObjectIds, getLoginUser(), request, response);
        return success(responseMessages);
    }

    @Operation(summary = "重置任务接口")
    @PatchMapping("batchRenew")
    public ResponseMessage<List<MutiResponseMessage>> batchRenew(@RequestParam("taskIds") List<String> taskIds,
                                                                 HttpServletRequest request,
                                                                 HttpServletResponse response) {
        List<ObjectId> taskObjectIds = taskIds.stream().map(MongoUtils::toObjectId).collect(Collectors.toList());
        List<MutiResponseMessage> responseMessages = taskService.batchRenew(taskObjectIds, getLoginUser(), request, response);
        return success(responseMessages);
    }

    @GetMapping("search/logCollector")
    public ResponseMessage<LogCollectorResult> searchLogCollector(@RequestParam("key") String key) {
        return success(taskService.searchLogCollector(key));
    }

    /**
     * 模型推演
     */
    @PostMapping("/metadata")
    public ResponseMessage<List<SchemaTransformerResult>> transformSchema(@RequestBody TaskDto taskDto) {

        if (taskDto.getDag() == null || taskDto.getDag().getSources() == null || taskDto.getDag().getSources().size() == 0) {
            return success(Collections.emptyList());
        }

        ObjectId taskDtoId = taskDto.getId();
        if (Objects.nonNull(taskDtoId)) {
            TaskDto dto = taskService.findById(taskDtoId);
            if (Objects.nonNull(dto)) {
                taskDto.setSyncType(dto.getSyncType());
                taskDto.setType(dto.getType());
            }
        }

        DAG dag = taskDto.getDag();
        if (dag != null) {
            List<Node> nodes = dag.getNodes();
            if (CollectionUtils.isNotEmpty(nodes)) {
                for (Node node : nodes) {
                    if (node instanceof DatabaseNode) {
                        DatabaseNode databaseNode = ((DatabaseNode) node);

                        if ("all".equals(taskDto.getRollback())) {
                            databaseNode.setFieldProcess(null);
                        } else if (StringUtils.isNotBlank(taskDto.getRollbackTable())) {
                            List<FieldProcess> fieldProcess = databaseNode.getFieldProcess();
                            for (FieldProcess process : fieldProcess) {
                                if (process.getTableName().equals(taskDto.getRollbackTable())) {
                                    fieldProcess.remove(process);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }

        UserDetail user = getLoginUser();
        if (TaskDto.SYNC_TYPE_MIGRATE.equals(taskDto.getSyncType())) {
            transformSchemaAsyncService.transformSchema(taskDto, user, taskDtoId);
        } else {
            transformSchemaService.transformSchema(taskDto, user);
        }
        return success();

    }


    @PostMapping("/tranModelVersionControl")
    public ResponseMessage<Map<String, Boolean>> tranModelVersionControl(@RequestBody TranModelReqDto reqDto) {

        List<DatabaseNode> nodes = reqDto.getNodes();

        Graph<DatabaseNode, String> graph = new Graph<>();
        for (DatabaseNode node : nodes) {
            graph.setNode(node.getId(), node);
            if (node.getInputLanes() != null && node.getInputLanes().size() > 0) {
                for (String inputLane : node.getInputLanes()) {
                    graph.setEdge(inputLane, node.getId());
                }
            }
            if (node.getOutputLanes() != null && node.getOutputLanes().size() > 0) {
                for (String output : node.getOutputLanes()) {
                    graph.setEdge(node.getId(), output);
                }
            }
        }

        //a -> b -> c
        //d -> g

        Stream<Graph<DatabaseNode, String>> graphList;
        if (reqDto.getNodeId() != null) {
            graphList = graph.components().stream().filter(g -> g.hasNode(reqDto.getNodeId()));
        } else {
            graphList = graph.components().stream();
        }

        Map<String, Boolean> result = new HashMap<>();
        graphList.forEach(subGraph -> {
            AtomicBoolean subDagSupportMapping = new AtomicBoolean(true);
            for (String nodeId : subGraph.getNodes()) {
                DatabaseNode node = subGraph.getNode(nodeId);
                if (node.getDatabaseType() != null) {
                    DataSourceDefinitionDto definitionDto = definitionService.getByDataSourceType(node.getDatabaseType(), getLoginUser());

                    if (definitionDto == null) {
                        subDagSupportMapping.set(false);
                        break;
                    }
                }
            }
            subGraph.getSinks().forEach(id -> result.put(id, subDagSupportMapping.get()));
        });

        return success(result);
    }

    /**
     * 首页图
     *
     * @return map
     */
    @Operation(summary = "Chart")
    @GetMapping("/chart")
    public ResponseMessage<Map<String, Object>> chart() {
        return success(taskService.chart(getLoginUser()));
    }


    /**
     * @return true 重复名称， false 不重复
     */
    @Operation(summary = "check task repeat name")
    @PostMapping("checkName")
    public ResponseMessage<Boolean> checkName(@RequestBody CheckNameReq req) {

        if (StringUtils.isBlank(req.getName())) {
            return success(true);
        }

        ObjectId objectId = null;
        if (StringUtils.isNotBlank(req.getId())) {
            objectId = MongoUtils.toObjectId(req.getId());
        }
        return success(taskService.checkTaskNameNotError(req.getName(), getLoginUser(), objectId));
    }

    @Operation(summary = "任务导出")
    @GetMapping("batch/load")
    public void batchLoadTasks(@RequestParam("taskId") List<String> taskId, HttpServletResponse response) {
        taskService.batchLoadTask(response, taskId, getLoginUser());
    }

    @Operation(summary = "任务导入")
    @PostMapping(path = "batch/import", consumes = MediaType.MULTIPART_FORM_DATA_VALUE)
    public ResponseMessage<Void> upload(@RequestParam(value = "file") MultipartFile file,
                                        @RequestParam(value = "cover", required = false, defaultValue = "false") boolean cover,
                                        @RequestParam(value = "listtags", required = false) String listtags) {
        List<String> tags = Lists.newArrayList();
        if (StringUtils.isNoneBlank(listtags)) {
            tags = JSON.parseArray(listtags, String.class);
        }
        taskService.batchUpTask(file, getLoginUser(), cover, tags);
        return success();
    }


    @Operation(summary = "校验任务是否从来没有跑过，或者重置过")
    @GetMapping("checkRun/{id}")
    public ResponseMessage<Map<String, Boolean>> checkRun(@PathVariable("id") String taskId) {
        Map<String, Boolean> returnMap = new HashMap<>();
        returnMap.put("neverRun", taskService.checkRun(taskId, getLoginUser()));
        return success(returnMap);

    }

    /**
     *
     */
    @Operation(summary = " 批量修改所属类别")
    @PatchMapping("batchUpdateListtags")
    public ResponseMessage<List<String>> batchUpdateListTags(@RequestBody BatchUpdateParam batchUpdateParam) {
        return success(metadataDefinitionService.batchUpdateListTags("Task", batchUpdateParam, getLoginUser()));
    }

    @Operation(summary = "获取建表语句")
    @GetMapping("getTableDDL")
    public ResponseMessage<Void> getTableDDL(@RequestBody TaskDto taskDto) {
        taskService.getTableDDL(taskDto);
        return success();
    }


    @Operation(summary = "模型推演结果推送")
    @PostMapping("transformer/result")
    public ResponseMessage<Void> transformerResult(@RequestBody TransformerWsMessageResult result) {
        transformSchemaService.transformerResult(getLoginUser(), result, false);
        return success();
    }
    @Operation(summary = "模型推演结果推送")
    @PostMapping("transformer/resultWithHistory")
    public ResponseMessage<Void> transformerResultHistory(@RequestBody TransformerWsMessageResult result) {
        transformSchemaService.transformerResult(getLoginUser(), result, true);
        return success();
    }

    @Operation(summary = "复制任务节点表字段数据")
    @GetMapping("getNodeTableInfo")
    public ResponseMessage<Page<MetadataTransformerItemDto>> getNodeTableInfo(
            @RequestParam("taskId") String taskId,
            @RequestParam(value = "taskRecordId", required = false) String taskRecordId,
            @RequestParam("nodeId") String nodeId,
            @RequestParam(value = "searchTable", required = false) String searchTableName,
            @RequestParam(value = "page", defaultValue = "1") Integer page,
            @RequestParam(value = "pageSize", defaultValue = "10") Integer pageSize) {
        return success(taskNodeService.getNodeTableInfo(taskId, taskRecordId, nodeId, searchTableName, page, pageSize, getLoginUser()));
    }


    @GetMapping("transformParam/{taskId}")
    public ResponseMessage<TransformerWsMessageDto> findTransformParam(@PathVariable("taskId") String taskId) {
        TransformerWsMessageDto dto = taskService.findTransformParam(taskId, getLoginUser());
        return success(dto);
    }

    @GetMapping("transformAllParam/{taskId}")
    public ResponseMessage<TransformerWsMessageDto> findTransformAllParam(@PathVariable("taskId") String taskId) {
        TransformerWsMessageDto dto = taskService.findTransformAllParam(taskId, getLoginUser());
        return success(dto);
    }


    @PostMapping("dag")
    public ResponseMessage<Void> updateDagAndHistory(@RequestBody TaskDto taskDto) {
        taskService.updateDag(taskDto, getLoginUser(), true);
        return success();
    }

    @PostMapping("dagNotHistory")
    public ResponseMessage<Void> updateDag(@RequestBody TaskDto taskDto) {
        taskService.updateDag(taskDto, getLoginUser(), false);
        return success();
    }

    @PostMapping("migrate-js/test-run")
    @Operation(description = "js节点试运行")
    public ResponseMessage<Void> testRun(@RequestBody TestRunDto dto) {
        taskNodeService.testRunJsNode(dto, getLoginUser());
        return success();
    }

    @PostMapping("migrate-js/save-result")
    @Operation(description = "js节点运行结果保存")
    public ResponseMessage<Void> saveResult(@RequestBody JsResultDto jsResultDto) {
        taskNodeService.saveResult(jsResultDto);
        return success();
    }

    @GetMapping("migrate-js/get-result")
    @Operation(description = "js节点试运行结果获取")
    public ResponseMessage<JsResultVo> getRun(@RequestParam String taskId,
                                         @RequestParam String jsNodeId, @RequestParam Long version) {
        return taskNodeService.getRun(taskId, jsNodeId, version);
    }

    	@Operation(summary = "更新子任务断点信息")
	@PostMapping("syncProgress/{taskId}")
	public ResponseMessage<Void> updateSyncProgress(@PathVariable("taskId") String taskId, @RequestBody String body) {
		if (StringUtils.isBlank(body)) {
			return success();
		}
		Document document = Document.parse(body);
		taskService.updateSyncProgress(MongoUtils.toObjectId(taskId), document);
		return success();
	}

    @Operation(summary = "任务运行记录")
    @GetMapping("/records/{id}")
    public ResponseMessage<Page<TaskRecordListVo>> records(@PathVariable(value = "id") String taskId,
                                                           @RequestParam(defaultValue = "1") Integer page,
                                                           @RequestParam(defaultValue = "20") Integer size) {
        return success(taskRecordService.queryRecords(new TaskRecordDto(taskId, page, size)));
    }

    @Operation(summary = "任务日志设置")
    @PutMapping("/logSetting/{taskId}")
    public ResponseMessage<Void> logSetting (@PathVariable("taskId") String taskId,
                                             @RequestBody LogSettingParam logSettingParam) {
        taskService.updateTaskLogSetting(taskId, logSettingParam, getLoginUser());
        return success();
    }

    @Operation(summary = "任务日志设置")
    @PostMapping("/logSetting/{level}/{taskId}")
    public ResponseMessage<Void> logSettingPost (@PathVariable("taskId") String taskId,
                                                 @PathVariable("level") String level) {
        LogSettingParam logSettingParam = new LogSettingParam();
        logSettingParam.setLevel(level);
        taskService.updateTaskLogSetting(taskId, logSettingParam, getLoginUser());
        return success();
    }

    @Operation(summary = "任务统计数据接口")
    @GetMapping("/stats")
    public ResponseMessage<TaskStatsDto> stats() {
        return success(taskService.stats(getLoginUser()));
    }


    @Operation(summary = "任务数据量统计")
    @GetMapping("/stats/transport")
    public ResponseMessage<DataFlowInsightStatisticsDto> statsTransport(@RequestParam("granularity") String granularity) {
        return success(taskService.statsTransport(getLoginUser()));
    }



    @PatchMapping("rename/{taskId}")
    public ResponseMessage<Void> rename(@PathVariable("taskId") String taskId, @RequestParam("newName") String newName) {
        taskService.rename(taskId, newName, getLoginUser());
        return success();
    }

    @PostMapping("/stopTaskByAgentId/{agentId}")
    public ResponseMessage<Void> stopTaskByAgentId(@PathVariable("agentId") String agentId) {
        taskService.stopTaskIfNeedByAgentId(agentId, getLoginUser());
        return success();
    }
}
