//package com.tapdata.tm.task.controller;
//
//import com.tapdata.tm.base.controller.BaseController;
//import com.tapdata.tm.base.dto.*;
//import com.tapdata.tm.commons.task.dto.SubTaskDto;
//import com.tapdata.tm.commons.task.dto.TaskDto;
//import com.tapdata.tm.message.constant.Level;
//import com.tapdata.tm.message.constant.MsgTypeEnum;
//import com.tapdata.tm.message.service.MessageService;
//import com.tapdata.tm.task.bean.*;
//import com.tapdata.tm.task.entity.TaskEntity;
//import com.tapdata.tm.task.repository.TaskRepository;
//import com.tapdata.tm.task.service.SnapshotEdgeProgressService;
//import com.tapdata.tm.task.service.SubTaskService;
//import com.tapdata.tm.task.vo.SubTaskDetailVo;
//import com.tapdata.tm.utils.MongoUtils;
//import io.swagger.v3.oas.annotations.Operation;
//import io.swagger.v3.oas.annotations.Parameter;
//import io.swagger.v3.oas.annotations.enums.ParameterIn;
//import io.swagger.v3.oas.annotations.tags.Tag;
//import lombok.Setter;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.lang3.StringUtils;
//import org.bson.Document;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.web.bind.annotation.*;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//
///**
// * @Date: 2021/11/03
// * @Description:
// */
//@Tag(name = "子任务", description = "子任务相关接口")
//@RestController
//@Slf4j
//@RequestMapping("/api/SubTask")
//@Setter(onMethod_ = {@Autowired})
//public class SubTaskController extends BaseController {
//
//    private SubTaskService subTaskService;
//    private TaskRepository taskRepository;
//    private MessageService messageService;
//    private SnapshotEdgeProgressService snapshotEdgeProgressService;
//
//    /**
//     * Create a new instance of the model and persist it into the data source
//     * @param subTask
//     * @return
//     */
//    @Operation(summary = "Create a new instance of the model and persist it into the data source")
//    @PostMapping
//    public ResponseMessage<SubTaskDto> save(@RequestBody SubTaskDto subTask) {
//        subTask.setId(null);
//        return success(subTaskService.save(subTask, getLoginUser()));
//    }
//
//    /**
//     *  Patch an existing model instance or insert a new one into the data source
//     * @param subTask
//     * @return
//     */
//    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
//    @PatchMapping()
//    public ResponseMessage<SubTaskDto> update(@RequestBody SubTaskDto subTask) {
//        return success(subTaskService.save(subTask, getLoginUser()));
//    }
//
//
//    /**
//     * Find all instances of the model matched by filter from the data source
//     * @param filterJson
//     * @return
//     */
//    @Operation(summary = "Find all instances of the model matched by filter from the data source")
//    @GetMapping
//    public ResponseMessage<Page<SubTaskDto>> find(
//            @Parameter(in = ParameterIn.QUERY,
//                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
//            )
//            @RequestParam(value = "filter", required = false) String filterJson) {
//        Filter filter = parseFilter(filterJson);
//        if (filter == null) {
//            filter = new Filter();
//        }
//        return success(subTaskService.find(filter, getLoginUser()));
//    }
//
//    /**
//     *  Replace an existing model instance or insert a new one into the data source
//     * @param subTask
//     * @return
//     */
//    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
//    @PutMapping
//    public ResponseMessage<SubTaskDto> put(@RequestBody SubTaskDto subTask) {
//        return success(subTaskService.replaceOrInsert(subTask, getLoginUser()));
//    }
//
//
//    /**
//     * Check whether a model instance exists in the data source
//     * @return
//     */
//    @Operation(summary = "Check whether a model instance exists in the data source")
//    @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
//    public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
//        long count = subTaskService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
//        HashMap<String, Boolean> existsValue = new HashMap<>();
//        existsValue.put("exists", count > 0);
//        return success(existsValue);
//    }
//
//    /**
//     *  Patch attributes for a model instance and persist it into the data source
//     * @param subTask
//     * @return
//     */
//    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
//    @PatchMapping("{id}")
//    public ResponseMessage<SubTaskDto> updateById(@PathVariable("id") String id, @RequestBody SubTaskDto subTask) {
//        subTask.setId(MongoUtils.toObjectId(id));
//        return success(subTaskService.save(subTask, getLoginUser()));
//    }
//
//
//    /**
//     * Find a model instance by {{id}} from the data source
//     * @param fieldsJson
//     * @return
//     */
//    @Operation(summary = "Find a model instance by {{id}} from the data source")
//    @GetMapping("{id}")
//    public ResponseMessage<SubTaskDetailVo> findById(@PathVariable("id") String id,
//                                                     @RequestParam(value = "fields", required = false) String fieldsJson) {
//        Field fields = parseField(fieldsJson);
//        return success(subTaskService.findById(MongoUtils.toObjectId(id),  fields, getLoginUser()));
//    }
//
//    /**
//     *  Replace attributes for a model instance and persist it into the data source.
//     * @param subTask
//     * @return
//     */
//    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
//    @PutMapping("{id}")
//    public ResponseMessage<SubTaskDto> replceById(@PathVariable("id") String id, @RequestBody SubTaskDto subTask) {
//        return success(subTaskService.replaceById(MongoUtils.toObjectId(id), subTask, getLoginUser()));
//    }
//
//    /**
//     *  Replace attributes for a model instance and persist it into the data source.
//     * @param subTask
//     * @return
//     */
//    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
//    @PostMapping("{id}/replace")
//    public ResponseMessage<SubTaskDto> replaceById2(@PathVariable("id") String id, @RequestBody SubTaskDto subTask) {
//        return success(subTaskService.replaceById(MongoUtils.toObjectId(id), subTask, getLoginUser()));
//    }
//
//
//
//    /**
//     * Delete a model instance by {{id}} from the data source
//     * @param id
//     * @return
//     */
//    @Operation(summary = "Delete a model instance by {{id}} from the data source")
//    @DeleteMapping("{id}")
//    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
//        subTaskService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
//        return success();
//    }
//
//    /**
//     *  Check whether a model instance exists in the data source
//     * @param id
//     * @return
//     */
//    @Operation(summary = "Check whether a model instance exists in the data source")
//    @GetMapping("{id}/exists")
//    public ResponseMessage<HashMap<String, Boolean>> checkById1(@PathVariable("id") String id) {
//        long count = subTaskService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
//        HashMap<String, Boolean> existsValue = new HashMap<>();
//        existsValue.put("exists", count > 0);
//        return success(existsValue);
//    }
//
//    /**
//     *  Count instances of the model matched by where from the data source
//     * @param whereJson
//     * @return
//     */
//    @Operation(summary = "Count instances of the model matched by where from the data source")
//    @GetMapping("count")
//    public ResponseMessage<HashMap<String, Long>> count(@RequestParam("where") String whereJson) {
//        Where where = parseWhere(whereJson);
//        if (where == null) {
//            where = new Where();
//        }
//        long count = subTaskService.count(where, getLoginUser());
//        HashMap<String, Long> countValue = new HashMap<>();
//        countValue.put("count", count);
//        return success(countValue);
//    }
//
//    /**
//     *  Find first instance of the model matched by filter from the data source.
//     * @param filterJson
//     * @return
//     */
//    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
//    @GetMapping("findOne")
//    public ResponseMessage<SubTaskDto> findOne(
//            @Parameter(in = ParameterIn.QUERY,
//                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
//            )
//            @RequestParam(value = "filter", required = false) String filterJson) {
//        Filter filter = parseFilter(filterJson);
//        if (filter == null) {
//            filter = new Filter();
//        }
//        return success(subTaskService.findOne(filter, getLoginUser()));
//    }
//
//    /**
//     *  Update instances of the model matched by {{where}} from the data source.
//     * @param whereJson
//     * @return
//     */
//    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
//    @PostMapping("update")
//    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody String reqBody) {
//        log.info("subTask updateByWhere, whereJson:{},reqBody:{}",whereJson,reqBody);
//        Where where = parseWhere(whereJson);
//        Document update = Document.parse(reqBody);
//        if (!update.containsKey("$set") && !update.containsKey("$setOnInsert") && !update.containsKey("$unset")) {
//            Document _body = new Document();
//            _body.put("$set", update);
//            update = _body;
//        }
//
//        long count = subTaskService.updateByWhere(where, update, getLoginUser(), reqBody);
//        HashMap<String, Long> countValue = new HashMap<>();
//        countValue.put("count", count);
//
//        //更新完任务，addMessage
//        try {
//            Object id = where.get("_id");
//            String status = update.getString("status");
//            if (StringUtils.isNotEmpty(status) && null != id) {
//                String idString = id.toString();
//                SubTaskDto subTaskDto = subTaskService.findById(MongoUtils.toObjectId(idString));
//                TaskEntity task=taskRepository.findById(subTaskDto.getParentId().toString()).get();
//                String name = task.getName();
//                log.info("subtask addMessage ,id :{},name :{},status:{}  ", id, name, status);
//                if ("error".equals(status)) {
//                    messageService.addMigration(name, idString, MsgTypeEnum.STOPPED_BY_ERROR, Level.ERROR, getLoginUser());
//                } else if ("running".equals(status)) {
//                    messageService.addMigration(name, idString, MsgTypeEnum.CONNECTED, Level.INFO, getLoginUser());
//                }
//            }
//        } catch (Exception e) {
//            log.error("任务状态添加 message 异常",e);
//        }
//        return success(countValue);
//    }
//
//    /**
//     *  Update an existing model instance or insert a new one into the data source based on the where criteria.
//     * @param whereJson
//     * @return
//     */
//    @Operation(summary = "Update an existing model instance or insert a new one into the data source based on the where criteria.")
//    @PostMapping("upsertWithWhere")
//    public ResponseMessage<SubTaskDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody SubTaskDto subTask) {
//        Where where = parseWhere(whereJson);
//        return success(subTaskService.upsertByWhere(where, subTask, getLoginUser()));
//    }
//
//    @Operation(summary = "根据主任务id查询子任务接口")
//    @GetMapping("byTaskId/{taskId}")
//    public ResponseMessage<List<SubTaskDto>> findByTaskId(@PathVariable("taskId") String taskId
//            , @RequestParam(value = "fields", required = false) String... fields) {
//        return success(subTaskService.findByTaskId(MongoUtils.toObjectId(taskId), getLoginUser(), fields));
//
//    }
//
//
//    @Operation(summary = "运行时信息接口")
//    @GetMapping("runtimeInfo/{taskId}")
//    public ResponseMessage<RunTimeInfo> runtimeInfo(@PathVariable("taskId") String taskId
//            , @RequestParam(value = "endTime", required = false) Long endTime) {
//        return success(subTaskService.runtimeInfo(MongoUtils.toObjectId(taskId), endTime, getLoginUser()));
//
//    }
//
//    /**
//     * 收到子任务已经运行的消息
//     * @param id
//     */
//    @Operation(summary = "启动子任务接口")
//    @PostMapping("start/{id}")
//    public ResponseMessage<Void> start(@PathVariable("id") String id) {
//        subTaskService.start(MongoUtils.toObjectId(id), getLoginUser());
//        return success();
//    }
//
//    /**
//     * 收到子任务已经运行的消息
//     * @param id
//     */
//    @Operation(summary = "子任务已经成功运行回调接口")
//    @PostMapping("running/{id}")
//    public ResponseMessage<TaskOpResp> running(@PathVariable("id") String id) {
//        log.info("subTask running status report by http, id = {}", id);
//        String successId = subTaskService.running(MongoUtils.toObjectId(id), getLoginUser());
//        TaskOpResp taskOpResp = new TaskOpResp();
//        if (StringUtils.isBlank(successId)) {
//            taskOpResp = new TaskOpResp(successId);
//        }
//
//        return success(taskOpResp);
//    }
//
//    /**
//     * 收到子任务运行失败的消息
//     * @param id
//     */
//    @Operation(summary = "子任务运行失败回调接口")
//    @PostMapping("runError/{id}")
//    public ResponseMessage<TaskOpResp> runError(@PathVariable("id") String id, @RequestParam(value = "errMsg", required = false) String errMsg,
//                                          @RequestParam(value = "errStack", required = false) String errStack) {
//        String successId = subTaskService.runError(MongoUtils.toObjectId(id), getLoginUser(), errMsg, errStack);
//        TaskOpResp taskOpResp = new TaskOpResp();
//        if (StringUtils.isBlank(successId)) {
//            taskOpResp = new TaskOpResp(successId);
//        }
//
//        return success(taskOpResp);
//    }
//
//    /**
//     * 收到子任务运行完成的消息
//     * @param id
//     */
//    @Operation(summary = "子任务执行完成回调接口")
//    @PostMapping("complete/{id}")
//    public ResponseMessage<TaskOpResp> complete(@PathVariable("id") String id) {
//        String successId = subTaskService.complete(MongoUtils.toObjectId(id), getLoginUser());
//        TaskOpResp taskOpResp = new TaskOpResp();
//        if (StringUtils.isBlank(successId)) {
//            taskOpResp = new TaskOpResp(successId);
//        }
//
//        return success(taskOpResp);
//    }
//
//    /**
//     * 收到子任务已经停止的消息
//     * @param id
//     */
//    @Operation(summary = "停止成功回调接口")
//    @PostMapping("stopped/{id}")
//    public ResponseMessage<TaskOpResp> stopped(@PathVariable("id") String id) {
//        String successId = subTaskService.stopped(MongoUtils.toObjectId(id), getLoginUser());
//        TaskOpResp taskOpResp = new TaskOpResp();
//        if (StringUtils.isBlank(successId)) {
//            taskOpResp = new TaskOpResp(successId);
//        }
//
//        return success(taskOpResp);
//    }
//
//    /**
//     * 暂停子任务
//     * @param id
//     */
//    @Operation(summary = "暂停子任务接口")
//    //@PostMapping("pause/{id}")
//    public ResponseMessage<Void> pause(@PathVariable("id") String id, @RequestParam(value = "force", defaultValue = "false") Boolean force) {
//        subTaskService.pause(MongoUtils.toObjectId(id), getLoginUser(), force);
//        return success();
//    }
//    /**
//     * 停止子任务接口
//     * @param id
//     */
//    @Operation(summary = "停止子任务接口")
//    @PostMapping("stop/{id}")
//    public ResponseMessage<Void> stop(@PathVariable("id") String id, @RequestParam(value = "force", defaultValue = "false") Boolean force) {
//        subTaskService.pause(MongoUtils.toObjectId(id), getLoginUser(), force);
//        return success();
//    }
//
//
//    /**
//     * 重置子任务接口
//     * @param id
//     */
//    @Operation(summary = "重置任务接口")
//    @PostMapping("renew/{id}")
//    public ResponseMessage<Void> renew(@PathVariable("id") String id, @RequestParam(value = "force", defaultValue = "false") Boolean force) {
//        subTaskService.renew(MongoUtils.toObjectId(id), getLoginUser());
//        return success();
//    }
//
//    /**
//     * 更新子任务的node跟任务中的node
//     * @param subTaskId 子任务id
//     * @param nodeId nodeId
//     * @param reqBody 更新的参数
//     * @return
//     */
//    @Operation(summary = "更新子任务的node跟任务中的node")
//    @PostMapping("node/{subTaskId}/{nodeId}")
//    public ResponseMessage<Void> updateNode(@PathVariable("subTaskId") String subTaskId, @PathVariable("nodeId") String nodeId, @RequestBody String reqBody) {
//        Document update = Document.parse(reqBody);
//        if (!update.containsKey("$set") && !update.containsKey("$setOnInsert") && !update.containsKey("$unset")) {
//            Document _body = new Document();
//            _body.put("$set", update);
//            update = _body;
//        }
//        subTaskService.updateNode(MongoUtils.toObjectId(subTaskId), nodeId, update, getLoginUser());
//        return success();
//    }
//
//	/**
//	 * 更新子任务断点信息
//	 *
//	 * @param subTaskId 子任务id
//	 * @param body      断点信息
//	 * @return
//	 */
//	@Operation(summary = "更新子任务断点信息")
//	@PostMapping("syncProgress/{subTaskId}")
//	public ResponseMessage<Void> updateSyncProgress(@PathVariable("subTaskId") String subTaskId, @RequestBody String body) {
//		if (StringUtils.isBlank(body)) {
//			return success();
//		}
//		Document document = Document.parse(body);
//		subTaskService.updateSyncProgress(MongoUtils.toObjectId(subTaskId), document);
//		return success();
//	}
//
//
//    @GetMapping("view/sync/overview/{subTaskId}")
//    public ResponseMessage<FullSyncVO> syncOverview(@PathVariable("subTaskId") String subTaskId) {
//        return success(snapshotEdgeProgressService.syncOverview(subTaskId));
//    }
//
//    @GetMapping("view/sync/table/{subTaskId}")
//    public ResponseMessage<Page<TableStatus>> syncTableView(@PathVariable("subTaskId") String subTaskId,
//                                                            @RequestParam(value = "skip", required = false, defaultValue = "0") Long skip,
//                                                            @RequestParam(value = "limit", required = false, defaultValue = "20") Integer limit) {
//        return success(snapshotEdgeProgressService.syncTableView(subTaskId, skip, limit));
//    }
//
//    @GetMapping("view/increase/{subTaskId}")
//    public ResponseMessage<List<IncreaseSyncVO>> increaseView(@PathVariable("subTaskId") String subTaskId,
//                                                              @RequestParam(value = "skip", required = false, defaultValue = "0") Long skip,
//                                                              @RequestParam(value = "limit", required = false, defaultValue = "20") Integer limit) {
//        return success(subTaskService.increaseView(subTaskId));
//    }
//
//    @PostMapping("increase/clear/{subTaskId}")
//    public ResponseMessage<List<IncreaseSyncVO>> increaseClear(@PathVariable("subTaskId") String subTaskId,
//                                                               @RequestParam("srcNode") String srcNode,
//                                                               @RequestParam("tgtNode") String tgtNode) {
//        subTaskService.increaseClear(MongoUtils.toObjectId(subTaskId), srcNode, tgtNode, getLoginUser());
//        return success();
//    }
//
//    @PostMapping("increase/backtracking/{subTaskId}")
//    public ResponseMessage<List<IncreaseSyncVO>> increaseBacktracking(@PathVariable("subTaskId") String subTaskId,
//                                                                      @RequestParam("srcNode") String srcNode,
//                                                                      @RequestParam("tgtNode") String tgtNode,
//                                                                      @RequestBody TaskDto.SyncPoint point) {
//        subTaskService.increaseBacktracking(MongoUtils.toObjectId(subTaskId), srcNode, tgtNode, point, getLoginUser());
//        return success();
//
//    }
//
//    @GetMapping("byCacheName/{cacheName}")
//    public ResponseMessage<SubTaskDto> findByCacheName(@PathVariable("cacheName") String cacheName) {
//        return success(subTaskService.findByCacheName(cacheName, getLoginUser()));
//    }
//
//
//    @PostMapping("dag")
//    public ResponseMessage<Void> updateDag(@RequestBody SubTaskDto subTaskDto) {
//        subTaskService.updateDag(subTaskDto, getLoginUser());
//        return success();
//    }
//
//
//    @GetMapping("history")
//    public ResponseMessage<SubTaskDto> queryHistory(@RequestParam("id") String id, @RequestParam("time") Long time) {
//        SubTaskDto subTaskDto  = subTaskService.findByVersionTime(id, time);
//        return success(subTaskDto);
//    }
//
//    @DeleteMapping("history")
//    public ResponseMessage<SubTaskDto> cleanHistory(@RequestParam("id") String id, @RequestParam("time") Long time) {
//        subTaskService.clean(id, time);
//        return success();
//    }
//
//
//}
