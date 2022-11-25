package com.tapdata.tm.worker.controller;

import com.google.gson.reflect.TypeToken;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.manager.common.utils.StringUtils;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.service.UserLogService;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.utils.MongoUtils;
import com.tapdata.tm.worker.dto.CheckTaskUsedAgentDto;
import com.tapdata.tm.worker.dto.WorkerDto;
import com.tapdata.tm.worker.dto.WorkerProcessInfoDto;
import com.tapdata.tm.worker.service.WorkerService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2021/9/11 下午5:31
 * @description
 */
@Slf4j
@RestController
@RequestMapping("/api/Workers")
public class WorkerController extends BaseController {
    @PostMapping("/test")
    public String postTest() {
        return "post success";
    }

    @GetMapping("/test")
    public String getTest() {
        return "get success";
    }

    private final UserLogService userLogService;
    private SettingsService settingsService;
    private WorkerService workerService;

    public WorkerController(WorkerService workerService, UserLogService userLogService, SettingsService settingsService) {
        this.workerService = workerService;
        this.userLogService = userLogService;
        this.settingsService = settingsService;
    }

    /**
     * Create a new instance of the model and persist it into the data source
     * @param worker
     * @return
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<WorkerDto> save(@RequestBody WorkerDto worker) {
        worker.setId(null);
        return success(workerService.save(worker, getLoginUser()));
    }

    /**
     * Create a new instance of the model and persist it into the data source
     * @param worker
     * @return
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping("/health")
    public ResponseMessage<WorkerDto> health(@RequestBody WorkerDto worker) {
        // flow-engine will set pingTime to 1 when users ask to stop the flow-engine in DFS， so here we
        // keep the pingTime value 1
        if (worker.getPingTime() == null || worker.getPingTime() != 1) {
            worker.setPingTime(new Date().getTime());
        }
        return success(workerService.health(worker, getLoginUser()));
    }

    /**
     *  Patch an existing model instance or insert a new one into the data source
     * @param worker
     * @return
     */
    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
    @PatchMapping()
    public ResponseMessage<WorkerDto> update(@RequestBody WorkerDto worker) {
        return success(workerService.save(worker, getLoginUser()));
    }


    /**
     * Find all instances of the model matched by filter from the data source
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<WorkerDto>> find(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        Where where=filter.getWhere();
        Map<String,Object> pingTime= (Map) where.get("ping_time");
        //todo 前端参数不好，这里待优化
        if (null!=pingTime){
            Map pintTimeQueryMap=new HashMap();
            if (null!=pingTime.get("gte")&&("$serverDate").equals(pingTime.get("gte"))&&(null!=pingTime.get("gte_offset"))){
                Number offset= (Number) pingTime.get("gte_offset");
                pintTimeQueryMap.put("$gte",new Date().getTime()-offset.longValue());
                where.put("ping_time",pintTimeQueryMap);
            }
        }
        return success(workerService.find(filter, getLoginUser()));
    }


    /**
     * api发布，获取api-worker的状态
     */
    @Operation(summary = "api发布，获取api-worker的状态")
    @GetMapping("findApiWorkerStatus")
    public ResponseMessage findApiWorkerStatus() {

        return success(workerService.findApiWorkerStatus(getLoginUser()));
    }


    /**
     *  Replace an existing model instance or insert a new one into the data source
     * @param worker
     * @return
     */
    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<WorkerDto> put(@RequestBody WorkerDto worker) {
        return success(workerService.replaceOrInsert(worker, getLoginUser()));
    }


    /**
     * Check whether a model instance exists in the data source
     * @return
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
    public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
        long count = workerService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     *  Patch attributes for a model instance and persist it into the data source
     * @param worker
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<WorkerDto> updateById(@PathVariable("id") String id, @RequestBody WorkerDto worker) {
        worker.setId(MongoUtils.toObjectId(id));
        return success(workerService.save(worker, getLoginUser()));
    }


    /**
     * Find a model instance by {{id}} from the data source
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<WorkerDto> findById(@PathVariable("id") String id,
                                               @RequestParam("fields") String fieldsJson) {
        Field fields = parseField(fieldsJson);
        return success(workerService.findById(MongoUtils.toObjectId(id),  fields, getLoginUser()));
    }

    /**
     *  Replace attributes for a model instance and persist it into the data source.
     * @param worker
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PutMapping("{id}")
    public ResponseMessage<WorkerDto> replceById(@PathVariable("id") String id, @RequestBody WorkerDto worker) {
        return success(workerService.replaceById(MongoUtils.toObjectId(id), worker, getLoginUser()));
    }

    /**
     *  Replace attributes for a model instance and persist it into the data source.
     * @param worker
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PostMapping("{id}/replace")
    public ResponseMessage<WorkerDto> replaceById2(@PathVariable("id") String id, @RequestBody WorkerDto worker) {
        return success(workerService.replaceById(MongoUtils.toObjectId(id), worker, getLoginUser()));
    }



    /**
     * Delete a model instance by {{id}} from the data source
     * @param id
     * @return
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        workerService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
        return success();
    }

    /**
     *  Check whether a model instance exists in the data source
     * @param id
     * @return
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @GetMapping("{id}/exists")
    public ResponseMessage<HashMap<String, Boolean>> checkById1(@PathVariable("id") String id) {
        long count = workerService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     *  Count instances of the model matched by where from the data source
     * @param whereJson
     * @return
     */
    @Operation(summary = "Count instances of the model matched by where from the data source")
    @GetMapping("count")
    public ResponseMessage<HashMap<String, Long>> count(@RequestParam("where") String whereJson) {
        Where where = parseWhere(whereJson);
        if (where == null) {
            where = new Where();
        }
        long count = workerService.count(where, getLoginUser());
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }

    /**
     *  Find first instance of the model matched by filter from the data source.
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<WorkerDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(workerService.findOne(filter, getLoginUser()));
    }

    /**
     *  Update instances of the model matched by {{where}} from the data source.
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody String reqBody) {

        UserDetail userDetail = getLoginUser();

        Where where = parseWhere(whereJson);
        Document update = Document.parse(reqBody);
        if (update.containsKey("$set") && update.get("$set") instanceof Document) {
            if (!((Document)update.get("$set")).containsKey("ping_time")){
                ((Document)update.get("$set")).put("ping_time", new Date().getTime());
            }
        }

        com.tapdata.tm.userLog.constant.Operation operation = null;

        if (update.containsKey("isDeleted") && update.getBoolean("isDeleted")) {
            update.put("ping_time", 0L);
            operation = com.tapdata.tm.userLog.constant.Operation.DELETE;
        }else if (update.containsKey("stopping") && update.getBoolean("stopping")){
            update.put("ping_time", System.currentTimeMillis() - 1000 * 60 * 5);
            operation = com.tapdata.tm.userLog.constant.Operation.STOP;
        }
        boolean isTcmRequest = update.containsKey("isTCM") && update.getBoolean("isTCM");
        if (isTcmRequest && operation != null) {
            try {
                Filter filter = new Filter();
                filter.setWhere(where);

                WorkerDto worker = workerService.findOne(filter, userDetail);
                if (worker != null && worker.getTcmInfo() != null) {
                    userLogService.addUserLog(
                            Modular.AGENT, operation,
                            userDetail, worker.getTcmInfo().getAgentName());
                }
            } catch (Exception e) {
                // Ignore record agent operation log error
                log.error("Record update worker operation fail", e);
            }
        }

        if (!update.containsKey("$set") && !update.containsKey("$setOnInsert") && !update.containsKey("$unset")) {
            Document _body = new Document();
            _body.put("$set", update);
            update = _body;
        }

        long count = workerService.updateByWhere(where, update, userDetail);
        /*if (reqBody.contains("\"$set\"") || reqBody.contains("\"$setOnInsert\"") || reqBody.contains("\"$unset\"")) {
            UpdateDto<WorkerDto> updateDto = JsonUtil.parseJsonUseJackson(reqBody, new TypeReference<UpdateDto<WorkerDto>>(){});

            if (updateDto != null && updateDto.getSet() != null && updateDto.getSet().getHostname() != null) {
                updateDto.getSet().setPingTime(new Date().getTime());
            }
            count = workerService.updateByWhere(where, updateDto, getLoginUser());
        } else {
            WorkerDto workerDto = JsonUtil.parseJsonUseJackson(reqBody, WorkerDto.class);
            count = workerService.updateByWhere(where, workerDto, getLoginUser());
        }*/
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }

    /**
     * 进程调用该方法，上报各个进程的数据，ping_time 之类 的
     *
     */
    @Operation(summary = "进程调用该方法，上报各个进程的数据，ping_time 之类 的")
    @PostMapping("upsertWithWhere")
    public ResponseMessage<WorkerDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody WorkerDto worker) {
        Where where = parseWhere(whereJson);
        if ("api-server".equals(worker.getWorkerType())){
            Long nowTimeStamp=new Date().getTime();
            worker.setPingTime(nowTimeStamp);
        }
        return success(workerService.upsertByWhere(where, worker, getLoginUser()));
    }

    @Operation(summary = "创建实例接口")
    @PostMapping("/createWorker")
    public ResponseMessage<WorkerDto> createWorker(@RequestBody WorkerDto workerDto) {
        return success(workerService.createWorker(workerDto, getLoginUser()));
    }

    @Operation(summary = "查询实例状态")
    @GetMapping("/getProcessInfo")
    public ResponseMessage<Map<String, WorkerProcessInfoDto>> getProcessInfo(@RequestParam("process_id") String processId) {
        if (processId == null) {
            return success();
        }
        List<String> ids = JsonUtil.parseJson(processId, new TypeToken<List<String>>(){}.getType());
        return success(workerService.getProcessInfo(ids));
    }


    @Operation(summary = "获取可用实例信息")
    @GetMapping("/availableAgent")
    public ResponseMessage<Map<String, List>> availableAgent() {
	    List<WorkerDto> workerDtos = workerService.convertToDto(workerService.findAvailableAgent(getLoginUser()), WorkerDto.class);
	    return success(new HashMap<String, List>(){{put("result", workerDtos);}});
    }

    /**
     * 校验任务所使用的agent是否可以强制停止, agent如果离线状态，则提醒用户会存在问题，入agent在线，或者已删除，则可以使用。
     * @param taskId
     * @return key:status  value: online在线 offline离线 deleted已删除
     */
    @Operation(summary = "校验任务所使用的agent是否可以强制停止")
    @GetMapping("/available/taskUsedAgent")
    public ResponseMessage<CheckTaskUsedAgentDto> checkTaskUsedAgent(@RequestParam("taskId") String taskId) {
        String status = workerService.checkTaskUsedAgent(taskId, getLoginUser());

        CheckTaskUsedAgentDto dto = new CheckTaskUsedAgentDto();
        dto.setStatus(status);
        Object buildProfile = settingsService.getByCategoryAndKey("System", "buildProfile");
        final boolean isCloud = buildProfile.equals("CLOUD") || buildProfile.equals("DRS") || buildProfile.equals("DFS");

        dto.setCloudVersion(isCloud);
        return success(dto);
    }

}
