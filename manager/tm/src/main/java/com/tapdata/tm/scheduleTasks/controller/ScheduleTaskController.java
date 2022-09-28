package com.tapdata.tm.scheduleTasks.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.scheduleTasks.dto.ScheduleTasksDto;
import com.tapdata.tm.scheduleTasks.param.UpdateScheduleParam;
import com.tapdata.tm.scheduleTasks.service.ScheduleTasksService;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;


/**
 * @Date: 2021/12/16
 * @Description:
 */
@Tag(name = "ScheduleTask", description = "ScheduleTask相关接口")
@RestController
@RequestMapping("/api/ScheduleTasks")
public class ScheduleTaskController extends BaseController {

    @Autowired
    private ScheduleTasksService scheduleTaskService;

    /**
     * todo MONGODB_DROP_INDEX  在node.js中没有得到处理？？
     *
     * @param scheduleTask
     * @return
     */
    @Operation(summary = "创建一个生命周期")
    @PostMapping
    public ResponseMessage<ScheduleTasksDto> save(@RequestBody ScheduleTasksDto scheduleTask) {
        scheduleTask.setId(null);
        return success(scheduleTaskService.save(scheduleTask, getLoginUser()));

    }

    /**
     * Patch an existing model instance or insert a new one into the data source
     *
     * @param scheduleTask
     * @return
     */
    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
    @PatchMapping()
    public ResponseMessage<ScheduleTasksDto> update(@RequestBody ScheduleTasksDto scheduleTask) {
        return success(scheduleTaskService.save(scheduleTask, getLoginUser()));
    }


    /**
     * Find all instances of the model matched by filter from the data source
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<ScheduleTasksDto>> find(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(scheduleTaskService.find(filter, getLoginUser()));
    }

    /**
     * Replace an existing model instance or insert a new one into the data source
     *
     * @param scheduleTask
     * @return
     */
    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<ScheduleTasksDto> put(@RequestBody ScheduleTasksDto scheduleTask) {
        return success(scheduleTaskService.replaceOrInsert(scheduleTask, getLoginUser()));
    }


    /**
     * Check whether a model instance exists in the data source
     *
     * @return
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
    public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
        long count = scheduleTaskService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     * Patch attributes for a model instance and persist it into the data source
     *
     * @param scheduleTask
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<ScheduleTasksDto> updateById(@PathVariable("id") String id, @RequestBody ScheduleTasksDto scheduleTask) {
        scheduleTask.setId(MongoUtils.toObjectId(id));
        return success(scheduleTaskService.save(scheduleTask, getLoginUser()));
    }


    /**
     * Find a model instance by {{id}} from the data source
     *
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<ScheduleTasksDto> findById(@PathVariable("id") String id,
                                                      @RequestParam(value = "fields", required = false) String fieldsJson) {
        Field fields = parseField(fieldsJson);
        return success(scheduleTaskService.findById(MongoUtils.toObjectId(id), fields, getLoginUser()));
    }

    /**
     * Replace attributes for a model instance and persist it into the data source.
     *
     * @param scheduleTask
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PutMapping("{id}")
    public ResponseMessage<ScheduleTasksDto> replceById(@PathVariable("id") String id, @RequestBody ScheduleTasksDto scheduleTask) {
        return success(scheduleTaskService.replaceById(MongoUtils.toObjectId(id), scheduleTask, getLoginUser()));
    }

    /**
     * Replace attributes for a model instance and persist it into the data source.
     *
     * @param scheduleTask
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PostMapping("{id}/replace")
    public ResponseMessage<ScheduleTasksDto> replaceById2(@PathVariable("id") String id, @RequestBody ScheduleTasksDto scheduleTask) {
        return success(scheduleTaskService.replaceById(MongoUtils.toObjectId(id), scheduleTask, getLoginUser()));
    }


    /**
     * 目前没有用到
     */
    @Deprecated
    @Operation(summary = "删除一个所以")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        scheduleTaskService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
        return success();
    }

    /**
     * Check whether a model instance exists in the data source
     *
     * @param id
     * @return
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @GetMapping("{id}/exists")
    public ResponseMessage<HashMap<String, Boolean>> checkById1(@PathVariable("id") String id) {
        long count = scheduleTaskService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     * Count instances of the model matched by where from the data source
     *
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
        long count = scheduleTaskService.count(where, getLoginUser());
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }

    /**
     * Find first instance of the model matched by filter from the data source.
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<ScheduleTasksDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(scheduleTaskService.findOne(filter, getLoginUser()));
    }

    /**
     * engine 调用，更新 scheduleTask 表
     */
    @Operation(summary = "engine 调用，更新 scheduleTask 表")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody UpdateScheduleParam updateScheduleParam) {
        Where where = parseWhere(whereJson);
        ScheduleTasksDto scheduleTasksDto = new ScheduleTasksDto();
        Map setMap = updateScheduleParam.getSet();
        HashMap<String, Long> countValue = new HashMap<>();
        if (null != setMap) {
            String status = (String) setMap.get("status");
            String agentId = (String) setMap.get("agent_id");
            if (Objects.isNull(agentId)) {
                countValue.put("count", 0L);
                return success(countValue);
            }
            Long ping_time = (Long) setMap.get("ping_time");
            scheduleTasksDto.setLast_updated(new Date());
            scheduleTasksDto.setStatus(status);
            scheduleTasksDto.setAgent_id(agentId);
            scheduleTasksDto.setPing_time(ping_time);
        }

        long count = scheduleTaskService.updateByWhere(where, scheduleTasksDto, getLoginUser());
        countValue.put("count", count);
        return success(countValue);
    }

    /**
     * Update an existing model instance or insert a new one into the data source based on the where criteria.
     *
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update an existing model instance or insert a new one into the data source based on the where criteria.")
    @PostMapping("upsertWithWhere")
    public ResponseMessage<ScheduleTasksDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody ScheduleTasksDto scheduleTask) {
        Where where = parseWhere(whereJson);
        return success(scheduleTaskService.upsertByWhere(where, scheduleTask, getLoginUser()));
    }

}