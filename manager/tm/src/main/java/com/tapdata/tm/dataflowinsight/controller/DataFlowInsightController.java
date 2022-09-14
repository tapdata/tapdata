package com.tapdata.tm.dataflowinsight.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.behavior.service.BehaviorService;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.dataflowinsight.dto.DataFlowInsightDto;
import com.tapdata.tm.dataflowinsight.dto.RuntimeMonitorReq;
import com.tapdata.tm.dataflowinsight.dto.RuntimeMonitorResp;
import com.tapdata.tm.dataflowinsight.service.DataFlowInsightService;
import com.tapdata.tm.task.service.TaskService;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;


/**
 * @Date: 2021/09/13
 * @Description:
 */
@Tag(name = "DataFlowInsight", description = "DataFlowInsight相关接口")
@RestController
@RequestMapping("/api/DataFlowInsights")
public class DataFlowInsightController extends BaseController {

    @Autowired
    private DataFlowInsightService dataFlowInsightService;

    @Autowired
    private BehaviorService behaviorService;
    @Autowired
    private TaskService taskService;

    /**
     * Create a new instance of the model and persist it into the data source
     * @param dataFlowInsight
     * @return
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<DataFlowInsightDto> save(@RequestBody DataFlowInsightDto dataFlowInsight) {
        dataFlowInsight.setId(null);

        UserDetail userDetail = getLoginUser();
        behaviorService.trace(dataFlowInsight, userDetail);

        return success(dataFlowInsightService.save(dataFlowInsight, userDetail));
    }

    /**
     *  Patch an existing model instance or insert a new one into the data source
     * @param dataFlowInsight
     * @return
     */
    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
    @PatchMapping()
    public ResponseMessage<DataFlowInsightDto> update(@RequestBody DataFlowInsightDto dataFlowInsight) {
        UserDetail userDetail = getLoginUser();
        behaviorService.trace(dataFlowInsight, userDetail);
        return success(dataFlowInsightService.save(dataFlowInsight, userDetail));
    }


    /**
     * Find all instances of the model matched by filter from the data source
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<DataFlowInsightDto>> find(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(dataFlowInsightService.find(filter, getLoginUser()));
    }

    /**
     *  Replace an existing model instance or insert a new one into the data source
     * @param dataFlowInsight
     * @return
     */
    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<DataFlowInsightDto> put(@RequestBody DataFlowInsightDto dataFlowInsight) {
        return success(dataFlowInsightService.replaceOrInsert(dataFlowInsight, getLoginUser()));
    }


    /**
     * Check whether a model instance exists in the data source
     * @return
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
    public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
        long count = dataFlowInsightService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     *  Patch attributes for a model instance and persist it into the data source
     * @param dataFlowInsight
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<DataFlowInsightDto> updateById(@PathVariable("id") String id, @RequestBody DataFlowInsightDto dataFlowInsight) {
        dataFlowInsight.setId(MongoUtils.toObjectId(id));
        return success(dataFlowInsightService.save(dataFlowInsight, getLoginUser()));
    }


    /**
     * Find a model instance by {{id}} from the data source
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<DataFlowInsightDto> findById(@PathVariable("id") String id,
            @RequestParam("fields") String fieldsJson) {
        Field fields = parseField(fieldsJson);
        return success(dataFlowInsightService.findById(MongoUtils.toObjectId(id),  fields, getLoginUser()));
    }

    /**
     *  Replace attributes for a model instance and persist it into the data source.
     * @param dataFlowInsight
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PutMapping("{id}")
    public ResponseMessage<DataFlowInsightDto> replceById(@PathVariable("id") String id, @RequestBody DataFlowInsightDto dataFlowInsight) {
        return success(dataFlowInsightService.replaceById(MongoUtils.toObjectId(id), dataFlowInsight, getLoginUser()));
    }

    /**
     *  Replace attributes for a model instance and persist it into the data source.
     * @param dataFlowInsight
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PostMapping("{id}/replace")
    public ResponseMessage<DataFlowInsightDto> replaceById2(@PathVariable("id") String id, @RequestBody DataFlowInsightDto dataFlowInsight) {
        return success(dataFlowInsightService.replaceById(MongoUtils.toObjectId(id), dataFlowInsight, getLoginUser()));
    }



    /**
     * Delete a model instance by {{id}} from the data source
     * @param id
     * @return
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        dataFlowInsightService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
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
        long count = dataFlowInsightService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
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
        long count = dataFlowInsightService.count(where, getLoginUser());
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
    public ResponseMessage<DataFlowInsightDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(dataFlowInsightService.findOne(filter, getLoginUser()));
    }

    /**
     *  Update instances of the model matched by {{where}} from the data source.
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody String reqBody) {

        Where where = parseWhere(whereJson);
        UserDetail user = getLoginUser();
        Document body = Document.parse(reqBody);
        if (!body.containsKey("$set") && !body.containsKey("$setOnInsert") && !body.containsKey("$unset")) {
            Document _body = new Document();
            _body.put("$set", body);
            body = _body;
        }
        long count = dataFlowInsightService.updateByWhere(where, body, user);

        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }

    /**
     *  Update an existing model instance or insert a new one into the data source based on the where criteria.
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update an existing model instance or insert a new one into the data source based on the where criteria.")
    @PostMapping("upsertWithWhere")
    public ResponseMessage<DataFlowInsightDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody DataFlowInsightDto dataFlowInsight) {
        Where where = parseWhere(whereJson);
        UserDetail userDetail = getLoginUser();
        behaviorService.trace(dataFlowInsight, userDetail);
        return success(dataFlowInsightService.upsertByWhere(where, dataFlowInsight, userDetail));
    }

    @Operation(summary = "DataFlowInsight runtimeMonitor")
    @GetMapping("/runtimeMonitor")
    public ResponseMessage<RuntimeMonitorResp> runtimeMonitor(@Validated RuntimeMonitorReq runtimeMonitorReq) {
        return success(dataFlowInsightService.runtimeMonitor(runtimeMonitorReq, getLoginUser()));
    }

    @Operation(summary = "DataFlowInsight statistics")
    @GetMapping("/statistics")
    public ResponseMessage<Object> statistics(@RequestParam("granularity") String granularity) {
        return success(taskService.statsTransport(getLoginUser()));
    }

}
