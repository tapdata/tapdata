package com.tapdata.tm.task.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.commons.task.dto.progress.BatchOperationDto;
import com.tapdata.tm.commons.task.dto.progress.TaskSnapshotProgress;
import com.tapdata.tm.task.service.SnapshotEdgeProgressService;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @Date: 2021/12/15
 * @Description:
 */
@Tag(name = "snapshotEdgeProgress", description = "snapshotEdgeProgress相关接口")
@RestController
@RequestMapping("/api/TaskProgress")
public class SnapshotEdgeProgressController extends BaseController {

    @Autowired
    private SnapshotEdgeProgressService snapshotEdgeProgressService;

    /**
     * Create a new instance of the model and persist it into the data source
     * @param taskSnapshotProgress
     * @return
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<TaskSnapshotProgress> save(@RequestBody TaskSnapshotProgress taskSnapshotProgress) {
        taskSnapshotProgress.setId(null);
        return success(snapshotEdgeProgressService.save(taskSnapshotProgress, getLoginUser()));
    }

    /**
     *  Patch an existing model instance or insert a new one into the data source
     * @param taskSnapshotProgress
     * @return
     */
    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
    @PatchMapping()
    public ResponseMessage<TaskSnapshotProgress> update(@RequestBody TaskSnapshotProgress taskSnapshotProgress) {
        return success(snapshotEdgeProgressService.save(taskSnapshotProgress, getLoginUser()));
    }


    /**
     * Find all instances of the model matched by filter from the data source
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<TaskSnapshotProgress>> find(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        //校验用户是否存在
        getLoginUser();
        return success(snapshotEdgeProgressService.find(filter));
    }

    /**
     *  Patch attributes for a model instance and persist it into the data source
     * @param taskSnapshotProgress
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<TaskSnapshotProgress> updateById(@PathVariable("id") String id, @RequestBody TaskSnapshotProgress taskSnapshotProgress) {
        taskSnapshotProgress.setId(MongoUtils.toObjectId(id));
        return success(snapshotEdgeProgressService.save(taskSnapshotProgress, getLoginUser()));
    }


    /**
     * Find a model instance by {{id}} from the data source
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<TaskSnapshotProgress> findById(@PathVariable("id") String id,
                                                          @RequestParam(value = "fields", required = false) String fieldsJson) {
        Field fields = parseField(fieldsJson);
        //getLoginUser();//校验用户
        return success(snapshotEdgeProgressService.findById(MongoUtils.toObjectId(id),  fields));
    }


    /**
     * Delete a model instance by {{id}} from the data source
     * @param id
     * @return
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        getLoginUser();//校验用户
        snapshotEdgeProgressService.deleteById(MongoUtils.toObjectId(id));
        return success();
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
        getLoginUser();
        long count = snapshotEdgeProgressService.count(where);
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
    public ResponseMessage<TaskSnapshotProgress> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        getLoginUser();
        return success(snapshotEdgeProgressService.findOne(filter));
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
        Document update = Document.parse(reqBody);
        if (!update.containsKey("$set") && !update.containsKey("$setOnInsert") && !update.containsKey("$unset")) {
            Document _body = new Document();
            _body.put("$set", update);
            update = _body;
        }

        long count = snapshotEdgeProgressService.updateByWhere(where, update, getLoginUser());
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
    public ResponseMessage<TaskSnapshotProgress> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody TaskSnapshotProgress taskSnapshotProgress) {
        Where where = parseWhere(whereJson);
        return success(snapshotEdgeProgressService.upsertByWhere(where, taskSnapshotProgress, getLoginUser()));
    }

    @Operation(summary = "batch operation ")
    @PostMapping("batch")
    public ResponseMessage<Void> batchUpsert(@RequestBody List<BatchOperationDto> batchUpsertDtos) {
        snapshotEdgeProgressService.batchUpsert(batchUpsertDtos);
        return success();
    }

}