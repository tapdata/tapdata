package com.tapdata.tm.autoinspect;

import com.tapdata.tm.autoinspect.dto.TaskAutoInspectResultDto;
import com.tapdata.tm.autoinspect.service.TaskAutoInspectResultsService;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.commons.base.dto.UpdateDto;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;


/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/16 14:39 Create
 */
@Tag(name = "AutoInspectResults", description = "任务自动校验相关接口")
@RestController
@Slf4j
@RequestMapping({"/api/AutoInspectResults", "/api/auto-inspect-results"})
@Setter(onMethod_ = {@Autowired})
public class AutoInspectResultsController extends BaseController {
    private TaskAutoInspectResultsService resultsService;

    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<TaskAutoInspectResultDto> save(@RequestBody TaskAutoInspectResultDto dto) {
        dto.setId(null);

        Criteria criteria = Criteria.where("taskId").is(dto.getTaskId())
                .and("originalTableName").is(dto.getOriginalTableName())
                .and("originalKeymap").is(dto.getOriginalKeymap());
        Query query = Query.query(criteria);
        resultsService.upsert(query, dto, getLoginUser());
        return success(dto);

//        return success(resultsService.save(dto, getLoginUser()));
    }

    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<TaskAutoInspectResultDto>> find(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(resultsService.find(filter, getLoginUser()));
    }

    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<TaskAutoInspectResultDto> put(@RequestBody TaskAutoInspectResultDto dto) {
        return success(resultsService.replaceOrInsert(dto, getLoginUser()));
    }

    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<TaskAutoInspectResultDto> updateById(
            @PathVariable("id") String id,
            @RequestBody TaskAutoInspectResultDto dto) {
        dto.setId(MongoUtils.toObjectId(id));
        return success(resultsService.save(dto, getLoginUser()));
    }

    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<TaskAutoInspectResultDto> findById(
            @PathVariable("id") String id,
            @RequestParam("fields") String fieldsJson) {
        Field fields = parseField(fieldsJson);
        return success(resultsService.findById(MongoUtils.toObjectId(id), fields, getLoginUser()));
    }

    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        resultsService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
        return success();
    }

    @Operation(summary = "Count instances of the model matched by where from the data source")
    @GetMapping("count")
    @Deprecated
    public ResponseMessage<HashMap<String, Long>> count(@RequestParam("where") String whereJson) {
        Where where = parseWhere(whereJson);
        if (where == null) {
            where = new Where();
        }
        long count = resultsService.count(where, getLoginUser());
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }

    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<TaskAutoInspectResultDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(resultsService.findOne(filter, getLoginUser()));
    }

    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody UpdateDto<TaskAutoInspectResultDto> dto) {
        Where where = parseWhere(whereJson);
        long count = resultsService.updateByWhere(where, dto, getLoginUser());
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }

    @Operation(summary = "Update an existing model instance or insert a new one into the data source based on the where criteria.")
    @PostMapping("upsertWithWhere")
    public ResponseMessage<TaskAutoInspectResultDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody TaskAutoInspectResultDto dto) {
        Where where = parseWhere(whereJson);
        return success(resultsService.upsertByWhere(where, dto, getLoginUser()));
    }
}
