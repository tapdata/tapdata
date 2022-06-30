package com.tapdata.tm.datarules.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.datarules.dto.DataRulesDto;
import com.tapdata.tm.datarules.service.DataRulesService;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;


/**
 * @Date: 2021/10/19
 * @Description:
 */
@Tag(name = "DataRules", description = "DataRules相关接口")
@RestController
@RequestMapping("/api/DataRules")
public class DataRulesController extends BaseController {

    @Autowired
    private DataRulesService dataRulesService;

    /**
     * Create a new instance of the model and persist it into the data source
     *
     * @param dataRules
     * @return
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<DataRulesDto> save(@RequestBody DataRulesDto dataRules) {
        dataRules.setId(null);
        return success(dataRulesService.save(dataRules, getLoginUser()));
    }

    /**
     * Patch an existing model instance or insert a new one into the data source
     *
     * @param dataRules
     * @return
     */
    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
    @PatchMapping()
    public ResponseMessage<DataRulesDto> update(@RequestBody DataRulesDto dataRules) {
        return success(dataRulesService.save(dataRules, getLoginUser()));
    }


    /**
     *  查询数据规则 列表
     */
    @Operation(summary = "查询数据规则 列表")
    @GetMapping
    public ResponseMessage<Page<DataRulesDto>> find(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(dataRulesService.find(filter, getLoginUser()));
    }

    /**
     * Replace an existing model instance or insert a new one into the data source
     *
     * @param dataRules
     * @return
     */
    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<DataRulesDto> put(@RequestBody DataRulesDto dataRules) {
        return success(dataRulesService.replaceOrInsert(dataRules, getLoginUser()));
    }


    /**
     * Check whether a model instance exists in the data source
     *
     * @return
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
    public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
        long count = dataRulesService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     * Patch attributes for a model instance and persist it into the data source
     *
     * @param dataRules
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<DataRulesDto> updateById(@PathVariable("id") String id, @RequestBody DataRulesDto dataRules) {
        dataRules.setId(MongoUtils.toObjectId(id));
        return success(dataRulesService.save(dataRules, getLoginUser()));
    }


    /**
     * Find a model instance by {{id}} from the data source
     *
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<DataRulesDto> findById(@PathVariable("id") String id,
                                                  @RequestParam("fields") String fieldsJson) {
        Field fields = parseField(fieldsJson);
        return success(dataRulesService.findById(MongoUtils.toObjectId(id), fields, getLoginUser()));
    }

    /**
     * Replace attributes for a model instance and persist it into the data source.
     *
     * @param dataRules
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PutMapping("{id}")
    public ResponseMessage<DataRulesDto> replceById(@PathVariable("id") String id, @RequestBody DataRulesDto dataRules) {
        return success(dataRulesService.replaceById(MongoUtils.toObjectId(id), dataRules, getLoginUser()));
    }

    /**
     * Replace attributes for a model instance and persist it into the data source.
     *
     * @param dataRules
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PostMapping("{id}/replace")
    public ResponseMessage<DataRulesDto> replaceById2(@PathVariable("id") String id, @RequestBody DataRulesDto dataRules) {
        return success(dataRulesService.replaceById(MongoUtils.toObjectId(id), dataRules, getLoginUser()));
    }


    /**
     * Delete a model instance by {{id}} from the data source
     *
     * @param id
     * @return
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        dataRulesService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
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
        long count = dataRulesService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
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
        long count = dataRulesService.count(where, getLoginUser());
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
    public ResponseMessage<DataRulesDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(dataRulesService.findOne(filter, getLoginUser()));
    }

    /**
     * Update instances of the model matched by {{where}} from the data source.
     *
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody DataRulesDto dataRules) {
        Where where = parseWhere(whereJson);
        long count = dataRulesService.updateByWhere(where, dataRules, getLoginUser());
        HashMap<String, Long> countValue = new HashMap<>();
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
    public ResponseMessage<DataRulesDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody DataRulesDto dataRules) {
        Where where = parseWhere(whereJson);
        return success(dataRulesService.upsertByWhere(where, dataRules, getLoginUser()));
    }

}