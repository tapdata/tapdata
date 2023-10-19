package com.tapdata.tm.shareCdcTableMapping.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.shareCdcTableMapping.ShareCdcTableMappingDto;
import com.tapdata.tm.shareCdcTableMapping.service.ShareCdcTableMappingService;
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
 * @Date: 2023/10/16
 * @Description:
 */
@Tag(name = "ShareCdcTableMapping", description = "shareCdcTableMapping相关接口")
@RestController
@RequestMapping("/api/ShareCdcTableMapping")
public class ShareCdcTableMappingController extends BaseController {

    @Autowired
    private ShareCdcTableMappingService shareCdcTableMappingService;

    /**
     * Create a new instance of the model and persist it into the data source
     * @param shareCdcTableMapping
     * @return
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<ShareCdcTableMappingDto> save(@RequestBody ShareCdcTableMappingDto shareCdcTableMapping) {
        shareCdcTableMapping.setId(null);
        return success(shareCdcTableMappingService.save(shareCdcTableMapping, getLoginUser()));
    }

    /**
     *  Patch an existing model instance or insert a new one into the data source
     * @param shareCdcTableMapping
     * @return
     */
    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
    @PatchMapping()
    public ResponseMessage<ShareCdcTableMappingDto> update(@RequestBody ShareCdcTableMappingDto shareCdcTableMapping) {
        return success(shareCdcTableMappingService.save(shareCdcTableMapping, getLoginUser()));
    }


    /**
     * Find all instances of the model matched by filter from the data source
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<ShareCdcTableMappingDto>> find(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(shareCdcTableMappingService.find(filter, getLoginUser()));
    }

    /**
     *  Replace an existing model instance or insert a new one into the data source
     * @param shareCdcTableMapping
     * @return
     */
    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<ShareCdcTableMappingDto> put(@RequestBody ShareCdcTableMappingDto shareCdcTableMapping) {
        return success(shareCdcTableMappingService.replaceOrInsert(shareCdcTableMapping, getLoginUser()));
    }


    /**
     * Check whether a model instance exists in the data source
     * @return
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
    public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
        long count = shareCdcTableMappingService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     *  Patch attributes for a model instance and persist it into the data source
     * @param shareCdcTableMapping
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<ShareCdcTableMappingDto> updateById(@PathVariable("id") String id, @RequestBody ShareCdcTableMappingDto shareCdcTableMapping) {
        shareCdcTableMapping.setId(MongoUtils.toObjectId(id));
        return success(shareCdcTableMappingService.save(shareCdcTableMapping, getLoginUser()));
    }


    /**
     * Find a model instance by {{id}} from the data source
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<ShareCdcTableMappingDto> findById(@PathVariable("id") String id,
            @RequestParam(value = "fields", required = false) String fieldsJson) {
        Field fields = parseField(fieldsJson);
        return success(shareCdcTableMappingService.findById(MongoUtils.toObjectId(id),  fields, getLoginUser()));
    }

    /**
     *  Replace attributes for a model instance and persist it into the data source.
     * @param shareCdcTableMapping
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PutMapping("{id}")
    public ResponseMessage<ShareCdcTableMappingDto> replceById(@PathVariable("id") String id, @RequestBody ShareCdcTableMappingDto shareCdcTableMapping) {
        return success(shareCdcTableMappingService.replaceById(MongoUtils.toObjectId(id), shareCdcTableMapping, getLoginUser()));
    }

    /**
     *  Replace attributes for a model instance and persist it into the data source.
     * @param shareCdcTableMapping
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PostMapping("{id}/replace")
    public ResponseMessage<ShareCdcTableMappingDto> replaceById2(@PathVariable("id") String id, @RequestBody ShareCdcTableMappingDto shareCdcTableMapping) {
        return success(shareCdcTableMappingService.replaceById(MongoUtils.toObjectId(id), shareCdcTableMapping, getLoginUser()));
    }



    /**
     * Delete a model instance by {{id}} from the data source
     * @param id
     * @return
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        shareCdcTableMappingService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
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
        long count = shareCdcTableMappingService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
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
        long count = shareCdcTableMappingService.count(where, getLoginUser());
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
    public ResponseMessage<ShareCdcTableMappingDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(shareCdcTableMappingService.findOne(filter, getLoginUser()));
    }

    /**
     *  Update instances of the model matched by {{where}} from the data source.
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody ShareCdcTableMappingDto shareCdcTableMapping) {
        Where where = parseWhere(whereJson);
        long count = shareCdcTableMappingService.updateByWhere(where, shareCdcTableMapping, getLoginUser());
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
    public ResponseMessage<ShareCdcTableMappingDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody ShareCdcTableMappingDto shareCdcTableMapping) {
        Where where = parseWhere(whereJson);
        return success(shareCdcTableMappingService.upsertByWhere(where, shareCdcTableMapping, getLoginUser()));
    }

}