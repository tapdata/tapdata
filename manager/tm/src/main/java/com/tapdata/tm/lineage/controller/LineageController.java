package com.tapdata.tm.lineage.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.commons.task.dto.Dag;
import com.tapdata.tm.lineage.dto.LineageDto;
import com.tapdata.tm.lineage.dto.TableLineageDto;
import com.tapdata.tm.lineage.service.LineageService;
import com.tapdata.tm.lineage.vo.TableLineageRequestVo;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @Date: 2023/05/19
 * @Description:
 */
@Tag(name = "Lineage", description = "Lineage相关接口")
@RestController
@RequestMapping({"/api/Lineage", "/api/lineage"})
public class LineageController extends BaseController {

    @Autowired
    private LineageService lineageService;

    /**
     * Create a new instance of the model and persist it into the data source
     *
     * @param lineage
     * @return
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<LineageDto> save(@RequestBody LineageDto lineage) {
        lineage.setId(null);
        return success(lineageService.save(lineage, getLoginUser()));
    }

    /**
     * Patch an existing model instance or insert a new one into the data source
     *
     * @param lineage
     * @return
     */
    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
    @PatchMapping()
    public ResponseMessage<LineageDto> update(@RequestBody LineageDto lineage) {
        return success(lineageService.save(lineage, getLoginUser()));
    }


    /**
     * Find all instances of the model matched by filter from the data source
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<LineageDto>> find(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(lineageService.find(filter, getLoginUser()));
    }

    /**
     * Replace an existing model instance or insert a new one into the data source
     *
     * @param lineage
     * @return
     */
    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<LineageDto> put(@RequestBody LineageDto lineage) {
        return success(lineageService.replaceOrInsert(lineage, getLoginUser()));
    }


    /**
     * Check whether a model instance exists in the data source
     *
     * @return
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
    public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
        long count = lineageService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     * Patch attributes for a model instance and persist it into the data source
     *
     * @param lineage
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<LineageDto> updateById(@PathVariable("id") String id, @RequestBody LineageDto lineage) {
        lineage.setId(MongoUtils.toObjectId(id));
        return success(lineageService.save(lineage, getLoginUser()));
    }


    /**
     * Find a model instance by {{id}} from the data source
     *
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<LineageDto> findById(@PathVariable("id") String id,
                                                @RequestParam(value = "fields", required = false) String fieldsJson) {
        Field fields = parseField(fieldsJson);
        return success(lineageService.findById(MongoUtils.toObjectId(id), fields, getLoginUser()));
    }

    /**
     * Replace attributes for a model instance and persist it into the data source.
     *
     * @param lineage
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PutMapping("{id}")
    public ResponseMessage<LineageDto> replceById(@PathVariable("id") String id, @RequestBody LineageDto lineage) {
        return success(lineageService.replaceById(MongoUtils.toObjectId(id), lineage, getLoginUser()));
    }

    /**
     * Replace attributes for a model instance and persist it into the data source.
     *
     * @param lineage
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PostMapping("{id}/replace")
    public ResponseMessage<LineageDto> replaceById2(@PathVariable("id") String id, @RequestBody LineageDto lineage) {
        return success(lineageService.replaceById(MongoUtils.toObjectId(id), lineage, getLoginUser()));
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
        lineageService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
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
        long count = lineageService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
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
        long count = lineageService.count(where, getLoginUser());
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
    public ResponseMessage<LineageDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(lineageService.findOne(filter, getLoginUser()));
    }

    /**
     * Update instances of the model matched by {{where}} from the data source.
     *
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody LineageDto lineage) {
        Where where = parseWhere(whereJson);
        long count = lineageService.updateByWhere(where, lineage, getLoginUser());
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
    public ResponseMessage<LineageDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody LineageDto lineage) {
        Where where = parseWhere(whereJson);
        return success(lineageService.upsertByWhere(where, lineage, getLoginUser()));
    }

    @Operation(summary = "Find table lineage")
    @GetMapping("/table/findByTable")
    public ResponseMessage<TableLineageDto> tableLineage(
            @RequestParam("connectionId") String connectionId,
            @RequestParam("table") String table,
            @RequestParam(value = "type", required = false) String type
    ) {
        TableLineageRequestVo tableLineageRequestVo = new TableLineageRequestVo(connectionId, table, type);
        try {
            return success(lineageService.tableLineage(tableLineageRequestVo));
        } catch (Exception e) {
            return failed(e);
        }
    }

}