package com.tapdata.tm.lineagegraph.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.lineagegraph.dto.LineageGraphDto;
import com.tapdata.tm.lineagegraph.service.LineageGraphService;
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
 * @Date: 2021/10/15
 * @Description:
 */
@Tag(name = "LineageGraph", description = "LineageGraph相关接口")
@RestController
@RequestMapping({"/api/LineageGraph","/api/LineageGraphs"})
public class LineageGraphController extends BaseController {

    @Autowired
    private LineageGraphService lineageGraphService;

    /**
     * Create a new instance of the model and persist it into the data source
     * @param lineageGraph
     * @return
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<LineageGraphDto> save(@RequestBody LineageGraphDto lineageGraph) {
        lineageGraph.setId(null);
        return success(lineageGraphService.save(lineageGraph, getLoginUser()));
    }

    /**
     *  Patch an existing model instance or insert a new one into the data source
     * @param lineageGraph
     * @return
     */
    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
    @PatchMapping()
    public ResponseMessage<LineageGraphDto> update(@RequestBody LineageGraphDto lineageGraph) {
        return success(lineageGraphService.save(lineageGraph, getLoginUser()));
    }


    /**
     * 数据链路图 查询
     */
    @Operation(summary = "数据链路图 查询")
    @GetMapping
    public ResponseMessage<Page<LineageGraphDto>> find(  @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(lineageGraphService.find(filter));
    }

    /**
     *  Replace an existing model instance or insert a new one into the data source
     * @param lineageGraph
     * @return
     */
    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<LineageGraphDto> put(@RequestBody LineageGraphDto lineageGraph) {
        return success(lineageGraphService.replaceOrInsert(lineageGraph, getLoginUser()));
    }


    /**
     * Check whether a model instance exists in the data source
     * @return
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
    public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
        long count = lineageGraphService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     *  Patch attributes for a model instance and persist it into the data source
     * @param lineageGraph
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<LineageGraphDto> updateById(@PathVariable("id") String id, @RequestBody LineageGraphDto lineageGraph) {
        lineageGraph.setId(MongoUtils.toObjectId(id));
        return success(lineageGraphService.save(lineageGraph, getLoginUser()));
    }


    /**
     * Find a model instance by {{id}} from the data source
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<LineageGraphDto> findById(@PathVariable("id") String id,
            @RequestParam("fields") String fieldsJson) {
        Field fields = parseField(fieldsJson);
        return success(lineageGraphService.findById(MongoUtils.toObjectId(id),  fields, getLoginUser()));
    }

    /**
     *  Replace attributes for a model instance and persist it into the data source.
     * @param lineageGraph
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PutMapping("{id}")
    public ResponseMessage<LineageGraphDto> replceById(@PathVariable("id") String id, @RequestBody LineageGraphDto lineageGraph) {
        return success(lineageGraphService.replaceById(MongoUtils.toObjectId(id), lineageGraph, getLoginUser()));
    }

    /**
     *  Replace attributes for a model instance and persist it into the data source.
     * @param lineageGraph
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PostMapping("{id}/replace")
    public ResponseMessage<LineageGraphDto> replaceById2(@PathVariable("id") String id, @RequestBody LineageGraphDto lineageGraph) {
        return success(lineageGraphService.replaceById(MongoUtils.toObjectId(id), lineageGraph, getLoginUser()));
    }



    /**
     * Delete a model instance by {{id}} from the data source
     * @param id
     * @return
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        lineageGraphService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
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
        long count = lineageGraphService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
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
        long count = lineageGraphService.count(where, getLoginUser());
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
    public ResponseMessage<LineageGraphDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(lineageGraphService.findOne(filter, getLoginUser()));
    }

    /**
     *  Update instances of the model matched by {{where}} from the data source.
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody LineageGraphDto lineageGraph) {
        Where where = parseWhere(whereJson);
        long count = lineageGraphService.updateByWhere(where, lineageGraph, getLoginUser());
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
    public ResponseMessage<LineageGraphDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody LineageGraphDto lineageGraph) {
        Where where = parseWhere(whereJson);
        return success(lineageGraphService.upsertByWhere(where, lineageGraph, getLoginUser()));
    }


    /**
     *  Update an existing model instance or insert a new one into the data source based on the where criteria.
     * @return
     */
  /*  @Operation(summary = "Update an existing model instance or insert a new one into the data source based on the where criteria.")
    @PostMapping("graphData")
    public ResponseMessage<LineageGraphDto> graphData(@RequestBody GraphDataParam graphDataParam) {
        String level=graphDataParam.getLevel();
        String qualifiedName =graphDataParam.getQualifiedName();
        return success(lineageGraphService.graphData(level, qualifiedName,null ));
    }*/



}