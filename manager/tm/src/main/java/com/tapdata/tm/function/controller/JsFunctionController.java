package com.tapdata.tm.function.controller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.function.dto.JsFunctionDto;
import com.tapdata.tm.function.service.JsFunctionService;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @Date: 2022/04/07
 * @Description:
 */
@Tag(name = "JsFunction", description = "JsFunction相关接口")
@RestController
@RequestMapping("/api/Javascript_functions")
public class JsFunctionController extends BaseController {

    @Autowired
    private JsFunctionService jsFunctionService;

    /**
     * Create a new instance of the model and persist it into the data source
     * @param jsFunctionJson
     * @return
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<Object> save(@RequestBody String jsFunctionJson) {
        if (jsFunctionJson.startsWith("[")) {
            List<JsFunctionDto> jsFunctionDtos = JsonUtil.parseJsonUseJackson(jsFunctionJson, new TypeReference<List<JsFunctionDto>>() {
            });
            List<JsFunctionDto> returnList = new ArrayList<>();
            for (JsFunctionDto jsFunction : jsFunctionDtos) {
                jsFunction.setId(null);
                returnList.add(jsFunctionService.save(jsFunction, getLoginUser()));
            }
            return success(returnList);
        } else {
            JsFunctionDto jsFunction = JsonUtil.parseJsonUseJackson(jsFunctionJson, JsFunctionDto.class);

            jsFunction.setId(null);
            return success(jsFunctionService.save(jsFunction, getLoginUser()));
        }
    }

    /**
     *  Patch an existing model instance or insert a new one into the data source
     * @param jsFunction
     * @return
     */
    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
    @PatchMapping()
    public ResponseMessage<JsFunctionDto> update(@RequestBody JsFunctionDto jsFunction) {
        return success(jsFunctionService.save(jsFunction, getLoginUser()));
    }


    /**
     * Find all instances of the model matched by filter from the data source
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<JsFunctionDto>> find(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        Where where = filter.getWhere();
        if (where != null && "system".equals(where.get("type"))) {
            return success(jsFunctionService.find(filter));
        }
        return success(jsFunctionService.find(filter, getLoginUser()));
    }

    /**
     *  Replace an existing model instance or insert a new one into the data source
     * @param jsFunction
     * @return
     */
    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<JsFunctionDto> put(@RequestBody JsFunctionDto jsFunction) {
        return success(jsFunctionService.replaceOrInsert(jsFunction, getLoginUser()));
    }


    /**
     * Check whether a model instance exists in the data source
     * @return
     */
    @Operation(summary = "Check whether a model instance exists in the data source")
    @RequestMapping(value = "{id}", method = RequestMethod.HEAD)
    public ResponseMessage<HashMap<String, Boolean>> checkById(@PathVariable("id") String id) {
        long count = jsFunctionService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
        HashMap<String, Boolean> existsValue = new HashMap<>();
        existsValue.put("exists", count > 0);
        return success(existsValue);
    }

    /**
     *  Patch attributes for a model instance and persist it into the data source
     * @param jsFunction
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<JsFunctionDto> updateById(@PathVariable("id") String id, @RequestBody JsFunctionDto jsFunction) {
        jsFunction.setId(MongoUtils.toObjectId(id));
        return success(jsFunctionService.save(jsFunction, getLoginUser()));
    }


    /**
     * Find a model instance by {{id}} from the data source
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<JsFunctionDto> findById(@PathVariable("id") String id,
            @RequestParam(value = "fields", required = false) String fieldsJson) {
        Field fields = parseField(fieldsJson);
        return success(jsFunctionService.findById(MongoUtils.toObjectId(id),  fields, getLoginUser()));
    }

    /**
     *  Replace attributes for a model instance and persist it into the data source.
     * @param jsFunction
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PutMapping("{id}")
    public ResponseMessage<JsFunctionDto> replceById(@PathVariable("id") String id, @RequestBody JsFunctionDto jsFunction) {
        return success(jsFunctionService.replaceById(MongoUtils.toObjectId(id), jsFunction, getLoginUser()));
    }

    /**
     *  Replace attributes for a model instance and persist it into the data source.
     * @param jsFunction
     * @return
     */
    @Operation(summary = "Replace attributes for a model instance and persist it into the data source.")
    @PostMapping("{id}/replace")
    public ResponseMessage<JsFunctionDto> replaceById2(@PathVariable("id") String id, @RequestBody JsFunctionDto jsFunction) {
        return success(jsFunctionService.replaceById(MongoUtils.toObjectId(id), jsFunction, getLoginUser()));
    }



    /**
     * Delete a model instance by {{id}} from the data source
     * @param id
     * @return
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        jsFunctionService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
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
        long count = jsFunctionService.count(Where.where("_id", MongoUtils.toObjectId(id)), getLoginUser());
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
        long count = jsFunctionService.count(where, getLoginUser());
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
    public ResponseMessage<JsFunctionDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(jsFunctionService.findOne(filter, getLoginUser()));
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

        long count = jsFunctionService.updateByWhere(where, update, getLoginUser());


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
    public ResponseMessage<JsFunctionDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody JsFunctionDto jsFunction) {
        Where where = parseWhere(whereJson);
        return success(jsFunctionService.upsertByWhere(where, jsFunction, getLoginUser()));
    }

}