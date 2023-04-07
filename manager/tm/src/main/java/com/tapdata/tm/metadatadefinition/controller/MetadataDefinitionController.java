package com.tapdata.tm.metadatadefinition.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @Date: 2021/10/15
 * @Description:
 */
@Tag(name = "MetadataDefinition", description = "MetadataDefinition相关接口")
@RestController
@RequestMapping(value = {"/api/MetadataDefinition","/api/MetadataDefinitions"})
public class MetadataDefinitionController extends BaseController {

    @Autowired
    private MetadataDefinitionService metadataDefinitionService;

    /**
     * 创建分类
     * @param metadataDefinition
     * @return
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<MetadataDefinitionDto> save(@RequestBody MetadataDefinitionDto metadataDefinition) {
        metadataDefinition.setId(null);
        metadataDefinitionService.findByItemtypeAndValue(metadataDefinition,getLoginUser());
        return success(metadataDefinitionService.save(metadataDefinition, getLoginUser()));
    }

    /**
     *  Patch an existing model instance or insert a new one into the data source
     * @param metadataDefinition
     * @return
     */
    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
    @PatchMapping()
    public ResponseMessage<MetadataDefinitionDto> update(@RequestBody MetadataDefinitionDto metadataDefinition) {
        return success(metadataDefinitionService.save(metadataDefinition, getLoginUser()));
    }


    /**
     * Find all instances of the model matched by filter from the data source
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<MetadataDefinitionDto>> find(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }

        //数据目录不需要分页
        filter.setLimit(10000);
        return success(metadataDefinitionService.find(filter, getLoginUser()));
    }

    /**
     *  Replace an existing model instance or insert a new one into the data source
     * @param metadataDefinition
     * @return
     */
    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<MetadataDefinitionDto> put(@RequestBody MetadataDefinitionDto metadataDefinition) {
        return success(metadataDefinitionService.replaceOrInsert(metadataDefinition, getLoginUser()));
    }


    /**
     *  Patch attributes for a model instance and persist it into the data source
     * @param metadataDefinition
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<MetadataDefinitionDto> updateById(@PathVariable("id") String id, @RequestBody MetadataDefinitionDto metadataDefinition) {
        metadataDefinition.setId(MongoUtils.toObjectId(id));
        return success(metadataDefinitionService.save(metadataDefinition, getLoginUser()));
    }


    /**
     * Find a model instance by {{id}} from the data source
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<MetadataDefinitionDto> findById(@PathVariable("id") String id,
            @RequestParam("fields") String fieldsJson) {
        Field fields = parseField(fieldsJson);
        return success(metadataDefinitionService.findById(MongoUtils.toObjectId(id),  fields, getLoginUser()));
    }






    /**
     * Delete a model instance by {{id}} from the data source
     * @param id
     * @return
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        metadataDefinitionService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
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
        long count = metadataDefinitionService.count(where, getLoginUser());
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
    public ResponseMessage<MetadataDefinitionDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(metadataDefinitionService.findOne(filter, getLoginUser()));
    }

    /**
     *  Update instances of the model matched by {{where}} from the data source.
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody MetadataDefinitionDto metadataDefinition) {
        Where where = parseWhere(whereJson);
        long count = metadataDefinitionService.updateByWhere(where, metadataDefinition, getLoginUser());
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
    public ResponseMessage<MetadataDefinitionDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody MetadataDefinitionDto metadataDefinition) {
        Where where = parseWhere(whereJson);
        return success(metadataDefinitionService.upsertByWhere(where, metadataDefinition, getLoginUser()));
    }




    @GetMapping("child")
    public ResponseMessage<List<MetadataDefinitionDto>> findAndChild(String tagId) {
        return success(metadataDefinitionService.findAndChild(Lists.of(MongoUtils.toObjectId(tagId)), getLoginUser()));
    }

}