package com.tapdata.tm.metadatadefinition.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.metadatadefinition.dto.ApiAppDetail;
import com.tapdata.tm.metadatadefinition.dto.MetadataDefinitionDto;
import com.tapdata.tm.metadatadefinition.service.ApiAppService;
import com.tapdata.tm.metadatadefinition.service.MetadataDefinitionService;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


/**
 * @Date: 2021/10/15
 * @Description:
 */
@Tag(name = "ApiApp", description = "api应用接口")
@RestController
@RequestMapping("api/app")
public class ApiAppController extends BaseController {



    @Autowired
    private MetadataDefinitionService metadataDefinitionService;
    @Autowired
    private ApiAppService apiAppService;

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


        return success(apiAppService.save(metadataDefinition, getLoginUser()));
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
        return success(apiAppService.find(filter, getLoginUser()));
    }



    /**
     *  Patch attributes for a model instance and persist it into the data source
     * @param metadataDefinition
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<MetadataDefinitionDto> updateById(@PathVariable("id") String id, @RequestBody MetadataDefinitionDto metadataDefinition) {
        return success(apiAppService.updateById(MongoUtils.toObjectId(id), metadataDefinition, getLoginUser()));
    }

    /**
     * Find a model instance by {{id}} from the data source
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<MetadataDefinitionDto> findById(@PathVariable("id") String id,
            @RequestParam(value = "fields", required = false) String fieldsJson) {
        Field fields = parseField(fieldsJson);
        return success(apiAppService.findById(MongoUtils.toObjectId(id),  fields, getLoginUser()));
    }


    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("detail/{id}")
    public ResponseMessage<ApiAppDetail> detail(@PathVariable("id") String id) {
        return success(apiAppService.detail(MongoUtils.toObjectId(id), getLoginUser()));
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
     * move object tags
     * @param id
     * @param moveId
     * @return
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @PutMapping("move/{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id, @RequestParam("moveId") String moveId) {
        apiAppService.move(id, moveId, getLoginUser());
        return success();
    }

}