package com.tapdata.tm.application.controller;

import cn.hutool.core.map.MapUtil;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.tapdata.tm.application.dto.ApplicationDto;
import com.tapdata.tm.application.service.ApplicationService;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.utils.Lists;
import com.tapdata.tm.utils.MapUtils;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.security.oauth2.core.ClientAuthenticationMethod;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;


/**
 * @Date: 2021/10/15
 * @Description:
 */
@Tag(name = "Application", description = "Applications 相关接口")
@RestController
@RequestMapping(value = {"/api/Applications"})
public class ApplicationController extends BaseController {

    @Autowired
    private ApplicationService applicationService;

    /**
     * Create a new instance of the model and persist it into the data source
     *
     * @param metadataDefinition
     * @return
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<ApplicationDto> save(@RequestBody ApplicationDto applicationDto) {
        applicationDto.setId(null);
//        ObjectId objectId = new ObjectId();
//        metadataDefinition.setId(objectId);
//        metadataDefinition.setClientId(objectId.toHexString());
        if (CollectionUtils.isEmpty(applicationDto.getClientAuthenticationMethods())) {
            applicationDto.setClientAuthenticationMethods(Sets.newHashSet(ClientAuthenticationMethod.POST.getValue()));
        }
        ApplicationDto dto = applicationService.save(applicationDto, getLoginUser());
        dto.setClientId(dto.getId().toHexString());
        applicationService.updateById(dto, getLoginUser());
        return success(dto);
    }

    /**
     * 分页返回
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<ApplicationDto>> find(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        Where where = filter.getWhere();
        if (where == null) {
            where = new Where();
            filter.setWhere(where);
        }
        Document document = new Document();
        document.put("$ne", true);
        where.putIfAbsent("is_deleted", document);

        return success(applicationService.find(filter, getLoginUser()));
    }


    /**
     * Patch attributes for a model instance and persist it into the data source
     *
     * @param metadataDefinition
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping
    public ResponseMessage<ApplicationDto> updateById(@RequestBody ApplicationDto metadataDefinition) {
        return success(applicationService.updateById(metadataDefinition, getLoginUser()));
    }


    /**
     * Find a model instance by {{id}} from the data source
     *
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<ApplicationDto> findById(@PathVariable("id") String id,
                                                    @RequestParam("fields") String fieldsJson) {
        Field fields = parseField(fieldsJson);
        return success(applicationService.findById(MongoUtils.toObjectId(id), fields, getLoginUser()));
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
        applicationService.deleteLogicsById(id);
        return success();
    }


    /**
     *  Count instances of the model matched by where from the data source
     * @param whereJson
     * @return
     */
  /*  @Operation(summary = "Count instances of the model matched by where from the data source")
    @GetMapping("count")
    public ResponseMessage<HashMap<String, Long>> count(@RequestParam("where") String whereJson) {
        Where where = parseWhere(whereJson);
        if (where == null) {
            where = new Where();
        }
        long count = applicationService.count(where, getLoginUser());
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }*/

    /**
     * Find first instance of the model matched by filter from the data source.
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<ApplicationDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(applicationService.findOne(filter, getLoginUser()));
    }

    /**
     * Update instances of the model matched by {{where}} from the data source.
     *
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody ApplicationDto metadataDefinition) {
        Where where = parseWhere(whereJson);
        long count = applicationService.updateByWhere(where, metadataDefinition, getLoginUser());
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
    public ResponseMessage<ApplicationDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody ApplicationDto metadataDefinition) {
        Where where = parseWhere(whereJson);
        return success(applicationService.upsertByWhere(where, metadataDefinition, getLoginUser()));
    }

}