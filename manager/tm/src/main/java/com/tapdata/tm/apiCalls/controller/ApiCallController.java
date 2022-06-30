package com.tapdata.tm.apiCalls.controller;

import com.tapdata.tm.apiCalls.dto.ApiCallDto;
import com.tapdata.tm.apiCalls.param.GetClientParam;
import com.tapdata.tm.apiCalls.service.ApiCallService;
import com.tapdata.tm.apiCalls.vo.ApiCallDetailVo;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @Date: 2021/10/15
 * @Description:
 */
@Tag(name = "ApiCalls", description = "ApiCalls 相关接口")
@RestController
@Slf4j
@RequestMapping(value = {"/api/ApiCalls"})
public class ApiCallController extends BaseController {

    @Autowired
    private ApiCallService apiCallService;


    @Operation(summary = "api 没发送一次调用，都会调用该接口，网apicalls表新增一条记录")
    @PostMapping
    public ResponseMessage save(@RequestBody List<ApiCallDto> saveApiCallParamList) {
        return success(apiCallService.save(saveApiCallParamList));
    }

    /**
     * 分页返回
     *
     * @param filterJson
     * @return
     */
    @GetMapping
    public ResponseMessage<Page<ApiCallDetailVo>> find(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(apiCallService.find(filter,getLoginUser()));
    }


    /**
     * Patch attributes for a model instance and persist it into the data source
     *
     * @param metadataDefinition
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping
    public ResponseMessage<ApiCallDto> updateById(@RequestBody ApiCallDto metadataDefinition) {
        return success(apiCallService.updateById(metadataDefinition, getLoginUser()));
    }


    /**
     * Find a model instance by {{id}} from the data source
     *
     * @param fieldsJson
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("{id}")
    public ResponseMessage<ApiCallDetailVo> findById(@PathVariable("id") String id,
                                                     @RequestParam(value = "fields",required = false) String fieldsJson) {
        Field fields = parseField(fieldsJson);
        return success(apiCallService.findById(id, fields, getLoginUser()));
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
        apiCallService.deleteLogicsById(id);
        return success();
    }






    /**
     * Update instances of the model matched by {{where}} from the data source.
     *
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody ApiCallDto metadataDefinition) {
        Where where = parseWhere(whereJson);
        long count = apiCallService.updateByWhere(where, metadataDefinition, getLoginUser());
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
    public ResponseMessage<ApiCallDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody ApiCallDto metadataDefinition) {
        Where where = parseWhere(whereJson);
        return success(apiCallService.upsertByWhere(where, metadataDefinition, getLoginUser()));
    }

    @GetMapping("findClients")
    public ResponseMessage getClients(@RequestBody(required = false) GetClientParam getClientParam){
        List<String> moduleIds=new ArrayList<>();
        if (null!=getClientParam){
            moduleIds=getClientParam.getModuleIdList();
        }
        return success(apiCallService.findClients(moduleIds));
    }


    @GetMapping("getAllMethod")
    public ResponseMessage getAllMethod(@RequestBody(required = false) GetClientParam getClientParam){
        List<String> allMethod=new ArrayList<>();
        allMethod.add("GET");
        allMethod.add("POST");
        allMethod.add("DELETE");
        allMethod.add("PUT");
        allMethod.add("PATCH");
        return success(allMethod);
    }

    @GetMapping("getAllResponseCode")
    public ResponseMessage getAllResponseCode(@RequestBody(required = false) GetClientParam getClientParam){
        List<String> allMethod=new ArrayList<>();
        allMethod.add("200");
        allMethod.add(" ");
        return success(allMethod);
    }
}