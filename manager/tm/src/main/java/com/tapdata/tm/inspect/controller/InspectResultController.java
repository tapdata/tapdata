package com.tapdata.tm.inspect.controller;

import cn.hutool.core.bean.BeanUtil;
import com.alibaba.fastjson.JSON;
import com.tapdata.tm.commons.util.JsonUtil;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.inspect.bean.GroupByFilter;
import com.tapdata.tm.inspect.dto.InspectResultDto;
import com.tapdata.tm.inspect.param.SaveInspectResultParam;
import com.tapdata.tm.inspect.param.UpdateInspectResultParam;
import com.tapdata.tm.inspect.service.InspectResultService;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;


/**
 * @Date: 2021/09/14
 * @Description:
 */
@Tag(name = "InspectResult", description = "数据校验结果相关接口")
@RestController
@RequestMapping("/api/InspectResults")
@Slf4j
public class InspectResultController extends BaseController {

    @Autowired
    private InspectResultService inspectResultService;


    /**
     * 保存校验结果
     * @param inspectResult
     * @return
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<InspectResultDto> save(@RequestBody SaveInspectResultParam inspectResult) {
        log.info("InspectResultController--save。 inspectResult：{} ",  JSON.toJSONString(inspectResult));

        InspectResultDto save = inspectResultService.saveInspectResult(inspectResult, getLoginUser());
        inspectResultService.createAndPatch(save);
        return success(save);
    }

    /**
     *  Patch an existing model instance or insert a new one into the data source
     * @param inspectResult
     * @return
     */
    @Operation(summary = "Patch an existing model instance or insert a new one into the data source")
    @PatchMapping()
    public ResponseMessage<InspectResultDto> update(@RequestBody InspectResultDto inspectResult) {
        log.info("InspectResultController--update。 inspectResult：{} ",  JSON.toJSONString(inspectResult));
        InspectResultDto save = inspectResultService.save(inspectResult, getLoginUser());
        inspectResultService.createAndPatch(save);
        return success(save);
    }


    /**
     * Find all instances of the model matched by filter from the data source
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find all instances of the model matched by filter from the data source")
    @GetMapping
    public ResponseMessage<Page<InspectResultDto>> find(
            @RequestParam(value = "filter", required = false) String filterJson,
            @RequestParam(value = "inspectGroupByFirstCheckId", required = false) Boolean inspectGroupByFirstCheckId) {
        filterJson = replaceLoopBack(filterJson);
        GroupByFilter filter = JsonUtil.parseJson(filterJson, GroupByFilter.class);
        if (filter == null) {
            filter = new GroupByFilter();
        }

        if (inspectGroupByFirstCheckId == null) {
            inspectGroupByFirstCheckId = false;
            if (filter.getInspectGroupByFirstCheckId() != null) {
                inspectGroupByFirstCheckId = filter.getInspectGroupByFirstCheckId();
            }
        }
        return success(inspectResultService.find(filter, getLoginUser(), inspectGroupByFirstCheckId));
    }

    /**
     *  Replace an existing model instance or insert a new one into the data source
     * @param inspectResult
     * @return
     */
    @Operation(summary = "Replace an existing model instance or insert a new one into the data source")
    @PutMapping
    public ResponseMessage<InspectResultDto> put(@RequestBody InspectResultDto inspectResult) {
        return success(inspectResultService.replaceOrInsert(inspectResult, getLoginUser()));
    }



    /**
     *  Patch attributes for a model instance and persist it into the data source
     * @param inspectResult
     * @return
     */
    @Operation(summary = "Patch attributes for a model instance and persist it into the data source")
    @PatchMapping("{id}")
    public ResponseMessage<InspectResultDto> updateById(@PathVariable("id") String id, @RequestBody InspectResultDto inspectResult) {
        log.info("InspectResultController--updateById。 inspectResult：{} ",  JSON.toJSONString(inspectResult));
        inspectResult.setId(MongoUtils.toObjectId(id));
        return success(inspectResultService.save(inspectResult, getLoginUser()));
    }


    /**
     * 获取校验详情
     * @param filter
     * @return
     */
    @Operation(summary = "获取校验详情")
    @GetMapping("findById")
    public ResponseMessage<InspectResultDto> findById(  @RequestParam("filter") String filter) {
        Filter f = parseFilter(filter);
        return success(inspectResultService.findById(f,getLoginUser()));
    }

    /**
     * Delete a model instance by {{id}} from the data source
     * @param id
     * @return
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        inspectResultService.deleteById(MongoUtils.toObjectId(id), getLoginUser());
        return success();
    }

    /**
     *  Find first instance of the model matched by filter from the data source.
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<InspectResultDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        log.info("InspectResultController--findOne。 filterJson：{} ",  filterJson);
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(inspectResultService.findOne(filter, getLoginUser()));
    }

    /**
     *  Update instances of the model matched by {{where}} from the data source.
     * @param whereJson
     * @return
     */
    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody UpdateInspectResultParam updateInspectResultParam) {
        log.info("InspectResultController--updateByWhere 。whereJson：{}， InspectDto：{} ", whereJson, JSON.toJSONString(updateInspectResultParam));
        Where where = parseWhere(whereJson);
        InspectResultDto inspectResultDto= BeanUtil.copyProperties(updateInspectResultParam,InspectResultDto.class);
        long count = inspectResultService.updateByWhere(where, inspectResultDto, getLoginUser());
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }


    /**
     * engine 更新inspectResult 用
     * @param whereJson
     * @return
     */
    @Operation(summary = "engine 更新inspectResult 用")
    @PostMapping("upsertWithWhere")
    public ResponseMessage<InspectResultDto> upsertByWhere(@RequestParam("where") String whereJson,
                                                           @RequestBody SaveInspectResultParam saveInspectResultParam) {
        log.info("InspectResultController--upsertWithWhere 。whereJson：{}， inspectResult：{} ", whereJson, JSON.toJSONString(saveInspectResultParam));
        whereJson = whereJson.replace("\"_id\"", "\"id\"");
        Where where = parseWhere(whereJson);
        return success(inspectResultService.upsertInspectResultByWhere(where, saveInspectResultParam, getLoginUser()));
    }

}