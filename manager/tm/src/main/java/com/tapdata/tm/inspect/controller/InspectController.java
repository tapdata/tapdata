package com.tapdata.tm.inspect.controller;

import com.alibaba.fastjson.JSON;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.*;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.schema.bean.PlatformInfo;
import com.tapdata.tm.inspect.dto.InspectDto;
import com.tapdata.tm.inspect.service.InspectResultService;
import com.tapdata.tm.inspect.service.InspectService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.List;
import java.util.Map;


/**
 * @Date: 2021/09/14
 * @Description:
 */
@Tag(name = "Inspect", description = "校验任务相关接口")
@Slf4j
@RestController
@RequestMapping("/api/Inspects")
public class InspectController extends BaseController {

    @Autowired
    private InspectService inspectService;

    @Autowired
    private InspectResultService inspectResultService;

    /**
     * Create a new instance of the model and persist it into the data source
     *
     * @param inspect
     * @return
     */
    @Operation(summary = "Create a new instance of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<InspectDto> save(@RequestBody InspectDto inspect) {
        List task = inspect.getTasks();
        PlatformInfo platformInfo = inspect.getPlatformInfo();
        if (CollectionUtils.isEmpty(task)) {
            throw new BizException("Inspect.task.null");
        }

        if (null == platformInfo || StringUtils.isEmpty(platformInfo.getAgentType())) {
            throw new BizException("Inspect.agentTag.null");
        }

        if (inspectService.findByName(inspect.getName()).size() > 0) {
            throw new BizException("Inspect.Name.Exist");
        }

        Date date=new Date();
        inspect.setPing_time(date.getTime());
        return success(inspectService.save(inspect, getLoginUser()));
    }


    /**
     * 编辑inspect属性的时候调用
     *
     * @param inspect
     * @return
     */
    @PatchMapping()
    public ResponseMessage<InspectDto> updateById(@RequestBody InspectDto inspect) {
        InspectDto inspectDto = inspectService.updateById(inspect.getId(), inspect, getLoginUser());
        return success(inspect);
    }


    /**
     * @param filterJson
     * 获取校验任务列表
     * @return
     */
    @GetMapping
    public ResponseMessage<Page<InspectDto>> find(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(inspectService.list(filter, getLoginUser()));
    }


    /**
     * 获取校验详情
     *
     * @return
     */
    @Operation(summary = "Find a model instance by {{id}} from the data source")
    @GetMapping("findById")
    public ResponseMessage<InspectDto> findById(@RequestParam("filter") String filterStr) {
        Filter filter = parseFilter(filterStr);
        InspectDto inspectDto = inspectService.findById(filter, getLoginUser());
        return success(inspectDto);
    }


    /**
     * Delete a model instance by {{id}} from the data source
     *
     * @param id
     * @return
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Map<String, Long>> delete(@PathVariable("id") String id) {
        return success(inspectService.delete(id, getLoginUser()));
    }


    /**
     * Find first instance of the model matched by filter from the data source.
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<InspectDto> findOne(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"field\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(inspectService.findOne(filter, getLoginUser()));
    }


    /**
     * 页面点击  和engine更新inspect的时候，都会调用这个方法
     *
     * @param whereJson
     * @param inspect
     * @return
     */
    @Operation(summary = "页面点击  和 engine 更新inspect的时候，都会调用这个方法")
    @PostMapping("update")
    public ResponseMessage updateByWhere(@RequestParam("where") String whereJson, @RequestBody InspectDto inspect) throws Exception {
        log.info("InspectController--updateByWhere。whereJson：{}， InspectDto：{} ", whereJson, JSON.toJSONString(inspect));
        whereJson = whereJson.replace("\"_id\"", "\"id\"");
        Where where = parseWhere(whereJson);
        InspectDto inspectDto = inspectService.updateInspectByWhere(where, inspect, getLoginUser());
        return success(inspectDto);
    }


    /**
     * 给engin调用，更新inspect状态的
     * engin  调用该方法，result  总是返回空ss字符串，导致inspect表更新总是不正确，因此需要另作调整
     *
     * @param whereJson
     * @param inspect
     * @return
     */
    @Operation(summary = "给engin调用，更新inspect状态的")
    @PostMapping("upsertWithWhere")
    public ResponseMessage<InspectDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody InspectDto inspect) {
        log.info("engin upsertWithWhere whereJson:{}, inspect:{} ", whereJson, JSON.toJSONString(inspect));
        Where where = parseWhere(whereJson);
        Long inspectDto = inspectService.updateByWhere(where, inspect, getLoginUser());
        return success(inspect);
    }

}
