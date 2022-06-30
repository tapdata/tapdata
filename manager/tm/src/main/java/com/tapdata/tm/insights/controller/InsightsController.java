package com.tapdata.tm.insights.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.insights.dto.InsightsDto;
import com.tapdata.tm.insights.service.InsightsService;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;


/**
 * @Date: 2021/10/14
 * @Description:
 */
@Tag(name = "Insights", description = "Insights  相关接口")
@RestController
@RequestMapping("/api/Insights")
public class InsightsController extends BaseController {

    @Autowired
    private InsightsService insightsService;


    @Operation(summary = "新增module")
    @PostMapping
    public ResponseMessage<InsightsDto> save(@RequestBody InsightsDto modules) {
        modules.setId(null);
        return success(insightsService.save(modules, getLoginUser()));
    }




    /**
     * 查询api列表,  分页返回
     *
     * @param filterJson
     * @return
     */
    @GetMapping
    public ResponseMessage find(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(insightsService.findInsightList(filter));
    }

    @GetMapping("{id}")
    public ResponseMessage<InsightsDto> findById(@PathVariable("id") String id) {
        return success(insightsService.findById(MongoUtils.toObjectId(id), getLoginUser()));
    }


    /**
     * 逻辑删除
     *
     * @param id
     * @return
     */
    @Operation(summary = "Delete a model instance by {{id}} from the data source")
    @DeleteMapping("{id}")
    public ResponseMessage<Void> delete(@PathVariable("id") String id) {
        insightsService.deleteLogicsById(id);
        return success();
    }


    @Operation(summary = "Find first instance of the model matched by filter from the data source.")
    @GetMapping("findOne")
    public ResponseMessage<InsightsDto> findOne(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(insightsService.findOne(filter, getLoginUser()));
    }


    @Operation(summary = "Update instances of the model matched by {{where}} from the data source")
    @PostMapping("update")
    public ResponseMessage<Map<String, Long>> updateByWhere(@RequestParam("where") String whereJson, @RequestBody InsightsDto modules) {
        Where where = parseWhere(whereJson);
        long count = insightsService.updateByWhere(where, modules, getLoginUser());
        HashMap<String, Long> countValue = new HashMap<>();
        countValue.put("count", count);
        return success(countValue);
    }


    @Operation(summary = "Update an existing model instance or insert a new one into the data source based on the where criteria.")
    @PostMapping("upsertWithWhere")
    public ResponseMessage<InsightsDto> upsertByWhere(@RequestParam("where") String whereJson, @RequestBody InsightsDto modules) {
        Where where = parseWhere(whereJson);
        return success(insightsService.upsertByWhere(where, modules, getLoginUser()));
    }
}