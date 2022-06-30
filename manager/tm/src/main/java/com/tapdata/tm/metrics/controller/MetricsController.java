package com.tapdata.tm.metrics.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.metrics.dto.MetricsDto;
import com.tapdata.tm.metrics.service.MetricsService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2021/12/9 下午4:47
 */
@Tag(name = "Metrics", description = "监控相关接口")
@RestController
@RequestMapping("/api/Metrics")
@Slf4j
public class MetricsController extends BaseController {

    private final MetricsService metricsService;

    public MetricsController(MetricsService metricsService) {
        this.metricsService = metricsService;
    }

    /**
     * 查询列表
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "Query list for metrics whit filter.")
    @GetMapping
    public ResponseMessage<Page<MetricsDto>> find(@RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        return success(metricsService.find(filter, getLoginUser()));
    }

    /**
     * 获取维度消息的数字
     *
     * @param filterJson
     * @return
     */
    @Operation(summary = "Query row count by where.")
    @GetMapping("count")
    public ResponseMessage<Long> count(@RequestParam(value = "where", required = false) String filterJson) {
        Where where = parseWhere(filterJson);
        return success(metricsService.count(where, getLoginUser()));
    }

    @Operation(summary = "Create a new Metrics of the model and persist it into the data source")
    @PostMapping
    public ResponseMessage<MetricsDto> save(@RequestBody MetricsDto dto) {
        return success(metricsService.save(dto, getLoginUser()));
    }

    @Operation(summary = "Batch create new Metrics of the model and persist it into the data source")
    @PostMapping("/batch")
    public ResponseMessage<Map<String, Integer>> save(@RequestBody List<MetricsDto> dto) {
        int size = metricsService.save(dto, getLoginUser()).size();
        Map<String, Integer> returnMap = new HashMap<>();
        returnMap.put("count", size);
        return success(returnMap);
    }

}
