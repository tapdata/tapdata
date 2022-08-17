package com.tapdata.tm.task.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.autoinspect.dto.TaskAutoInspectResultDto;
import com.tapdata.tm.task.entity.TaskAutoInspectGroupTableResultEntity;
import com.tapdata.tm.task.service.TaskAutoInspectResultsService;
import com.tapdata.tm.task.service.TaskService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;


/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/8/15 08:12 Create
 */
@Tag(name = "TaskAutoInspect", description = "任务自动校验相关接口")
@RestController
@Slf4j
@RequestMapping({"/api/Task", "/api/task"})
@Setter(onMethod_ = {@Autowired})
public class TaskAutoInspectResultsController extends BaseController {
    private TaskService taskService;
    private TaskAutoInspectResultsService resultsService;

    @Operation(summary = "保存自动校验结果")
    @PostMapping({"/{id}/auto-inspect-results"})
    public ResponseMessage<TaskAutoInspectResultDto> save(@PathVariable("id") String taskId, @RequestBody TaskAutoInspectResultDto dto) {
        dto.setId(null);
        dto.setTaskId(taskId);
        return success(resultsService.save(dto, getLoginUser()));
    }

    @Operation(summary = "更新自动校验结果")
    @PatchMapping({"/{id}/auto-inspect-results"})
    public ResponseMessage<TaskAutoInspectResultDto> update(@PathVariable("id") String taskId, @RequestBody TaskAutoInspectResultDto dto) {
        dto.setTaskId(taskId);
        return success(resultsService.save(dto, getLoginUser()));
    }

    @Operation(summary = "详情")
    @GetMapping({"/auto-inspect-results/{rid}"})
    public ResponseMessage<TaskAutoInspectResultDto> detail(@PathVariable("rid") String resultId) {
        TaskAutoInspectResultDto resultDto = resultsService.findById(new ObjectId(resultId), getLoginUser());
        return success(resultDto);
    }

    @Operation(summary = "结果列表")
    @GetMapping({"/{id}/auto-inspect-results"})
    public ResponseMessage<Page<TaskAutoInspectResultDto>> find(@PathVariable("id") String taskId
            , @RequestParam(value = "filter", required = false) String filterJson
    ) {
        Filter filter = parseFilter(filterJson);

        if (filter == null) {
            filter = new Filter();
        }
        filter.getWhere().and("taskId", taskId);

        return success(resultsService.find(filter, getLoginUser()));
    }

    @Operation(summary = "表差异统计")
    @GetMapping({"/{id}/auto-inspect-results-group-by-table"})
    public ResponseMessage<Page<TaskAutoInspectGroupTableResultEntity>> groupByTable(@PathVariable("id") String taskId
            , @RequestParam(value = "tableName", required = false) String tableName
            , @RequestParam(value = "skip", required = false, defaultValue = "0") Long skip
            , @RequestParam(value = "limit", required = false, defaultValue = "20") Integer limit
    ) {
        getLoginUser();
        return success(resultsService.groupByTable(taskId, tableName, skip, limit));
    }


}
