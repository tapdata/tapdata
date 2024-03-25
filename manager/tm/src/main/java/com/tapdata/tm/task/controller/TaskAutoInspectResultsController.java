package com.tapdata.tm.task.controller;

import com.mongodb.client.result.UpdateResult;
import com.tapdata.tm.autoinspect.constants.AutoInspectConstants;
import com.tapdata.tm.autoinspect.dto.TaskAutoInspectResultDto;
import com.tapdata.tm.autoinspect.entity.CheckAgainProgress;
import com.tapdata.tm.autoinspect.service.TaskAutoInspectResultsService;
import com.tapdata.tm.autoinspect.utils.AutoInspectUtil;
import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.monitor.param.IdFilterPageParam;
import com.tapdata.tm.monitor.param.IdParam;
import com.tapdata.tm.monitor.param.TablesParam;
import com.tapdata.tm.task.entity.TaskAutoInspectGroupTableResultEntity;
import com.tapdata.tm.task.service.TaskServiceImpl;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


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
    private TaskServiceImpl taskService;
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

    @Operation(summary = "再次校验")
    @PostMapping({"/{id}/auto-inspect-again"})
    public ResponseMessage<UpdateResult> checkAgain(@PathVariable("id") String taskId, @RequestBody TablesParam params) {
        UserDetail userDetail = getLoginUser();
        List<String> tables = params.getTables();
        if (null == tables || tables.isEmpty()) {
            return failed("Params.NotEmpty", "tables");
        }

        TaskDto taskDto = taskService.findByTaskId(new ObjectId(taskId), "agentId", AutoInspectConstants.CHECK_AGAIN_PROGRESS_PATH);
        if (null == taskDto) {
            return failed("Task.NotExists");
        }

        CheckAgainProgress checkAgainProgress = AutoInspectUtil.toCheckAgainProgress(taskDto.getAttrs());
        if (null != checkAgainProgress) {
            if (AutoInspectUtil.isTimeout(checkAgainProgress)) {
                resultsService.checkAgainTimeout(taskId, checkAgainProgress.getSn(), userDetail);
            }

            switch (checkAgainProgress.getStatus()) {
                case Running:
                case Scheduling:
                    return failed("AutoInspect.CheckAgain." + checkAgainProgress.getStatus());
                default:
                    break;
            }
        }

        return success(resultsService.checkAgainStart(taskId, taskDto.getAgentId(), tables, AutoInspectUtil.newBatchNumber(), userDetail));
    }

    @Operation(summary = "自动校验结果统计")
    @PostMapping({"/auto-inspect-totals"})
    public ResponseMessage<Map<String, Object>> save(@RequestBody IdParam param) {
        getLoginUser();

        return success(taskService.totalAutoInspectResultsDiffTables(param));
    }

    @Operation(summary = "表差异统计")
    @PostMapping({"/auto-inspect-results-group-by-table"})
    public ResponseMessage<Map<String, Object>> groupByTable(@RequestBody IdFilterPageParam param) {
        UserDetail userDetail = getLoginUser();

        String taskId = param.getId();
        TaskDto taskDto = taskService.findByTaskId(new ObjectId(taskId), AutoInspectConstants.CHECK_AGAIN_PROGRESS_PATH);
        if (null == taskDto) {
            return failed("Task.NotExists");
        }

        //clear againBatchNumber and status on results if checkAgainProgress heartbeat timeout
        CheckAgainProgress checkAgainProgress = AutoInspectUtil.toCheckAgainProgress(taskDto.getAttrs());
        if (AutoInspectUtil.isTimeout(checkAgainProgress)) {
            resultsService.checkAgainTimeout(taskId, checkAgainProgress.getSn(), userDetail);
        }

        Page<TaskAutoInspectGroupTableResultEntity> pageData = resultsService.groupByTable(param);
        return success(new HashMap<String, Object>(){{
            put("total", pageData.getTotal());
            put("items", pageData.getItems());
            put("progress", checkAgainProgress);
        }});
    }


}
