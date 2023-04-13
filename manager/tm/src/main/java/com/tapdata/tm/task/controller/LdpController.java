package com.tapdata.tm.task.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.task.bean.LdpFuzzySearchVo;
import com.tapdata.tm.task.service.LdpService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;


@Tag(name = "LDP", description = "Task相关接口")
@RestController
@Slf4j
@RequestMapping("/api/ldp")
@Setter(onMethod_ = {@Autowired})
public class LdpController extends BaseController {

    private LdpService ldpService;


    /**
     * Create a new task of the fdm
     *
     * @param task task
     * @return TaskDto
     */
    @Operation(summary = "Create a new task of the fdm")
    @PostMapping("fdm/task")
    public ResponseMessage<TaskDto> createFdmTask(@RequestBody TaskDto task) {
        return success(ldpService.createFdmTask(task, getLoginUser()));
    }


    /**
     * Create a new task of the mdm
     *
     * @param task task
     * @return TaskDto
     */
    @Operation(summary = "Create a new task of the fdm")
    @PostMapping("mdm/task")
    public ResponseMessage<TaskDto> createMdmTask(@RequestBody TaskDto task, @RequestParam(value = "tagId", required = false) String tagId,
                                                  @RequestParam(value = "confirmTable", required = false, defaultValue = "false") Boolean confirmTable,
                                                  @RequestParam(value = "start", required = false, defaultValue = "true") Boolean start) {
        return success(ldpService.createMdmTask(task, tagId, getLoginUser(), confirmTable, start));
    }


    /**
     * Query fdm task by tags
     *
     * @param tags tags
     * @return TaskDto
     */
    @Operation(summary = "Query fdm task by tags")
    @PostMapping("fdm/task/byTags")
    public ResponseMessage<Map<String, TaskDto>> queryFdmTaskByTags(@RequestParam("tags") List<String> tags) {
        return success(ldpService.queryFdmTaskByTags(tags, getLoginUser()));
    }


    @Operation(summary = "Query fdm task by tags")
    @GetMapping("fuzzy/search")
    public ResponseMessage<List<LdpFuzzySearchVo>> fuzzySearch(@RequestParam("key") String key,
                                                               @RequestParam(value = "connectType", required = false) List<String> connectType) {
        return success(ldpService.fuzzySearch(key, connectType, getLoginUser()));
    }

}
