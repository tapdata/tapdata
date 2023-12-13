package com.tapdata.tm.task.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.MutiResponseMessage;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.task.bean.FdmBatchStartDto;
import com.tapdata.tm.task.bean.LdpFuzzySearchVo;
import com.tapdata.tm.task.bean.MultiSearchDto;
import com.tapdata.tm.task.service.LdpService;
import com.tapdata.tm.user.service.UserService;
import com.tapdata.tm.utils.MongoUtils;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Tag(name = "LDP", description = "Task相关接口")
@RestController
@Slf4j
@RequestMapping("/api/ldp")
@Setter(onMethod_ = {@Autowired})
public class LdpController extends BaseController {

    private LdpService ldpService;

    private UserService userService;


    /**
     * Create a new task of the fdm
     *
     * @param task task
     * @return TaskDto
     */
    @Operation(summary = "Create a new task of the fdm")
    @PostMapping("fdm/task")
    public ResponseMessage<TaskDto> createFdmTask(@RequestBody TaskDto task,
                                                  @RequestParam(value = "start", required = false, defaultValue = "true") Boolean start) {
        return success(ldpService.createFdmTask(task, start, getLoginUser()));
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
    @GetMapping("fdm/task/byTags")
    public ResponseMessage<Map<String, List<TaskDto>>> queryFdmTaskByTags(@RequestParam("tags") List<String> tags) {
        return success(ldpService.queryFdmTaskByTags(tags, getLoginUser()));
    }


    @Operation(summary = "Query fdm task by tags")
    @GetMapping("fuzzy/search")
    public ResponseMessage<List<LdpFuzzySearchVo>> fuzzySearch(@RequestParam("key") String key,
                                                               @RequestParam(value = "connectType", required = false) List<String> connectType) {
        return success(ldpService.fuzzySearch(key, connectType, getLoginUser()));
    }

    @Operation(summary = "Query fdm task by tags")
    @PostMapping("multi/search")
    public ResponseMessage<List<LdpFuzzySearchVo>> multiSearch(@RequestBody List<MultiSearchDto> multiSearchDtos) {
        return success(ldpService.multiSearch(multiSearchDtos, getLoginUser()));
    }


    @Operation(summary = "check fdm tag status")
    @GetMapping("check/fdm/status")
    public ResponseMessage<Map<String, Boolean>> checkFdmTaskStatus(@RequestParam("tagId") String tagId) {
        boolean canStart = ldpService.checkFdmTaskStatus(tagId, getLoginUser());
        Map<String, Boolean> resultMap = new HashMap<>();
        resultMap.put("result", canStart);
        return success(resultMap);
    }


    @Operation(summary = "fdm task batch start")
    @PostMapping("fdm/batch/start")
    public ResponseMessage<List<MutiResponseMessage>> fdmBatchStart(@RequestBody FdmBatchStartDto fdmBatchStartDto,
                                                                    HttpServletRequest request,
                                                                    HttpServletResponse response) {
        List<MutiResponseMessage> mutiResponseMessages =
                ldpService.fdmBatchStart(fdmBatchStartDto.getTagId(), fdmBatchStartDto.getTaskIds(), getLoginUser(),request,response);
        return success(mutiResponseMessages);
    }



    @DeleteMapping("mdm/table/{id}")
    public ResponseMessage<Void> deleteMdmTable(@PathVariable("id") String id) {
        UserDetail userDetail = getLoginUser();
        ldpService.deleteMdmTable(id, userDetail);
        return success();
    }

}
