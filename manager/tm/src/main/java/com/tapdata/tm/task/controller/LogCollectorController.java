package com.tapdata.tm.task.controller;

import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.base.dto.Where;
import com.tapdata.tm.task.bean.*;
import com.tapdata.tm.task.service.LogCollectorExtendService;
import com.tapdata.tm.task.service.LogCollectorService;
import com.tapdata.tm.task.vo.LogCollectorRelateTaskVo;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: Zed
 * @Date: 2022/2/18
 * @Description:
 */
@Tag(name = "LogCollector", description = "Task相关接口")
@RestController
@RequestMapping("/api/logcollector")
@Setter(onMethod_ = {@Autowired})
public class LogCollectorController extends BaseController {
    private LogCollectorService logCollectorService;
    private LogCollectorExtendService logCollectorExtendService;

    /**
     *  查询挖掘任务列表
     * @param filterJson 可选参数名称 支持模糊查询
     * @return 挖掘任务列表
     */
    @GetMapping()
    @Operation(summary = "查询挖掘任务列表")
    public ResponseMessage<Page<LogCollectorVo>> find(
            @Parameter(in = ParameterIn.QUERY,
                    description = "Filter defining fields, where, sort, skip, and limit - must be a JSON-encoded string (`{\"where\":{\"something\":\"value\"},\"fields\":{\"something\":true|false},\"sort\": [\"name desc\"],\"page\":1,\"size\":20}`)."
            )
            @RequestParam(value = "filter", required = false) String filterJson) {
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        Where where = filter.getWhere();
        String taskName = (String) where.get("taskName");
        String connectionName = (String) where.get("connectionName");

        if (StringUtils.isBlank(connectionName)) {
            return success(logCollectorService.find(taskName, getLoginUser(), filter.getSkip(), filter.getLimit(), filter.getSort()));
        } else {
            return success(logCollectorService.findByConnectionName(taskName, connectionName, getLoginUser(), filter.getSkip(), filter.getLimit(), filter.getSort()));
        }
    }

    @PatchMapping("{id}")
    @Operation(summary = "更新挖掘任务")
    public ResponseMessage<Void> update(@PathVariable("id") String id, @RequestBody LogCollectorEditVo logCollectorEditVo) {
        logCollectorEditVo.setId(id);
        logCollectorService.update(logCollectorEditVo, getLoginUser());
        return success();
    }

    /**
     *  查询挖掘任务详情
     * @param logCollectorId 挖掘任务id
     * @return 挖掘任务详情
     */
    @GetMapping("/detail/{id}")
    @Operation(summary = "查询挖掘任务详情")
    public ResponseMessage<LogCollectorDetailVo> findDetail(@PathVariable("id") String logCollectorId) {
        return success(logCollectorService.findDetail(logCollectorId, getLoginUser()));
    }

    /**
     *  通过同步任务查询被用到的挖掘任务列表
     * @param taskId 任务id
     * @return 挖掘任务列表
     */
    @GetMapping("/byTaskId/{taskId}")
    @Operation(summary = "通过同步任务查询被用到的挖掘任务列表")
    public ResponseMessage<List<LogCollectorVo>> findByTaskId(@PathVariable("taskId")String taskId) {
        return success(logCollectorService.findByTaskId(taskId, getLoginUser()));
    }

    @GetMapping("/system/config")
    public ResponseMessage<LogSystemConfigDto> findSystemConfig() {
        return success(logCollectorService.findSystemConfig(getLoginUser()));
    }


    @PatchMapping("/system/config")
    public ResponseMessage<Void> updateSystemConfig(@RequestBody LogSystemConfigDto logSystemConfigDto) {
        logCollectorService.updateSystemConfig(logSystemConfigDto, getLoginUser());
        return success();
    }


    @GetMapping("/check/system/config")
    public ResponseMessage<Map<String, Boolean>> checkUpdateConfig() {
        Map<String, Boolean> data = new HashMap<>();
        data.put("data", logCollectorService.checkUpdateConfig(getLoginUser()));
        return  success(data);
    }

    @GetMapping("/tableNames/{taskId}")
    public ResponseMessage<Page<Map<String, String>>> findTableNames(@PathVariable("taskId") String taskId,
                                                                     @RequestParam(value = "skip", required = false, defaultValue = "0") int skip,
                                                                     @RequestParam(value = "limit", required = false, defaultValue = "20") int limit)  {
        return  success(logCollectorService.findTableNames(taskId, skip, limit, getLoginUser()));
    }

    @GetMapping("/tableNames/{taskId}/{callSubId}")
    public ResponseMessage<Page<Map<String, String>>> findTableNames(@PathVariable("taskId") String taskId,@PathVariable("callSubId") String callSubId,
                                                                     @RequestParam(value = "skip", required = false, defaultValue = "0") int skip,
                                                                     @RequestParam(value = "limit", required = false, defaultValue = "20") int limit)  {
        return  success(logCollectorService.findCallTableNames(taskId, callSubId, skip, limit, getLoginUser()));
    }

    @GetMapping("/relate_tasks")
    @Operation(summary = "共享挖掘关联的数据复制/开发任务列表")
    public ResponseMessage<Page<LogCollectorRelateTaskVo>> getRelateTasks(@RequestParam String taskId,
                                                                          @RequestParam String type,
                                                                          @RequestParam(defaultValue = "1") Integer page,
                                                                          @RequestParam(defaultValue = "20") Integer size) {
        return success(logCollectorExtendService.getRelationTask(taskId, type, page, size));
    }



    @GetMapping("connectionInfo")
    @Operation(summary = "查询合并的连接表详情")
    public ResponseMessage<Page<ShareCdcConnectionInfo>> connectionInfo(@RequestParam("taskId") String taskId, @RequestParam("connectionId") String connectionId,
                                                                  @RequestParam(value = "page", defaultValue = "1") Integer page,
                                                                  @RequestParam(value = "size", defaultValue = "20") Integer size) {
        Page<ShareCdcConnectionInfo> connectionInfos = logCollectorService.connectionInfo(taskId, connectionId, page, size, getLoginUser());
        return success(connectionInfos);
    }


    @PostMapping("cancel/merge")
    @Operation(summary = "取消当前连接的合并")
    public ResponseMessage<Void> cancelMerge(@RequestParam("taskId") String taskId, @RequestParam("connectionId") String connectionId) {

        logCollectorService.cancelMerge(taskId, connectionId, getLoginUser());

        return success();
    }



}
