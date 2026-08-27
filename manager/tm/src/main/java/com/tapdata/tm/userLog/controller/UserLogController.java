package com.tapdata.tm.userLog.controller;


import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.permissions.constants.DataPermissionEnumsName;
import com.tapdata.tm.userLog.constant.Modular;
import com.tapdata.tm.userLog.dto.UserLogDto;
import com.tapdata.tm.userLog.param.AddUserLogParam;
import com.tapdata.tm.userLog.service.UserLogService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

@RestController
@Tag(name = "UserLog", description = "操作日志相关接口")
@Slf4j
@RequestMapping("/api/UserLogs")
public class UserLogController extends BaseController {

    final UserLogService userLogService;
    final PermissionService permissionService;

    public UserLogController(UserLogService userLogService, PermissionService permissionService) {
        this.userLogService = userLogService;
        this.permissionService = permissionService;
    }

    @Operation(summary = "查询列表")
    @GetMapping
    public ResponseMessage<Page<UserLogDto>> list(@RequestParam(value = "filter", required = false) String filterJson) {
        if (!hasAuditPermission()) {
            return failed("NotAuthorized");
        }
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(userLogService.find(filter, getLoginUser()));
    }

    @Operation(summary = "查询审计日志详情")
    @GetMapping("/{id}")
    public ResponseMessage<UserLogDto> detail(@PathVariable("id") String id) {
        if (!hasAuditPermission()) {
            return failed("NotAuthorized");
        }
        UserLogDto userLog = userLogService.findById(id, getLoginUser());
        return userLog == null ? failed("Audit.Log.Not.Found") : success(userLog);
    }

    private boolean hasAuditPermission() {
        return getLoginUser().isRoot()
                || permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_USER_MANAGEMENT, getLoginUser().getUserId())
                || permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_ROLE_MANAGEMENT, getLoginUser().getUserId());
    }

    @Operation(summary = "添加操作日志")
    @PostMapping
    public ResponseMessage<Page<UserLogDto>> add(@RequestBody AddUserLogParam addUserLogParam) {
        try {
            userLogService.addUserLog(Modular.of(addUserLogParam.getModular())
                    , com.tapdata.tm.userLog.constant.Operation.of(addUserLogParam.getOperation()),
                    getLoginUser(), addUserLogParam.getSourceId(), addUserLogParam.getParameter1(),
                    addUserLogParam.getParameter2(), addUserLogParam.getRename());
        } catch (Exception e) {
            log.error("添加操作日志异常", e);
            return failed("添加操作日志异常");
        }
        return success();
    }


}
