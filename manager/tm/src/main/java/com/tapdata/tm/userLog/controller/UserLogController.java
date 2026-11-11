package com.tapdata.tm.userLog.controller;

import com.tapdata.tm.base.exception.BizException;


import com.tapdata.tm.base.controller.BaseController;
import com.tapdata.tm.base.dto.Filter;
import com.tapdata.tm.base.dto.Page;
import com.tapdata.tm.base.dto.ResponseMessage;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.constants.DataPermissionEnumsName;
import com.tapdata.tm.permissions.service.ModuleAccess;
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
    final ModuleAccess moduleAccess;

    public UserLogController(UserLogService userLogService, ModuleAccess moduleAccess) {
        this.userLogService = userLogService;
        this.moduleAccess = moduleAccess;
    }

    @Operation(summary = "查询列表")
    @GetMapping
    public ResponseMessage<Page<UserLogDto>> list(@RequestParam(value = "filter", required = false) String filterJson) {
        // 操作审计包含 IP、请求参数等全租户信息，需要审计查看权限；有权限可查看全部用户的操作日志
        UserDetail user = getLoginUser();
        moduleAccess.require(user, DataPermissionEnumsName.V2_AUDIT_LOG);
        Filter filter = parseFilter(filterJson);
        if (filter == null) {
            filter = new Filter();
        }
        return success(userLogService.find(filter, user, moduleAccess.allData(user, DataPermissionEnumsName.V2_AUDIT_LOG)));
    }

    /**
     * 审计记录只能由 TapData 服务端产生；该入口曾允许任意登录用户写入伪造记录，现已禁止（需求 BR-13）。
     */
    @Operation(summary = "添加操作日志（已禁止）")
    @PostMapping
    public ResponseMessage<Page<UserLogDto>> add(@RequestBody AddUserLogParam addUserLogParam) {
        throw new BizException("Audit.Log.ReadOnly");
    }


}
