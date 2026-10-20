package com.tapdata.tm.permissions.service;

import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.config.security.UserDetail;
import org.springframework.stereotype.Component;

/**
 * 告警事件、通知记录、操作审计的模块权限：拥有模块权限点即可使用模块并查看全部数据。
 */
@Component
public class ModuleAccess {
    private final PermissionService permissionService;
    private final SettingsService settingsService;

    public ModuleAccess(PermissionService permissionService, SettingsService settingsService) {
        this.permissionService = permissionService;
        this.settingsService = settingsService;
    }

    /** 能否使用模块：云版、未启用权限体系的租户沿用既有约定放行 */
    public boolean allowed(UserDetail user, String permission) {
        if (user == null) {
            return false;
        }
        return settingsService.isCloud() || user.isFreeAuth()
                || permissionService.checkCurrentUserHasPermission(permission, user.getUserId());
    }

    public void require(UserDetail user, String permission) {
        if (!allowed(user, permission)) {
            throw new BizException("NotAuthorized");
        }
    }

    /**
     * 能否查看模块全部数据。云版为多租户，直接查询集合不隔离租户，仍按原有个人/任务范围。
     */
    public boolean allData(UserDetail user, String permission) {
        if (user == null || settingsService.isCloud()) {
            return false;
        }
        return user.isFreeAuth() || permissionService.checkCurrentUserHasPermission(permission, user.getUserId());
    }
}
