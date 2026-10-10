package com.tapdata.tm.alarm.service;

import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiver;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverScope;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.constants.DataPermissionEnumsName;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 告警接收人的用户目录访问控制：谁能看全量目录，以及写入的 USER / USER_GROUP 是否在调用者的候选范围内。
 */
@Component
@Setter(onMethod_ = {@Autowired})
public class AlarmReceiverAccess {
    private AlarmService alarmService;
    private PermissionService permissionService;
    private SettingsService settingsService;

    /**
     * 全量用户目录只给有用户管理查看权限的人。云版 User 集合跨租户，一律按本人所在组收敛。
     */
    public boolean canViewUserDirectory(UserDetail user) {
        if (user == null || (settingsService != null && settingsService.isCloud())) {
            return false;
        }
        return permissionService != null
                && permissionService.checkCurrentUserHasPermission(DataPermissionEnumsName.V2_USER_MANAGEMENT, user.getUserId());
    }

    /**
     * 没有用户目录权限时，USER / USER_GROUP 必须在本人候选范围内，或已被这些任务引用；越界直接拒绝整个请求。
     *
     * @param taskIds 调用者有编辑权限的任务，用来放行任务上已有的接收对象
     */
    public void checkScope(UserDetail user, List<AlarmReceiver> receivers, Collection<String> taskIds) {
        if (!AlarmReceiverScope.hasDirectoryReference(receivers) || canViewUserDirectory(user)) {
            return;
        }
        if (user == null) {
            throw new BizException("AlarmReceiver.OutOfScope", "");
        }
        List<AlarmReceiver> rejected = AlarmReceiverScope.outOfScope(receivers,
                alarmService.receiverCandidates(user.getUserId(), taskIds));
        if (!rejected.isEmpty()) {
            throw new BizException("AlarmReceiver.OutOfScope", rejected.stream()
                    .map(receiver -> receiver.getType() + ":" + receiver.getId())
                    .collect(Collectors.joining(", ")));
        }
    }
}
