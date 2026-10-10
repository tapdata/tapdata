package com.tapdata.tm.alarm.service;

import com.tapdata.tm.Permission.service.PermissionService;
import com.tapdata.tm.Settings.service.SettingsService;
import com.tapdata.tm.base.exception.BizException;
import com.tapdata.tm.commons.task.dto.TaskDto;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiver;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverCandidates;
import com.tapdata.tm.commons.task.dto.alarm.AlarmReceiverScope;
import com.tapdata.tm.config.security.UserDetail;
import com.tapdata.tm.permissions.constants.DataPermissionEnumsName;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 告警接收人的用户目录访问控制：谁能看全量目录，以及新写入的 USER / USER_GROUP 是否在调用者范围内。
 * <p>
 * 只校验相对任务<b>当前</b> alarmReceivers 新增的 USER / USER_GROUP：任务上已有的条目（包括已软删的用户、已删的组）一律放行，
 * 也不借用其他任务的引用。所有写入口（updateTaskAlarm、batch-update、新建 / confirm 新建、导入）都经过这里，
 * 因此库里的引用都是校验过的，APPEND 时重新解析已有引用不会越界。
 */
@Slf4j
@Component
@Setter(onMethod_ = {@Autowired})
public class AlarmReceiverAccess {
    static final String OUT_OF_SCOPE = "AlarmReceiver.OutOfScope";

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

    /** 单个任务：requested 相对 current 新增的 USER / USER_GROUP 越界时拒绝整个请求 */
    public void checkNewReferences(UserDetail user, List<AlarmReceiver> requested, List<AlarmReceiver> current) {
        checkNewReferences(user, requested, Collections.singletonList(current));
    }

    /**
     * 批量：对每个任务分别计算 requested 相对它自己当前列表的新增引用，合并后一次校验，越界整批拒绝。
     *
     * @param currents 每个目标任务当前的 alarmReceivers（元素可为 null）
     */
    public void checkNewReferences(UserDetail user, List<AlarmReceiver> requested, Collection<List<AlarmReceiver>> currents) {
        List<AlarmReceiver> added = addedAcross(requested, currents);
        if (added.isEmpty() || canViewUserDirectory(user)) {
            return;
        }
        if (user == null) {
            throw new BizException(OUT_OF_SCOPE, describe(added));
        }
        List<AlarmReceiver> rejected = AlarmReceiverScope.outOfScope(added, scopeCandidates(user, true));
        if (!rejected.isEmpty()) {
            throw new BizException(OUT_OF_SCOPE, describe(rejected));
        }
    }

    /**
     * 新建、复制、导入这类不该整单失败的入口：从 task.alarmReceivers 去掉相对 current 新增且越界的 USER / USER_GROUP。
     *
     * @param current 该任务已落库的 alarmReceivers；新任务传 null
     * @return 被去掉的条目，调用方用来记日志或警告
     */
    public List<AlarmReceiver> retainInScope(UserDetail user, TaskDto task, List<AlarmReceiver> current) {
        List<AlarmReceiver> receivers = task == null ? null : task.getAlarmReceivers();
        List<AlarmReceiver> added = AlarmReceiverScope.newDirectoryReferences(receivers, current);
        if (added.isEmpty() || canViewUserDirectory(user)) {
            return new ArrayList<>();
        }
        List<AlarmReceiver> rejected = user == null ? added
                : AlarmReceiverScope.outOfScope(added, scopeCandidates(user, false));
        if (!rejected.isEmpty()) {
            Set<String> keys = rejected.stream().map(AlarmReceiverAccess::key).collect(Collectors.toSet());
            List<AlarmReceiver> kept = new ArrayList<>(receivers);
            kept.removeIf(receiver -> receiver != null && keys.contains(key(receiver)));
            task.setAlarmReceivers(kept);
        }
        return rejected;
    }

    private static List<AlarmReceiver> addedAcross(List<AlarmReceiver> requested, Collection<List<AlarmReceiver>> currents) {
        Map<String, AlarmReceiver> added = new LinkedHashMap<>();
        if (currents != null) {
            for (List<AlarmReceiver> current : currents) {
                for (AlarmReceiver receiver : AlarmReceiverScope.newDirectoryReferences(requested, current)) {
                    added.putIfAbsent(key(receiver), receiver);
                }
            }
        }
        return new ArrayList<>(added.values());
    }

    /**
     * 只取本人范围（本人、所在组及子组、组内成员），不再带任务引用。
     * 只有存在新增引用时才会走到这里，日常保存（未改 USER / USER_GROUP）不计算候选。
     */
    private AlarmReceiverCandidates scopeCandidates(UserDetail user, boolean strict) {
        try {
            return alarmService.receiverCandidates(user.getUserId(), List.of());
        } catch (BizException e) {
            if (strict) {
                throw e;
            }
            // 社区版没有用户目录：新建 / 导入时把目录引用当作越界去掉，不中断主流程
            log.warn("alarm receiver candidates unavailable, drop directory references, userId={}, code={}", user.getUserId(), e.getErrorCode());
            return null;
        }
    }

    private static String describe(List<AlarmReceiver> receivers) {
        return receivers.stream().map(AlarmReceiverAccess::key).collect(Collectors.joining(", "));
    }

    private static String key(AlarmReceiver receiver) {
        return receiver.getType() + ":" + receiver.getId();
    }
}
