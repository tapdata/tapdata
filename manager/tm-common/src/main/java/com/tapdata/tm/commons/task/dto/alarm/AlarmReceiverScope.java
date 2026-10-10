package com.tapdata.tm.commons.task.dto.alarm;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 没有用户目录权限时，写入的 USER / USER_GROUP 只能取自收敛后的候选（本人所在组及子组、本人、这些任务已引用的对象）。
 * 否则可以把任意组塞进任务，再从解析出的 emailReceivers 快照里读到该组成员的邮箱。EMAIL 条目不受限制。
 */
public final class AlarmReceiverScope {
    private AlarmReceiverScope() {
    }

    public static boolean hasDirectoryReference(List<AlarmReceiver> receivers) {
        if (receivers == null) {
            return false;
        }
        for (AlarmReceiver receiver : receivers) {
            if (receiver != null && (receiver.getType() == AlarmReceiverType.USER || receiver.getType() == AlarmReceiverType.USER_GROUP)) {
                return true;
            }
        }
        return false;
    }

    /** 返回不在候选范围内的 USER / USER_GROUP 条目，保持请求里的顺序 */
    public static List<AlarmReceiver> outOfScope(List<AlarmReceiver> receivers, AlarmReceiverCandidates candidates) {
        List<AlarmReceiver> rejected = new ArrayList<>();
        if (receivers == null) {
            return rejected;
        }
        Set<String> users = new HashSet<>();
        Set<String> groups = new HashSet<>();
        if (candidates != null) {
            if (candidates.getUsers() != null) {
                for (AlarmReceiverCandidates.CandidateUser user : candidates.getUsers()) {
                    if (user != null && user.getId() != null) {
                        users.add(user.getId());
                    }
                }
            }
            if (candidates.getGroups() != null) {
                for (AlarmReceiverCandidates.CandidateGroup group : candidates.getGroups()) {
                    if (group != null && group.getId() != null) {
                        groups.add(group.getId());
                    }
                }
            }
        }
        for (AlarmReceiver receiver : receivers) {
            if (receiver == null) {
                continue;
            }
            if ((receiver.getType() == AlarmReceiverType.USER && !users.contains(receiver.getId()))
                    || (receiver.getType() == AlarmReceiverType.USER_GROUP && !groups.contains(receiver.getId()))) {
                rejected.add(receiver);
            }
        }
        return rejected;
    }
}
