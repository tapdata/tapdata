package com.tapdata.tm.message.constant;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 系统通知下线后各消息类型的新归属（TAP-13052 §9.2）。
 * 站内系统通知不再生成；ALARM 本身即通知记录，许可证消息仅作平台提醒。
 */
public final class SystemNotificationMapping {

    public enum Target {
        /** 操作审计（UserLogs） */
        AUDIT,
        /** 告警事件 + 通知记录 */
        ALARM,
        /** 平台提醒 */
        PLATFORM_NOTICE,
        /** 通知记录本身 */
        NOTIFICATION_RECORD
    }

    private static final Map<MsgTypeEnum, Target> TARGETS;

    static {
        Map<MsgTypeEnum, Target> map = new EnumMap<>(MsgTypeEnum.class);
        map.put(MsgTypeEnum.STARTED, Target.AUDIT);
        map.put(MsgTypeEnum.DELETED, Target.AUDIT);
        map.put(MsgTypeEnum.COMPLETED, Target.AUDIT);
        map.put(MsgTypeEnum.PAUSED, Target.AUDIT);
        map.put(MsgTypeEnum.ERROR, Target.ALARM);
        map.put(MsgTypeEnum.STOPPED_BY_ERROR, Target.ALARM);
        map.put(MsgTypeEnum.ENCOUNT_ERERROR_SKIPPED, Target.ALARM);
        map.put(MsgTypeEnum.INSPECT_ERROR, Target.ALARM);
        map.put(MsgTypeEnum.INSPECT_COUNT, Target.ALARM);
        map.put(MsgTypeEnum.INSPECT_VALUE, Target.ALARM);
        map.put(MsgTypeEnum.CDCLag, Target.ALARM);
        map.put(MsgTypeEnum.CONNECTED, Target.ALARM);
        map.put(MsgTypeEnum.CONNECTION_INTERRUPTED, Target.ALARM);
        map.put(MsgTypeEnum.WILL_RELEASE_AGENT, Target.PLATFORM_NOTICE);
        map.put(MsgTypeEnum.RELEASE_AGENT, Target.PLATFORM_NOTICE);
        map.put(MsgTypeEnum.EXPIRED, Target.PLATFORM_NOTICE);
        map.put(MsgTypeEnum.EXPIRING, Target.PLATFORM_NOTICE);
        map.put(MsgTypeEnum.ALARM, Target.NOTIFICATION_RECORD);
        TARGETS = Collections.unmodifiableMap(map);
    }

    private SystemNotificationMapping() {
    }

    public static Map<MsgTypeEnum, Target> targets() {
        return TARGETS;
    }

    public static Target targetOf(MsgTypeEnum type) {
        return type == null ? null : TARGETS.get(type);
    }

    /** 需要停写站内的系统通知类型（除告警通知本身之外的全部类型） */
    public static boolean isRetiredSystemNotification(MsgTypeEnum type) {
        return type != null && type != MsgTypeEnum.ALARM;
    }

    public static Set<String> licenseMsgValues() {
        return TARGETS.keySet().stream()
                .filter(t -> t == MsgTypeEnum.EXPIRED || t == MsgTypeEnum.EXPIRING)
                .map(MsgTypeEnum::getValue).collect(Collectors.toSet());
    }
}
