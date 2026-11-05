package com.tapdata.tm.commons.task.dto.alarm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 旧客户端只发 {@code emailReceivers}。按旧接口语义转成新字段，避免请求被静默忽略：
 * <ul>
 *     <li>批量：非空 → EMAIL 接收人 + REPLACE；空列表 → 改回系统默认（旧实现写 [] 即回落默认）</li>
 *     <li>单任务：非空 → EMAIL 接收人；空列表 → 不改接收人（旧实现只在非空时写）</li>
 * </ul>
 * 请求里已带新字段时不处理。
 */
public final class LegacyEmailReceivers {
    private LegacyEmailReceivers() {
    }

    public static void normalize(BatchUpdateAlarmParam param) {
        if (param == null || param.getAlarmReceivers() != null || param.getUseSystemDefaultReceivers() != null
                || param.getEmailReceivers() == null) {
            return;
        }
        List<AlarmReceiver> receivers = toReceivers(param.getEmailReceivers());
        if (receivers.isEmpty()) {
            param.setUseSystemDefaultReceivers(Boolean.TRUE);
            return;
        }
        param.setAlarmReceivers(receivers);
        param.setReceiverMode(ReceiverBatchMode.REPLACE);
    }

    /**
     * @param currentSnapshot 任务当前的 emailReceivers。旧页面每次保存都会回传它，内容未变时不能把用户/组接收人压平成邮箱
     */
    public static void normalize(AlarmVO alarm, Collection<String> currentSnapshot) {
        if (!needsNormalize(alarm)) {
            return;
        }
        List<AlarmReceiver> receivers = toReceivers(alarm.getEmailReceivers());
        if (receivers.isEmpty() || sameEmails(alarm.getEmailReceivers(), currentSnapshot)) {
            return;
        }
        alarm.setAlarmReceivers(receivers);
    }

    /** 单任务请求是否只带了旧字段，调用方据此决定是否需要读取当前快照 */
    public static boolean needsNormalize(AlarmVO alarm) {
        return alarm != null && (alarm.getNodeId() == null || alarm.getNodeId().isBlank()) && alarm.getAlarmReceivers() == null
                && !Boolean.TRUE.equals(alarm.getUseSystemDefaultReceivers()) && alarm.getEmailReceivers() != null;
    }

    private static List<AlarmReceiver> toReceivers(Collection<String> emails) {
        List<AlarmReceiver> receivers = new ArrayList<>();
        for (String email : emails) {
            if (email == null || email.isBlank()) {
                continue;
            }
            receivers.add(new AlarmReceiver(AlarmReceiverType.EMAIL, null, email.trim()));
        }
        return receivers;
    }

    private static boolean sameEmails(Collection<String> left, Collection<String> right) {
        return right != null && lowerSet(left).equals(lowerSet(right));
    }

    private static Set<String> lowerSet(Collection<String> emails) {
        return emails.stream()
                .filter(email -> email != null && !email.isBlank())
                .map(email -> email.trim().toLowerCase(Locale.ROOT))
                .collect(Collectors.toSet());
    }
}
