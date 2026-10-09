package com.tapdata.tm.commons.task.dto.alarm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 没有用户目录权限的人看接收人预览时，只能看到任务上显式填写的邮箱；
 * 由用户、用户组、系统默认展开出来的地址打码，来源文案里邮箱形式的用户名同样打码。
 */
public final class PreviewEmailMask {
    private static final Pattern EMAIL = Pattern.compile("[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}");

    private PreviewEmailMask() {
    }

    /**
     * 任务上用户自己填写、本来就能在任务配置里看到的地址：CUSTOM 取 EMAIL 条目，LEGACY 取 emailReceivers。
     */
    public static Set<String> explicitEmails(List<AlarmReceiver> alarmReceivers, Collection<String> emailReceivers) {
        Set<String> explicit = new HashSet<>();
        if (alarmReceivers != null) {
            for (AlarmReceiver receiver : alarmReceivers) {
                if (receiver != null && receiver.getType() == AlarmReceiverType.EMAIL) {
                    add(explicit, receiver.getEmail());
                }
            }
        } else if (emailReceivers != null) {
            emailReceivers.forEach(email -> add(explicit, email));
        }
        return explicit;
    }

    public static void mask(AlarmReceiverPreview preview, Set<String> visibleEmails) {
        if (preview == null || preview.getEmails() == null) {
            return;
        }
        for (AlarmReceiverPreview.PreviewEmail item : preview.getEmails()) {
            if (item == null) {
                continue;
            }
            String email = item.getEmail();
            if (email != null && !visibleEmails.contains(email.trim().toLowerCase(Locale.ROOT))) {
                item.setEmail(maskEmail(email));
            }
            if (item.getSources() != null) {
                List<String> sources = new ArrayList<>();
                for (String source : item.getSources()) {
                    sources.add(maskInText(source));
                }
                item.setSources(sources);
            }
        }
    }

    static String maskEmail(String email) {
        int at = email.indexOf('@');
        if (at <= 0) {
            return "***";
        }
        return email.charAt(0) + "***" + email.substring(at);
    }

    private static String maskInText(String text) {
        if (text == null) {
            return null;
        }
        Matcher matcher = EMAIL.matcher(text);
        StringBuilder masked = new StringBuilder();
        while (matcher.find()) {
            matcher.appendReplacement(masked, Matcher.quoteReplacement(maskEmail(matcher.group())));
        }
        matcher.appendTail(masked);
        return masked.toString();
    }

    private static void add(Set<String> emails, String email) {
        if (email != null && !email.isBlank()) {
            emails.add(email.trim().toLowerCase(Locale.ROOT));
        }
    }
}
