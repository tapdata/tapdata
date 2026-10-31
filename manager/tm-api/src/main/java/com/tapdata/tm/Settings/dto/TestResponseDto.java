package com.tapdata.tm.Settings.dto;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class TestResponseDto implements Serializable {
    private boolean result;
    private String stack;
    private Set<String> validSentAddresses;
    private Set<String> failedAddresses;

    public boolean isResult() {
        return result;
    }

    public String getStack() {
        return stack;
    }

    /**
     * 群发时实际投递成功的地址。整体成功且无地址被拒时为空集合，调用方应以 {@link #isResult()} 判断。
     */
    public Set<String> getValidSentAddresses() {
        return validSentAddresses == null ? Collections.emptySet() : validSentAddresses;
    }

    /**
     * 群发时被邮件服务器拒绝或未投递的地址；整体成功且无失败地址时为空集合。
     */
    public Set<String> getFailedAddresses() {
        return failedAddresses == null ? Collections.emptySet() : failedAddresses;
    }

    public TestResponseDto(boolean result, String stack) {
        this(result, stack, null, null);
    }

    public TestResponseDto(boolean result, String stack, Collection<String> validSentAddresses) {
        this(result, stack, validSentAddresses, null);
    }

    public TestResponseDto(boolean result, String stack, Collection<String> validSentAddresses,
                           Collection<String> failedAddresses) {
        this.result = result;
        this.stack = stack;
        this.validSentAddresses = normalizeAll(validSentAddresses);
        this.failedAddresses = normalizeAll(failedAddresses);
    }

    private static Set<String> normalizeAll(Collection<String> addresses) {
        if (addresses == null || addresses.isEmpty()) {
            return Collections.emptySet();
        }
        return addresses.stream()
                .filter(Objects::nonNull)
                .map(TestResponseDto::normalizeAddress)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
    }

    private static String normalizeAddress(String address) {
        return address.trim().toLowerCase(Locale.ROOT);
    }
}
