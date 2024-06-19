package com.tapdata.exception;

import lombok.Getter;

/**
 * 不支持差异校验
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/6/15 15:28 Create
 */
@Getter
public class NonsupportMethodDifferenceInspectException extends RuntimeException {
    private final String inspectId;
    private final String method;

    public NonsupportMethodDifferenceInspectException(String inspectId, String method) {
        super("Nonsupport difference inspect by method: '" + method + "'");
        this.inspectId = inspectId;
        this.method = method;
    }
}
