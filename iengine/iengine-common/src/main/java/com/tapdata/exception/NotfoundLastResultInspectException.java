package com.tapdata.exception;

import lombok.Getter;

/**
 * 不支持差异校验
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/6/15 15:28 Create
 */
@Getter
public class NotfoundLastResultInspectException extends RuntimeException {
    private final String inspectId;
    private final String firstCheckId;

    public NotfoundLastResultInspectException(String inspectId, String firstCheckId) {
        super("Not found last difference inspect result in '" + firstCheckId + "'");
        this.inspectId = inspectId;
        this.firstCheckId = firstCheckId;
    }

}
