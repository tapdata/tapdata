package com.tapdata.tm.init.ex;

import lombok.Getter;

/**
 * 加载脚本补丁异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/4/23 19:10 Create
 */
@Getter
public class FindScriptPatchException extends RuntimeException {

    private final String filename;

    public FindScriptPatchException(Throwable cause, String filename) {
        super(String.format("Find script patch '%s' failed: %s", filename, cause.getMessage()), cause);
        this.filename = filename;
    }
}
