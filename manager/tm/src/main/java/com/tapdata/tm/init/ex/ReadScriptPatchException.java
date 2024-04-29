package com.tapdata.tm.init.ex;

import lombok.Getter;

/**
 * 读取脚本补丁异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/4/23 19:10 Create
 */
@Getter
public class ReadScriptPatchException extends RuntimeException {

    private final String filename;

    public ReadScriptPatchException(Throwable cause, String filename) {
        super(String.format("Read script patch '%s' failed: %s", filename, cause.getMessage()), cause);
        this.filename = filename;
    }
}
