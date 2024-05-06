package com.tapdata.tm.init.ex;

/**
 * 初始化 Java 补丁异常
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2024/4/23 19:10 Create
 */
public class InitJavaPatchException extends RuntimeException {

    private final String filename;

    public InitJavaPatchException(Throwable cause, String filename) {
        super(String.format("Init java patch '%s' failed: %s", filename, cause.getMessage()), cause);
        this.filename = filename;
    }

    public String getFilename() {
        return filename;
    }
}
