package com.tapdata.tm.openapi.generator.exception;

/**
 * Code generation exception
 *
 * @author tapdata
 * @date 2024/12/19
 */
public class CodeGenerationException extends RuntimeException {

    public CodeGenerationException(String message) {
        super(message);
    }

    public CodeGenerationException(String message, Throwable cause) {
        super(message, cause);
    }

    public CodeGenerationException(Throwable cause) {
        super(cause);
    }
}
