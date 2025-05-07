package com.tapdata.tm.mcp.exception;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2025/4/29 12:41
 */
public class McpException extends RuntimeException{
    public McpException(String message) {
        super(message);
    }
    public McpException(String message, Throwable e) {
        super(message, e);
    }
}
