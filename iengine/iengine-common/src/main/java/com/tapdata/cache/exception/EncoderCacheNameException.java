package com.tapdata.cache.exception;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/4/24 14:22 Create
 */
public class EncoderCacheNameException extends ShareCacheException {

    public EncoderCacheNameException(String code) {
        super(code);
    }

    public EncoderCacheNameException(String code, String message) {
        super(code, message);
    }

    public EncoderCacheNameException(String code, String message, Throwable cause) {
        super(code, message, cause);
    }

    public EncoderCacheNameException(String code, Throwable cause) {
        super(code, cause);
    }

    @Override
    public String getMessage() {
        return String.format("Share Cache encoder cache name '%s' failed: %s", getCacheName(), super.getMessage());
    }
}
