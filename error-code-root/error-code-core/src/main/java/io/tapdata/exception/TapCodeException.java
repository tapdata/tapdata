package io.tapdata.exception;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/12/13 15:11 Create
 */
public class TapCodeException extends TapRuntimeException {
    private static final long serialVersionUID = -5987893032913832596L;
    /**
     * Error code
     */
    private String code;

    public TapCodeException(String code) {
        this.code = code;
    }

    public TapCodeException(String code, String message) {
        super(message);
        this.code = code;
    }

    public TapCodeException(String code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public TapCodeException(String code, Throwable cause) {
        super(cause);
        this.code = code;
    }

    /**
     * Get error code
     *
     * @return error code
     */
    public String getCode() {
        return code;
    }

    @Override
    protected void clone(TapRuntimeException tapRuntimeException) {
        if (tapRuntimeException instanceof TapCodeException) {
            TapCodeException tapCodeException = (TapCodeException) tapRuntimeException;
            tapCodeException.code = this.code;
        }
        super.clone(tapRuntimeException);
    }

    @Override
    public String toString() {
        return getMessage();
    }
}
