package io.tapdata.exception;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/12/13 15:11 Create
 */
public class TapdataCodeException extends TapdataRuntimeException {
    /**
     * Error code
     */
    private final int code;
    /**
     * Information that causes an exception, You can bring low-level exception information to upper-layer logic
     */
    private Map<String, Serializable> info;

    public TapdataCodeException(int code) {
        this.code = code;
    }

    public TapdataCodeException(String message, int code) {
        super(message);
        this.code = code;
    }

    public TapdataCodeException(String message, Throwable cause, int code) {
        super(message, cause);
        this.code = code;
    }

    public TapdataCodeException(Throwable cause, int code) {
        super(cause);
        this.code = code;
    }

    /**
     * Get error code
     *
     * @return error code
     */
    public int getCode() {
        return code;
    }

    /**
     * Get or init info instance
     *
     * @return info instance
     */
    protected Map<String, Serializable> getOrInitInfo() {
        if (null == info) {
            info = new HashMap<>();
        }
        return info;
    }

    /**
     * Get all info keys
     *
     * @return keys
     */
    public Set<String> infoKeys() {
        return getOrInitInfo().keySet();
    }

    /**
     * Get info value by key
     *
     * @param key key name
     * @param <T> value type
     * @return info value of key
     */
    public <T extends Serializable> T info(String key) {
        return (T) getOrInitInfo().get(key);
    }

    /**
     * Set info
     *
     * @param key   key name
     * @param value info value
     * @param <T>   current instance type
     * @return current instance
     */
    public <T extends TapdataCodeException> T info(String key, Serializable value) {
        getOrInitInfo().put(key, value);
        return (T) this;
    }

    @Override
    public String toString() {
        return String.format("TAP%06d: %s", code, getMessage());
    }
}
