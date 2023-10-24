package io.tapdata.entity.error;

import io.tapdata.entity.utils.FormatUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class CoreException extends RuntimeException {
    private static final long serialVersionUID = -3101325177490138661L;
    private List<CoreException> moreExceptions;
    public CoreException coreException(CoreException coreException) {
        if(moreExceptions == null)
            moreExceptions = new ArrayList<>();
        if(!moreExceptions.contains(coreException))
            moreExceptions.add(coreException);
        return this;
    }
    private Map<String, Object> infoMap;
    public CoreException infoMap(Map<String, Object> infoMap) {
        this.infoMap = infoMap;
        return this;
    }
    private Object data;
    public CoreException data(Object data) {
        this.data = data;
        return this;
    }
    public CoreException cause(Throwable cause) {
        if(this.getCause() == null) {
            this.initCause(cause);
        }
        return this;
    }
    public final static String LEVEL_INFO = "INFO";
    public final static String LEVEL_WARN = "WARN";
    public final static String LEVEL_ERROR = "ERROR";
    public final static String LEVEL_FATAL = "FATAL";

    public CoreException() {
    }

    public CoreException(int code) {
        this.code = code;
    }

    public CoreException(int code, String message, Object... params) {
        this(code, null, message, params);
    }
    public CoreException(int code, Throwable throwable, String message, Object... params) {
        this(code, throwable, FormatUtils.format(message, params));
    }

    public CoreException(int code, Throwable throwable, String message) {
        super(message, throwable);
        this.code = code;
    }

    public CoreException(String message, Object... params) {
        super(FormatUtils.format(message, params));
    }

    public CoreException(Throwable throwable, String message) {
        super(message, throwable);
    }

    public void setCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    private int code;

    /**
     *
     */
    public String toString() {
        return "code: " + code + " | message: " + this.getMessage();
    }

    public List<CoreException> getMoreExceptions() {
        return moreExceptions;
    }

    public void setMoreExceptions(List<CoreException> moreExceptions) {
        this.moreExceptions = moreExceptions;
    }

    public CoreException info(String key, Object value) {
        if (infoMap == null)
            infoMap = new HashMap<>();
        infoMap.put(key, value);
        return this;
    }

    public Object getInfo(String key) {
        if (infoMap == null)
            return null;
        return infoMap.get(key);
    }

    public Map<String, Object> getInfoMap() {
        return infoMap;
    }

    public void setInfoMap(Map<String, Object> infoMap) {
        this.infoMap = infoMap;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }

}
