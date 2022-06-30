package io.tapdata.entity.error;

import io.tapdata.entity.utils.FormatUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class CoreException extends RuntimeException {
    private static final long serialVersionUID = -3101325177490138661L;
    private List<CoreException> moreExceptions;
    private Map<String, Object> infoMap;
    private Object data;
    public final static String LEVEL_INFO = "INFO";
    public final static String LEVEL_WARN = "WARN";
    public final static String LEVEL_ERROR = "ERROR";
    public final static String LEVEL_FATAL = "FATAL";

    public CoreException() {
    }

    public CoreException(int code) {
        this.code = code;
    }

    public CoreException(int code, String message, String logLevel) {
        this(code, message);
        this.logLevel = logLevel;
    }

    public CoreException(String logLevel, int code){
        this.code = code;
        this.logLevel = logLevel;
    }

    public CoreException(int code, String message, Object... params) {
        this(code, FormatUtils.format(message, params));
    }

    public CoreException(int code, Object data, String message) {
        this(code, message);
        this.data = data;
    }

    public CoreException(int code, String message) {
        super(message);
        this.code = code;
    }

    public CoreException(int code, String message, Throwable throwable) {
        super(message, throwable);
        this.code = code;
    }

    public CoreException(String message) {
        super(message);
    }

    public CoreException(String message, Throwable throwable) {
        super(message, throwable);
    }

    public void setCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    private int code;
    private String logLevel = LEVEL_ERROR;

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

    public CoreException setInfo(String key, Object value) {
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
