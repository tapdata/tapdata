package com.tapdata.tm.commons.task.dto;

import java.io.Serializable;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/4/14 15:25 Create
 */
public class SkipErrorEventDto implements Serializable {
    public enum ErrorMode {
        Disable,
        SkipTable,
        SkipData,
        ;
    }

    public enum LimitMode {
        Disable,
        SkipByRate,
        SkipByLimit,
        ;
    }

    private ErrorMode errorMode;
    private LimitMode limitMode;
    private Long limit;
    private Integer rate;

    public SkipErrorEventDto() {
    }

    public SkipErrorEventDto(ErrorMode errorMode, LimitMode limitMode, Long limit, Integer rate) {
        this.errorMode = errorMode;
        this.limitMode = limitMode;
        this.limit = limit;
        this.rate = rate;
    }

    public ErrorMode getErrorMode() {
        return errorMode;
    }

    public LimitMode getLimitMode() {
        return limitMode;
    }

    public void setErrorMode(ErrorMode errorMode) {
        this.errorMode = errorMode;
    }

    public void setLimitMode(LimitMode mode) {
        this.limitMode = mode;
    }

    public Long getLimit() {
        return limit;
    }

    public void setLimit(Long limit) {
        this.limit = limit;
    }

    public Integer getRate() {
        return rate;
    }

    public void setRate(Integer rate) {
        this.rate = rate;
    }
}
