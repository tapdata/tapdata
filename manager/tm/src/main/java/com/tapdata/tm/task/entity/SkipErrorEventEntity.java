package com.tapdata.tm.task.entity;

import java.io.Serializable;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/4/21 15:20 Create
 */
public class SkipErrorEventEntity implements Serializable {
    private String errorMode;
    private String limitMode;
    private Long limit;
    private Integer rate;

    public SkipErrorEventEntity() {
    }

    public SkipErrorEventEntity(String errorMode, String limitMode, Long limit, Integer rate) {
        this.errorMode = errorMode;
        this.limitMode = limitMode;
        this.limit = limit;
        this.rate = rate;
    }

    public String getErrorMode() {
        return errorMode;
    }

    public String getLimitMode() {
        return limitMode;
    }

    public void setErrorMode(String errorMode) {
        this.errorMode = errorMode;
    }

    public void setLimitMode(String mode) {
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
