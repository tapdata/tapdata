package io.tapdata.aspect.taskmilestones;

import io.tapdata.aspect.DataFunctionAspect;
import io.tapdata.entity.aspect.Aspect;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/11/4 11:36
 */
public class RetryLifeCycleAspect extends DataFunctionAspect<RetryLifeCycleAspect> {

    private boolean retrying;
    private Long retryTimes;
    private Long startRetryTs;
    private Long endRetryTs;
    private Long nextRetryTs;
    private Long totalRetries;
    private String retryOp;
    private Boolean success;
    private Map<String, Object> retryMetadata;

    public boolean isRetrying() {
        return retrying;
    }

    public void setRetrying(boolean retrying) {
        this.retrying = retrying;
    }

    public Long getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(Long retryTimes) {
        this.retryTimes = retryTimes;
    }

    public Long getStartRetryTs() {
        return startRetryTs;
    }

    public void setStartRetryTs(Long startRetryTs) {
        this.startRetryTs = startRetryTs;
    }

    public Long getEndRetryTs() {
        return endRetryTs;
    }

    public void setEndRetryTs(Long endRetryTs) {
        this.endRetryTs = endRetryTs;
    }

    public Long getNextRetryTs() {
        return nextRetryTs;
    }

    public void setNextRetryTs(Long nextRetryTs) {
        this.nextRetryTs = nextRetryTs;
    }

    public Long getTotalRetries() {
        return totalRetries;
    }

    public void setTotalRetries(Long totalRetries) {
        this.totalRetries = totalRetries;
    }

    public String getRetryOp() {
        return retryOp;
    }

    public void setRetryOp(String retryOp) {
        this.retryOp = retryOp;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    public Map<String, Object> getRetryMetadata() {
        return retryMetadata;
    }

    public void setRetryMetadata(Map<String, Object> retryMetadata) {
        this.retryMetadata = retryMetadata;
    }
}
