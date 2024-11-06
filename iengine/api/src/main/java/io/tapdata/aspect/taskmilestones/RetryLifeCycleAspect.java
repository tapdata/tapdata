package io.tapdata.aspect.taskmilestones;

import io.tapdata.aspect.DataFunctionAspect;
import lombok.Getter;
import lombok.Setter;

import java.util.Map;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/11/4 11:36
 */
@Getter
@Setter
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

    @Override
    public String toString() {
        return "RetryLifeCycleAspect{" +
                "retrying=" + retrying +
                ", retryTimes=" + retryTimes +
                ", startRetryTs=" + startRetryTs +
                ", endRetryTs=" + endRetryTs +
                ", nextRetryTs=" + nextRetryTs +
                ", totalRetries=" + totalRetries +
                ", retryOp='" + retryOp + '\'' +
                ", success=" + success +
                ", retryMetadata=" + retryMetadata +
                '}';
    }
}
