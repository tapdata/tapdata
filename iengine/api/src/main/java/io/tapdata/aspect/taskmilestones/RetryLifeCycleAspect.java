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
@EqualsAndHashCode(callSuper = true)
@Data
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
}
