package io.tapdata.aspect.taskmilestones;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/11/5 19:08
 */
public class RetryLifeCycleAspectTest {

    @Test
    void testRetryLifeCycleAspect() {

        RetryLifeCycleAspect retryLifeCycleAspect = new RetryLifeCycleAspect();

        retryLifeCycleAspect.setRetrying(true);
        retryLifeCycleAspect.setRetryTimes(10L);
        retryLifeCycleAspect.setStartRetryTs(1L);
        retryLifeCycleAspect.setEndRetryTs(1L);
        retryLifeCycleAspect.setNextRetryTs(1L);
        retryLifeCycleAspect.setTotalRetries(10L);
        retryLifeCycleAspect.setRetryOp("WRITE");
        retryLifeCycleAspect.setSuccess(true);
        retryLifeCycleAspect.setRetryMetadata(new HashMap<>());

        Assertions.assertTrue(retryLifeCycleAspect.isRetrying());
        Assertions.assertEquals(10L, retryLifeCycleAspect.getRetryTimes());
        Assertions.assertEquals(1L, retryLifeCycleAspect.getStartRetryTs());
        Assertions.assertEquals(1L, retryLifeCycleAspect.getEndRetryTs());
        Assertions.assertEquals(1L, retryLifeCycleAspect.getNextRetryTs());
        Assertions.assertEquals(10L, retryLifeCycleAspect.getTotalRetries());
        Assertions.assertEquals("WRITE", retryLifeCycleAspect.getRetryOp());
        Assertions.assertTrue(retryLifeCycleAspect.getSuccess());
        Assertions.assertNotNull(retryLifeCycleAspect.getRetryMetadata());

        Assertions.assertNotEquals(retryLifeCycleAspect, new RetryLifeCycleAspect());

        RetryLifeCycleAspect retryLifeCycleAspect1 = new RetryLifeCycleAspect();

        retryLifeCycleAspect1.setRetrying(true);
        retryLifeCycleAspect1.setRetryTimes(10L);
        retryLifeCycleAspect1.setStartRetryTs(1L);
        retryLifeCycleAspect1.setEndRetryTs(1L);
        retryLifeCycleAspect1.setNextRetryTs(1L);
        retryLifeCycleAspect1.setTotalRetries(10L);
        retryLifeCycleAspect1.setRetryOp("WRITE");
        retryLifeCycleAspect1.setSuccess(true);
        retryLifeCycleAspect1.setRetryMetadata(new HashMap<>());

        Assertions.assertEquals(retryLifeCycleAspect.toString(), retryLifeCycleAspect1.toString());
    }

}
