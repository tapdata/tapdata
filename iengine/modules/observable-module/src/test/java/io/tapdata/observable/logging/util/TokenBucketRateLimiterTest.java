package io.tapdata.observable.logging.util;

import com.tapdata.constant.BeanUtil;
import io.tapdata.common.SettingService;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.*;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/10/30 17:14
 */
public class TokenBucketRateLimiterTest {

    @Test
    public void testTryAcquire() throws InterruptedException {

        TokenBucketRateLimiter tokenBucket = TokenBucketRateLimiter.get();
        ScheduledExecutorService executorService = (ScheduledExecutorService) ReflectionTestUtils.getField(tokenBucket, "executorService");
        Map buckets = (Map) ReflectionTestUtils.getField(tokenBucket, "buckets");

        Assertions.assertNotNull(executorService);
        Assertions.assertNotNull(buckets);

        for (int i = 0; i < 100; i++) {
            Assertions.assertTrue(tokenBucket.tryAcquire("taskId"));
        }

        Assertions.assertEquals(1, buckets.size());

        tokenBucket.remove("taskId");
        Assertions.assertEquals(0, buckets.size());

        Thread.sleep(1000);

        tokenBucket.destroy();

        Assertions.assertTrue(executorService.isShutdown());

        for (int i = 0; i < 100; i++) {
            Assertions.assertTrue(tokenBucket.tryAcquire("taskId"));
        }
        Assertions.assertFalse(tokenBucket.tryAcquire("taskId"));

        TokenBucketRateLimiter.TokenBucket bucket = (TokenBucketRateLimiter.TokenBucket) buckets.get("taskId");
        Assertions.assertEquals(0, bucket.getTokens().get());
        Assertions.assertEquals(100, bucket.getMaxTokens());
        Assertions.assertEquals(10, bucket.getTokensPerSecond());
        Assertions.assertTrue(bucket.toString().contains("tokens"));

        tokenBucket.addTokens();
        Assertions.assertEquals(10, bucket.getTokens().get());
        tokenBucket.addTokens();
        Assertions.assertEquals(20, bucket.getTokens().get());


        ReflectionTestUtils.setField(tokenBucket, "buckets", null);
        Assertions.assertDoesNotThrow(() -> tokenBucket.remove("taskId"));
        Assertions.assertTrue(tokenBucket.tryAcquire("taskId"));

        ReflectionTestUtils.setField(tokenBucket, "executorService", null);
        Assertions.assertDoesNotThrow(tokenBucket::destroy);

        try (MockedStatic<BeanUtil> mockBeanUtil = mockStatic(BeanUtil.class)) {
            SettingService settingService = mock(SettingService.class);
            mockBeanUtil.when(() -> BeanUtil.getBean(SettingService.class)).thenReturn(settingService);

            when(settingService.getInt("log.upload.max", 100)).thenReturn(90);

            tokenBucket.updateSettings();
            int maxTokens = (int) ReflectionTestUtils.getField(tokenBucket, "maxTokens");
            Assertions.assertEquals(90, maxTokens);
        }
    }

}
