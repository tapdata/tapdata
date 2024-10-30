package io.tapdata.observable.logging.util;

import com.tapdata.constant.BeanUtil;
import io.tapdata.common.SettingService;
import lombok.Getter;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author lg&lt;lirufei0808@gmail.com&gt;
 * create at 2024/10/30 14:30
 */
public class TokenBucketRateLimiter {

    private static TokenBucketRateLimiter instance;
    private Map<String, TokenBucket> buckets;
    private ScheduledExecutorService executorService;

    private int maxTokens = 100;          // 令牌桶的容量
    private int tokensPerSecond = 10;    // 每秒生成的令牌数

    private TokenBucketRateLimiter() {
        init();
    }

    public static TokenBucketRateLimiter get() {
        if (instance == null) {
            synchronized (TokenBucketRateLimiter.class) {
                if (instance == null) instance = new TokenBucketRateLimiter();
            }
        }
        return instance;
    }

    private void init() {
        updateSettings();
        buckets = new HashMap<>();
        executorService = Executors.newSingleThreadScheduledExecutor();

        executorService.scheduleAtFixedRate(this::addTokens, 0, 1, TimeUnit.SECONDS);
    }

    protected void updateSettings() {
        SettingService settingService = BeanUtil.getBean(SettingService.class);
        if (settingService != null) {
            maxTokens = settingService.getInt("log.upload.max", 100);
            tokensPerSecond = settingService.getInt("log.upload.perSecond", 10);
        }
    }

    protected void addTokens() {
        updateSettings();
        buckets.forEach((key, bucket) -> {
            int currentTokens = bucket.tokens.get();
            if (currentTokens < bucket.maxTokens) {
                int newTokens = Math.min(bucket.maxTokens - currentTokens, bucket.tokensPerSecond);
                bucket.tokens.addAndGet(newTokens);
            }
        });
    }

    public boolean tryAcquire(String taskId) {
        if (buckets == null) return true;
        TokenBucket bucket = buckets.computeIfAbsent(taskId, k -> new TokenBucket(maxTokens, tokensPerSecond));
        return bucket.tokens.get() > 0 && bucket.tokens.decrementAndGet() >= 0;
    }

    public void destroy() {
        if (executorService != null) {
            executorService.shutdown();
        }
    }

    public void remove(String taskId) {
        if (buckets != null)
            buckets.remove(taskId);
    }

    @ToString
    @Getter
    public class TokenBucket {
        protected int maxTokens;          // 令牌桶的容量
        protected int tokensPerSecond;    // 每秒生成的令牌数
        protected AtomicInteger tokens;   // 当前令牌数

        public TokenBucket(int maxTokens, int tokensPerSecond) {
            this.maxTokens = maxTokens;
            this.tokensPerSecond = tokensPerSecond;
            this.tokens = new AtomicInteger(maxTokens);
        }
    }
}
