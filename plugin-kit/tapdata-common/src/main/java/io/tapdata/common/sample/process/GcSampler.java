package io.tapdata.common.sample.process;

import com.google.common.collect.EvictingQueue;
import io.tapdata.common.executor.ExecutorsManager;
import io.tapdata.common.sample.Sampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Dexter
 */
public class GcSampler implements Sampler {
    private static final Logger logger = LoggerFactory.getLogger(GcSampler.class.getSimpleName());

    private static final int BUCKET_SIZE = 60;
    private static final int PERIOD_SECONDS = 5;
    private static final List<GarbageCollectorMXBean> GC_MX_BEANS;
    static {
        GC_MX_BEANS = ManagementFactory.getGarbageCollectorMXBeans();
    }

    private final Queue<Long> buckets;
    private final GcPointEnum type;
    private final ScheduledExecutorService scheduleExecutorService;

    public GcSampler(GcPointEnum type) {
        this.type = type;
        buckets = EvictingQueue.create(BUCKET_SIZE);
        scheduleExecutorService = ExecutorsManager.getInstance().newSingleThreadScheduledExecutor(GcSampler.class.getSimpleName());
    }

    /**
     * start() should have been called to start the scheduled task to get
     * the gc related data every PERIOD_SECONDS;
     */
    public void start() {
        scheduleExecutorService.scheduleAtFixedRate(() -> {
            try {
                buckets.add(getCurrentGcPointValue());
            } catch (Throwable throwable) {
                throwable.printStackTrace();
                logger.error("PointExecutor calculate result failed, {}", throwable.getMessage());
            }
        }, 0, PERIOD_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public Number value() {
        long cur = getCurrentGcPointValue();
        if (buckets.size() < BUCKET_SIZE) {
            return cur;
        }
        return cur - buckets.peek();
    }

    private Long getCurrentGcPointValue() {
        long value = 0;
        for(GarbageCollectorMXBean gc : GC_MX_BEANS) {
            switch (type) {
                case GC_COUNT:
                    value += gc.getCollectionCount();
                    break;
                case GC_TIME:
                    value += gc.getCollectionTime();
                    break;
                default:
            }
        }
        return value;
    }

    public enum GcPointEnum {
        GC_COUNT,
        GC_TIME,
    }
}
