package io.tapdata.metric.collector;

import io.tapdata.entity.event.TapBaseEvent;
import io.tapdata.observable.logging.ObsLogger;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2023/12/12 18:32 Create
 */
public class SyncMetricCollector implements ISyncMetricCollector {

    private final int scale;
    private final long timeLag;
    private Long snapshotBeginTimes;
    private Long snapshotCompletedTimes;
    private Long cdcBeginTimes;
    private Long cdcCompletedTimes;
    private Long cdcFirstEventTimes;
    private Long cdcLastEventTimes;
    private final AtomicLong snapshotTotals = new AtomicLong(0);
    private final Map<String, AtomicLong> cdcTotals = new HashMap<>();
    private final Map<Long, AtomicLong> cdcDelayTotals = new HashMap<>();
    private Consumer<TapBaseEvent> calcConsumer = e -> {
    };
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public SyncMetricCollector(int scale, long timeLag) {
        this.scale = scale;
        this.timeLag = timeLag;
    }

    private long currentTimes() {
        return System.currentTimeMillis() - timeLag;
    }

    private Long calcDelay(Long ts) {
        return (currentTimes() - ts) / scale;
    }

    private AtomicLong getCdcTotals(String type) {
        AtomicLong v = cdcTotals.get(type);
        if (null == v) {
            synchronized (cdcTotals) {
                v = cdcTotals.computeIfAbsent(type, (k) -> new AtomicLong(0));
            }
        }
        return v;
    }

    private AtomicLong getCdcDelayTotals(Long type) {
        AtomicLong v = cdcDelayTotals.get(type);
        if (null == v) {
            synchronized (cdcDelayTotals) {
                v = cdcDelayTotals.computeIfAbsent(type, (k) -> new AtomicLong(0));
            }
        }
        return v;
    }

    @Override
    public void snapshotBegin() {
        this.calcConsumer = e -> {
            snapshotTotals.addAndGet(1);
            if (null == snapshotBeginTimes) {
                this.snapshotBeginTimes = currentTimes();
            }
        };
    }

    @Override
    public void snapshotCompleted() {
        this.snapshotCompletedTimes = currentTimes();
    }

    @Override
    public void cdcBegin() {
        this.calcConsumer = e -> {
            if (null == e.getReferenceTime()) return;
            if (null == this.cdcBeginTimes) {
                this.cdcBeginTimes = currentTimes();
            }

            getCdcTotals(String.valueOf(e.getType())).addAndGet(1);

            Long delay = calcDelay(e.getReferenceTime());
            getCdcDelayTotals(delay).addAndGet(1);

            this.cdcCompletedTimes = currentTimes();
            this.cdcLastEventTimes = e.getReferenceTime();
            if (null == cdcFirstEventTimes) {
                this.cdcFirstEventTimes = e.getReferenceTime();
            }
        };
    }

    @Override
    public void log(TapBaseEvent tapEvent) {
        this.calcConsumer.accept(tapEvent);
    }

    @Override
    public void log(List<? extends TapBaseEvent> tapEvents) {
        for (TapBaseEvent e : tapEvents) {
            this.calcConsumer.accept(e);
        }
    }

    @Override
    public synchronized String toString() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
        StringBuilder buf = new StringBuilder();
        if (null != snapshotBeginTimes && null != snapshotCompletedTimes) {
            long snapshotDuration = snapshotCompletedTimes - snapshotBeginTimes;
            buf.append(", \"snapshotBegin\": \"").append(sdf.format(snapshotBeginTimes)).append("\"");
            buf.append(", \"snapshotEnd\": \"").append(sdf.format(snapshotCompletedTimes)).append("\"");
            buf.append(", \"snapshotTotals\": ").append(snapshotTotals.get());
            buf.append(", \"snapshotDuration\": \"").append(Duration.ofMillis(snapshotDuration)).append("\"");

            BigDecimal snapshotQps = new BigDecimal(snapshotTotals.get());
            if (0 < snapshotDuration) {
                snapshotQps = snapshotQps.divide(BigDecimal.valueOf(snapshotDuration / 1000.0), 2, RoundingMode.HALF_UP);
            }
            buf.append(", \"snapshotQPS\": ").append(snapshotQps);
        }

        if (null != cdcBeginTimes) {
            buf.append(", \"cdcBegin\": \"").append(sdf.format(cdcBeginTimes)).append("\"");
            buf.append(", \"cdcEnd\": \"").append(sdf.format(cdcCompletedTimes)).append("\"");
            buf.append(", \"cdcFirst\": \"").append(sdf.format(cdcFirstEventTimes)).append("\"");
            buf.append(", \"cdcLast\": \"").append(sdf.format(cdcLastEventTimes)).append("\"");

            long cdcDuration = cdcCompletedTimes - cdcBeginTimes;
            AtomicLong totals = new AtomicLong(0);
            cdcTotals.forEach((k, v) -> {
                totals.addAndGet(v.get());
                buf.append(", \"cdcType").append(k).append("\": ").append(v.get());
            });
            buf.append(", \"cdcTotals\": ").append(totals.get());
            buf.append(", \"cdcDuration\": \"").append(Duration.ofMillis(cdcDuration)).append("\"");
            BigDecimal snapshotQps = new BigDecimal(totals.get());
            if (0 < cdcDuration) {
                snapshotQps = snapshotQps.divide(BigDecimal.valueOf(cdcDuration / 1000.0), 2, RoundingMode.HALF_UP);
            }
            buf.append(", \"cdcQPS\": ").append(snapshotQps);

            totals.set(0);
            AtomicReference<BigDecimal> allDelay = new AtomicReference<>(new BigDecimal(0));
            cdcDelayTotals.forEach((k, v) -> {
                totals.addAndGet(v.get());
                allDelay.set(allDelay.get().add(new BigDecimal(k * v.get())));
            });

            long latency99 = 0; // 99 百分位，最长延迟
            long latency95 = 0; // 95 百分位，最长延迟
            double currentTotals = totals.get();
            Long[] keySortArray = cdcDelayTotals.keySet().stream().sorted(Comparator.reverseOrder()).toArray(Long[]::new);
            for (Long k : keySortArray) {
                currentTotals -= cdcDelayTotals.get(k).get();
                if (currentTotals / totals.get() > 0.99) continue;

                if (0 == latency99) latency99 = k;

                if (currentTotals / totals.get() > 0.95) continue;

                latency95 = k;
                break;
            }

            buf.append(", \"99thLatency\": \"").append(latency99 * scale).append("ms\"");
            buf.append(", \"95thLatency\": \"").append(latency95 * scale).append("ms\"");
            buf.append(", \"avgLatency\": \"").append(allDelay.get()
                .multiply(new BigDecimal(scale))
                .divide(new BigDecimal(totals.get()), 2, RoundingMode.HALF_UP)
            ).append("ms\"");
        }

        if (buf.length() > 1) buf.delete(0, 2);
        return this.getClass().getSimpleName() + ": {" + buf + "}";
    }

    @Override
    public void close(ObsLogger obsLogger) {
        if (isClosed.compareAndSet(false, true)) {
            obsLogger.info(toString());
        }
    }

}
