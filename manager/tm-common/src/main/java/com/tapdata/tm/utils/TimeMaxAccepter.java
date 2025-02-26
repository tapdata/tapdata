package com.tapdata.tm.utils;

import lombok.Getter;
import lombok.Setter;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 按时间控制最大执行数
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2025/2/23 15:32 Create
 */
public class TimeMaxAccepter {

    @Setter
    @Getter
    private long times;
    @Setter
    @Getter
    private long max;
    private long expired = 0;
    private final AtomicLong counts = new AtomicLong(0);

    public TimeMaxAccepter(long times, long max) {
        setTimes(times);
        setMax(max);
    }

    public boolean check() {
        if (System.currentTimeMillis() > expired) {
            synchronized (counts) {
                if (System.currentTimeMillis() > expired) {
                    counts.set(0);
                    expired = System.currentTimeMillis() + getTimes();
                }
            }
        }
        return counts.incrementAndGet() <= getMax();
    }
}
