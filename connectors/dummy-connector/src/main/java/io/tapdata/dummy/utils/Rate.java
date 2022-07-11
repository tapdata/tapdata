package io.tapdata.dummy.utils;

import io.tapdata.dummy.IRate;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/10 15:32 Create
 */
public class Rate implements IRate {
    private final long interval;
    private final int intervalMax;
    private int totals;
    private long lastTimes;

    public Rate(long interval, int intervalMax) {
        this.interval = interval;
        this.intervalMax = intervalMax;
        this.lastTimes = System.currentTimeMillis();
    }

    @Override
    public void add() throws InterruptedException {
        totals++;
        if (totals >= intervalMax) {
            long sleepTimes = (lastTimes + interval) - System.currentTimeMillis();
            if (sleepTimes > 0) {
                Thread.sleep(sleepTimes);
            }
            totals = 0;
            lastTimes = System.currentTimeMillis();
        }
    }
}
