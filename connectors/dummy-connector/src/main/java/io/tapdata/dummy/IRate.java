package io.tapdata.dummy;

import io.tapdata.dummy.utils.Rate;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/11 08:13 Create
 */
public interface IRate {
    IRate EMPTY = () -> {
    };

    /**
     * Add and sleep when you need to wait
     *
     * @throws InterruptedException interrupted exception
     */
    void add() throws InterruptedException;

    /**
     * Call add and return false on interrupted exception
     *
     * @return not interrupted exception
     */
    default boolean addReturn() {
        try {
            add();
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }

    /**
     * Get IRate instance by interval and max records
     *
     * @param interval    rate interval
     * @param intervalMax max records in rate interval
     * @return rate instance
     */
    static IRate getInstance(long interval, int intervalMax) {
        if (interval > 0 && intervalMax > 0) {
            return new Rate(interval, intervalMax);
        }
        return IRate.EMPTY;
    }
}
