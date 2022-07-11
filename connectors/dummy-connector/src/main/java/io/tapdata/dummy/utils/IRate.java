package io.tapdata.dummy.utils;

/**
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2022/7/11 08:13 Create
 */
public interface IRate {
    IRate EMPTY = () -> {
    };

    void add() throws InterruptedException;

    default boolean addReturn() {
        try {
            add();
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }

    static IRate getInstance(long interval, int intervalMax) {
        if (interval > 0 && intervalMax > 0) {
            return new Rate(interval, intervalMax);
        }
        return IRate.EMPTY;
    }
}
