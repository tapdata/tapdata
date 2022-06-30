package com.tapdata.utils;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: Zed
 * @Date: 2021/9/3
 * @Description:
 */
public class IncrementUtils {
    private static AtomicInteger integer = new AtomicInteger(1);

    public static int get() {
        return integer.incrementAndGet();
    }

    public static int get(int step) {
        int value = 0;
        for (int i = 0; i < step; i++) {
            value = integer.incrementAndGet();
        }
        return value;
    }

}
