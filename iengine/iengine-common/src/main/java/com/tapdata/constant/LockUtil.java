package com.tapdata.constant;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.function.Predicate;

/**
 * @author samuel
 * @Description
 * @create 2022-06-07 11:44
 **/
public class LockUtil {
	public static void runWithLock(Lock lock, Predicate<?> stop, Runner runner) {
		if (null == lock) {
			runner.run();
			return;
		}
		try {
			while (true) {
				if (null != stop && stop.test(null)) {
					break;
				}
				try {
					if (lock.tryLock(1L, TimeUnit.SECONDS)) {
						break;
					}
				} catch (InterruptedException e) {
					break;
				}
			}
			runner.run();
		} finally {
			lock.unlock();
		}
	}

	public interface Runner {
		void run();
	}
}
