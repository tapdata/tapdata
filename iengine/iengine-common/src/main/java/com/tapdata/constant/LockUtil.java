package com.tapdata.constant;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * @author samuel
 * @Description
 * @create 2022-06-07 11:44
 **/
public class LockUtil {
	public static void runWithLock(Lock lock, StopPredicate stop, Runner runner) {
		if (null == lock) {
			throw new IllegalArgumentException("Lock cannot be null");
		}
		try {
			while (true) {
				if (null != stop && stop.test()) {
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

	public interface StopPredicate {
		boolean test();
	}
}
