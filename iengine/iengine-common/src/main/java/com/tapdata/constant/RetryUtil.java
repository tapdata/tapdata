package com.tapdata.constant;

/**
 * 重试工具
 * <pre>
 * Author: <a href="mailto:linhs@thoughtup.cn">Harsen</a>
 * CreateTime: 2021/6/8 下午6:48
 * </pre>
 */
public class RetryUtil {

	protected long interval;
	protected int counts;
	protected long initTimes;
	protected long tryTimes;
	protected long maxTimes;

	public RetryUtil(long interval, long maxTimes) {
		this.interval = interval;
		this.counts = 0;
		this.initTimes = System.currentTimeMillis();
		this.maxTimes = maxTimes;
	}

	public long getInterval() {
		return interval;
	}

	public int getCounts() {
		return counts;
	}

	public long getInitTimes() {
		return initTimes;
	}

	public long getTryTimes() {
		return tryTimes;
	}

	public long getMaxTimes() {
		return maxTimes;
	}

	public boolean checkByTryTimes() {
		if (0 == counts) {
			tryTimes = System.currentTimeMillis();
		}
		counts++;
		return System.currentTimeMillis() - tryTimes < maxTimes;
	}

	public boolean checkByTryTimesWithSleep() {
		if (sleepInterrupted()) {
			return false;
		}
		if (0 == counts) {
			tryTimes = System.currentTimeMillis();
		}
		counts++;
		return System.currentTimeMillis() - tryTimes < maxTimes;
	}

	public boolean checkByInitTimes() {
		if (0 == counts) {
			tryTimes = System.currentTimeMillis();
		}
		counts++;
		return System.currentTimeMillis() - initTimes < maxTimes;
	}

	public boolean sleepInterrupted() {
		try {
			Thread.sleep(interval);
		} catch (InterruptedException e) {
			return true;
		}
		return false;
	}

	/**
	 * 重试成功后，初始化重试次数
	 */
	public void reset() {
		this.counts = 0;
	}

	public static RetryUtil getInstance() {
		return new RetryUtil(60 * 1000L, 18 * 60 * 60 * 1000L);
	}

	public static RetryUtil getInstance(long interval, long maxTimes) {
		return new RetryUtil(interval, maxTimes);
	}
}
