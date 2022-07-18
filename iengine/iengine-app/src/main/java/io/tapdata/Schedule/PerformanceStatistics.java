package io.tapdata.Schedule;

import io.tapdata.common.SettingService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.math.BigInteger;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 性能统计定时器
 */
public class PerformanceStatistics {

	private final Logger logger = LogManager.getLogger(PerformanceStatistics.class);

	public final static Set<String> OLD_GC_ALGORITHM = new LinkedHashSet<String>() {
		{
			add("MarkSweepCompact");
			add("PS MarkSweep");
			add("ConcurrentMarkSweep");
			add("G1 Old Generation");
		}
	};

	/**
	 * 最大可用的缓存个数 8个小时 8*60
	 */
	public static final BigInteger MAX_AVAILABLE = new BigInteger("480");
	/**
	 * 一次清理的个数
	 */
	public static final int SINGLE_CLEAN_COUNT = 100;

	private final ConcurrentHashMap<BigInteger, Long> fullGcMap = new ConcurrentHashMap<>();

	private BigInteger count = BigInteger.ZERO;

	/**
	 * fullGcMap的起始位置
	 */
	private BigInteger startPos = BigInteger.ZERO;

	private final SettingService settingService;

	public PerformanceStatistics(SettingService settingService) {
		this.settingService = settingService;
		new ScheduledThreadPoolExecutor(1)
				.scheduleAtFixedRate(this::record, 0, 1, TimeUnit.MINUTES);
	}

	/**
	 * 每一分钟记录full gc消耗的时间
	 */
	public void record() {

		long currentFullGcTime = getCurrentFullGcTime();
		count = count.add(BigInteger.ONE);
		fullGcMap.put(count, currentFullGcTime);
		logger.debug("record full gc info {} {}", count, currentFullGcTime);
		//如果map中缓存的个数超出限度，则清理一部分
		if (fullGcMap.size() > MAX_AVAILABLE.intValue() + SINGLE_CLEAN_COUNT) {
			BigInteger endPos = count.subtract(MAX_AVAILABLE);
			logger.debug("clean {} to {}", startPos, endPos);
			while (startPos.compareTo(endPos) < 0) {
				fullGcMap.remove(startPos);
				startPos = startPos.add(BigInteger.ONE);
			}
		}
	}


	private long getCurrentFullGcTime() {
		long fullGcTime = 0;
		List<GarbageCollectorMXBean> garbageCollectorMXBeans = ManagementFactory.getGarbageCollectorMXBeans();
		for (GarbageCollectorMXBean garbageCollectorMXBean : garbageCollectorMXBeans) {
			if (OLD_GC_ALGORITHM.contains(garbageCollectorMXBean.getName())) {
				logger.debug("old gc info: [{}] [{}] [{}]", garbageCollectorMXBean.getName(), garbageCollectorMXBean.getCollectionTime(), garbageCollectorMXBean.getCollectionCount());
				fullGcTime += garbageCollectorMXBean.getCollectionTime();
			}
		}
		return fullGcTime;
	}


	public boolean isOverLoad() {
		//%
		double loadThreshold = Double.parseDouble(settingService.getString("task.load.threshold", "10"));
		//min
		int statisticsTime = settingService.getInt("task.load.statistics.time", 10);
		double fullGcTime = 0;
		int upTime = 0;
		for (int i = 0; i < statisticsTime; i++) {
			BigInteger pos1 = count.subtract(new BigInteger(String.valueOf(i)));
			Long pos1t = fullGcMap.get(pos1);
			if (pos1t == null) {
				continue;
			}
			BigInteger pos2 = count.subtract(new BigInteger(String.valueOf(i + 1)));
			Long pos2t = fullGcMap.get(pos2);
			if (pos2t == null) {
				pos2t = 0L;
			}
			fullGcTime += (pos1t - pos2t);
			upTime++;
		}
		//convert to ms
		long uptime = (long) upTime * 60 * 1000;

		logger.debug("load information: fullGc [{}], uptime [{}], load threshold [{}]", fullGcTime, uptime, loadThreshold);
		//超出指定负载
		return (fullGcTime / uptime) * 100 >= loadThreshold;
	}
}
