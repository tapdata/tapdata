package io.tapdata.flow.engine.manager.log;

import com.tapdata.entity.Setting;
import io.tapdata.common.SettingService;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.filter.BurstFilter;

public class LogUtil {

	private SettingService settingService;

	private static final int LOG_FILTER_INTERVAL = 20;
	private static final boolean LOG_FILTER_IS_FORMAT = true;
	private static final int LOG_FILTER_RATE = 16;

	public LogUtil(SettingService settingService) {
		this.settingService = settingService;
	}

	public BurstFilter buildBurstFilter() {
		Setting log4jRate = settingService.getSetting("log4jFilterRate");
		int rate = LOG_FILTER_RATE;
		if (log4jRate != null) {
			try {
				rate = Integer.parseInt(log4jRate.getValue());
			} catch (NumberFormatException e) {
				// do nothing
			}
		}

		BurstFilter burstFilter = new BurstFilter.Builder()
				.setRate(rate)
				.setMaxBurst(rate * 100)
				.setLevel(Level.INFO)
				.build();

		return burstFilter;
	}

	public TapdataLog4jFilter buildFilter() {
		Setting log4jInterval = settingService.getSetting("log4jFilterInterval");
		int interval = LOG_FILTER_INTERVAL;
		if (log4jInterval != null) {
			String intervalValue = log4jInterval.getValue();
			if (StringUtils.isNotBlank(intervalValue)) {
				try {
					interval = Integer.valueOf(intervalValue);
				} catch (Exception e) {
					// do nothing
				}
			}
		}
		Setting log4jIsFormat = settingService.getSetting("log4jFilterIsFormat");
		boolean isFormat = LOG_FILTER_IS_FORMAT;
		if (log4jIsFormat != null) {
			String isFormatStr = log4jIsFormat.getValue();
			if (StringUtils.isNotBlank(isFormatStr)) {
				try {
					isFormat = Boolean.valueOf(isFormatStr);
				} catch (Exception e) {
					// do nothing
				}
			}
		}

		TapdataLog4jFilter filter = new TapdataLog4jFilter.Builder()
				.setInterval(interval)
				.setOnmatch(Filter.Result.DENY)
				.setOnmismatch(Filter.Result.NEUTRAL)
				.setLevel(Level.INFO)
				.setFormat(isFormat)
				.build();

		return filter;
	}

}
