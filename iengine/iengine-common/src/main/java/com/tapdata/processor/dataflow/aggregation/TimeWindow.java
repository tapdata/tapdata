/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tapdata.processor.dataflow.aggregation;

import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public enum TimeWindow {
	TW_5S("5 Seconds", 5, TimeUnit.SECONDS, null),
	TW_10S("10 Seconds", 10, TimeUnit.SECONDS, null),
	TW_30S("30 Seconds", 30, TimeUnit.SECONDS, null),
	TW_1M("1 Minute", 1, TimeUnit.MINUTES, TimeWindow.TW_5S),
	TW_5M("5 Minutes", 5, TimeUnit.MINUTES, TimeWindow.TW_5S),
	TW_10M("10 Minutes", 10, TimeUnit.MINUTES, TimeWindow.TW_10S),
	TW_15M("15 Minutes", 15, TimeUnit.MINUTES, null),
	TW_20M("20 Minutes", 20, TimeUnit.MINUTES, null),
	TW_30M("30 Minutes", 30, TimeUnit.MINUTES, TimeWindow.TW_30S),
	TW_1H("1 Hour", 1, TimeUnit.HOURS, TimeWindow.TW_1M),
	TW_6H("6 Hours", 6, TimeUnit.HOURS, TimeWindow.TW_5M),
	TW_8H("8 Hours", 8, TimeUnit.HOURS, null),
	TW_12H("12 Hours", 12, TimeUnit.HOURS, TimeWindow.TW_10M),
	TW_1D("24 Hours", 24, TimeUnit.HOURS, TimeWindow.TW_20M),
	;

	private final String label;
	private final int interval;
	private final TimeUnit unit;
	private final TimeWindow microTimeWindow;

	TimeWindow(String label, int interval, TimeUnit unit, TimeWindow microTimeWindow) {
		this.label = label;
		this.interval = interval;
		this.unit = unit;
		this.microTimeWindow = microTimeWindow;
	}

	public String getLabel() {
		return label;
	}

	public int getInterval() {
		return interval;
	}

	public long getIntervalInMillis() {
		return getUnit().toMillis(getInterval());
	}

	public TimeUnit getUnit() {
		return unit;
	}

	long getCurrentWindowCloseTimeMillis(TimeZone timeZone, long currentTime) {
		Calendar calendar = Calendar.getInstance(timeZone);
		calendar.setTimeInMillis(currentTime);

		switch (getUnit()) {
			case HOURS:
				int currentHour = calendar.get(Calendar.HOUR_OF_DAY);
				// floor date to beginning of day
				calendar.set(Calendar.HOUR_OF_DAY, 0);
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MILLISECOND, 0);
				// compute starting hour of next window
				int hourNextWindow = (currentHour / getInterval() + 1) * getInterval();
				calendar.set(Calendar.HOUR_OF_DAY, hourNextWindow);
				break;
			case MINUTES:
				int currentMinute = calendar.get(Calendar.MINUTE);
				// floor date to beginning of hour
				calendar.set(Calendar.MINUTE, 0);
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MILLISECOND, 0);
				// compute starting hour of next window
				int minuteNextWindow = (currentMinute / getInterval() + 1) * getInterval();
				calendar.set(Calendar.MINUTE, minuteNextWindow);
				break;
			case SECONDS:
				int currentSecond = calendar.get(Calendar.SECOND);
				// floor date to beginning of minute
				calendar.set(Calendar.SECOND, 0);
				calendar.set(Calendar.MILLISECOND, 0);
				// compute starting hour of next window
				int secondNextWindow = (currentSecond / getInterval() + 1) * getInterval();
				calendar.set(Calendar.SECOND, secondNextWindow);
				break;
			default:
				throw new RuntimeException("Shouldn't happen: " + getUnit());
		}
		return calendar.getTimeInMillis();
	}

	public TimeWindow getMicroTimeWindow() {
		return microTimeWindow;
	}

	public int getNumberOfMicroTimeWindows() {
		if (microTimeWindow == null) {
			return 0;
		} else {
			return (int) (microTimeWindow.getUnit().convert(interval, unit) / microTimeWindow.getInterval());
		}
	}
}
