package com.tapdata.tm.monitor.constant;

import com.tapdata.tm.utils.TimeUtil;

import java.util.Date;

public class Granularity {
    public static final String GRANULARITY_MINUTE = "minute";
    public static final String GRANULARITY_HOUR = "hour";
    public static final String GRANULARITY_DAY = "day";
    public static final String GRANULARITY_MONTH = "month";

    private static final String INVALID_GRANULARITY_ERR_FORMAT = String.format("Invalid granularity value %%s, " +
            "granularity value should be in %s, %s, %s and %s", GRANULARITY_MINUTE, GRANULARITY_HOUR, GRANULARITY_DAY,
            GRANULARITY_MONTH);

    public static String getNextLevelGranularity(String granularity) {
        switch (granularity) {
            case Granularity.GRANULARITY_MINUTE:
                return GRANULARITY_HOUR;
            case Granularity.GRANULARITY_HOUR:
                return GRANULARITY_DAY;
            case Granularity.GRANULARITY_DAY:
                return GRANULARITY_MONTH;
            case Granularity.GRANULARITY_MONTH:
                throw new RuntimeException("You should not get next level granularity when granularity is month.");
            default:
                throw new RuntimeException(String.format(INVALID_GRANULARITY_ERR_FORMAT, granularity));
        }

    }

    public static String getPreviousLevelGranularity(String granularity) {
        switch (granularity) {
            case Granularity.GRANULARITY_MINUTE:
                throw new RuntimeException("You should not get previous level granularity when granularity is minute.");
            case Granularity.GRANULARITY_HOUR:
                return GRANULARITY_MINUTE;
            case Granularity.GRANULARITY_DAY:
                return GRANULARITY_HOUR;
            case Granularity.GRANULARITY_MONTH:
                return GRANULARITY_MONTH;
            default:
                throw new RuntimeException(String.format(INVALID_GRANULARITY_ERR_FORMAT, granularity));
        }

    }

    private static final long GRANULARITY_MILLIS_INTERVAL_MINUTE = 60 * 1000;
    private static final long GRANULARITY_MILLIS_INTERVAL_HOUR = 60 * 60 * 1000;
    private static final long GRANULARITY_MILLIS_INTERVAL_DAY = 24 * 60 * 60 * 1000;
    public static long getGranularityMillisInterval(String granularity) {
        switch (granularity) {
            case Granularity.GRANULARITY_MINUTE:
                return GRANULARITY_MILLIS_INTERVAL_MINUTE;
            case Granularity.GRANULARITY_HOUR:
                return GRANULARITY_MILLIS_INTERVAL_HOUR;
            case Granularity.GRANULARITY_DAY:
                return GRANULARITY_MILLIS_INTERVAL_DAY;
            case Granularity.GRANULARITY_MONTH:
                throw new RuntimeException("You should not get millis interval when granularity is month.");
            default:
                throw new RuntimeException(String.format(INVALID_GRANULARITY_ERR_FORMAT, granularity));
        }

    }


    private static final long TIMELINE_MILLIS_INTERVAL_MINUTE = 5 * 1000;
    private static final long TIMELINE_MILLIS_INTERVAL_HOUR = 60 * 1000;
    private static final long TIMELINE_MILLIS_INTERVAL_DAY = 60 * 60 * 1000;
    private static final long TIMELINE_MILLIS_INTERVAL_MONTH = 24 * 60 * 60 * 1000;
    public static long getTimelineMillisInterval(String granularity) {
        switch (granularity) {
            case Granularity.GRANULARITY_MINUTE:
                return TIMELINE_MILLIS_INTERVAL_MINUTE;
            case Granularity.GRANULARITY_HOUR:
                return TIMELINE_MILLIS_INTERVAL_HOUR;
            case Granularity.GRANULARITY_DAY:
                return TIMELINE_MILLIS_INTERVAL_DAY;
            case Granularity.GRANULARITY_MONTH:
                return TIMELINE_MILLIS_INTERVAL_MONTH;
            default:
                throw new RuntimeException(String.format(INVALID_GRANULARITY_ERR_FORMAT, granularity));
        }

    }

    private static final long MAX_MILLIS_FOR_MINUTE_GRANULARITY = 60 * 60 * 1000L;
    private static final long MAX_MILLIS_FOR_HOUR_GRANULARITY = 24 * 60 * 60 * 1000L;
    private static final long MAX_MILLIS_FOR_DAY_GRANULARITY = 30 * 24 * 60 * 60 * 1000L;

    public static String calculateReasonableGranularity(long start, long end) {
        long diff = end - start;
        if (diff < MAX_MILLIS_FOR_MINUTE_GRANULARITY) {
            return GRANULARITY_MINUTE;
        }
        if (diff < MAX_MILLIS_FOR_HOUR_GRANULARITY) {
            return GRANULARITY_HOUR;
        }
        if (diff < MAX_MILLIS_FOR_DAY_GRANULARITY) {
            return GRANULARITY_DAY;
        }

        return GRANULARITY_MONTH;
    }


    public static Date calculateGranularityDate(String granularity, Date date) {
        switch (granularity) {
            case Granularity.GRANULARITY_MINUTE:
                return TimeUtil.cleanTimeAfterMinute(date);
            case Granularity.GRANULARITY_HOUR:
                return TimeUtil.cleanTimeAfterHour(date);
            case Granularity.GRANULARITY_DAY:
                return TimeUtil.cleanTimeAfterDay(date);
            case Granularity.GRANULARITY_MONTH:
                return TimeUtil.cleanTimeAfterMonth(date);
            default:
                throw new RuntimeException(String.format(INVALID_GRANULARITY_ERR_FORMAT, granularity));
        }
    }

	public static long calculateGranularityStart(String granularity, long times) {
		switch (granularity) {
			case Granularity.GRANULARITY_MINUTE:
				return times - times % GRANULARITY_MILLIS_INTERVAL_MINUTE;
			case Granularity.GRANULARITY_HOUR:
				return times - times % GRANULARITY_MILLIS_INTERVAL_HOUR;
			case Granularity.GRANULARITY_DAY:
				return times - times % GRANULARITY_MILLIS_INTERVAL_DAY;
			case Granularity.GRANULARITY_MONTH:
				throw new RuntimeException("Not support start times calculate when granularity is month.");
			default:
				throw new RuntimeException(String.format(INVALID_GRANULARITY_ERR_FORMAT, granularity));
		}
	}
}
